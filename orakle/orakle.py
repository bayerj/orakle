# -*- coding: utf-8 -*-


import contextlib
import socket
import datetime
import functools
import itertools
import json
import struct
import time

import chopmunk
import numpy as np
import zmq


def coroutine(f):
    """Turn a generator function into a coroutine by calling .next() once."""

    @functools.wraps(f)
    def started(*args, **kwargs):
        cr = f(*args, **kwargs)
        cr.next()
        return cr

    return started


def log(info):
    if isinstance(info, (str, unicode)):
        info = {'message': info}
    info['datetime'] = str(datetime.datetime.now())
    logstring = json.dumps(chopmunk.replace_numpy_data(info))
    print logstring


@contextlib.contextmanager
def durate(seconds):
    """ContextManager to make sure the block takes at least `seconds` seconds of
    time."""
    start = time.time()
    yield
    elapsed = time.time() - start
    if elapsed < seconds:
        time.sleep(seconds - elapsed)
    else:
        log({'message': 'operation took too long',
             'duration': elapsed,
             'target duration': seconds})


@coroutine
def publish_arrays(publishment, msg_class):
    """Publish arrays encoded by `msg_class` to `publishment`."""
    while True:
        arr = (yield)
        if arr.size == 0:
            msg = msg_class(1, arr)
            log('received empty array, sending bad status message')
        else:
            msg = msg_class(0, arr)
        publishment.send(msg.tostring())


@coroutine
def subscribe_to_arrays(subscription, msg_class):
    """Yield arrays encoded by `msg_class` from `subscription`."""
    (yield)
    while True:
        data = subscription.receive()
        msg = msg_class.fromstring(data)
        if msg.status != 0:
            log('received bad status message')
            yield None
            continue
        yield msg.data


def sync_subscriptions(subscriptions, msg_classes):
    """Receive messages given by `msg_classes` published at `subscriptions`
    until all sources are somewhat in sync."""
    assert len(subscriptions) == len(msg_classes)

    # Wait until all subscription are sending.
    for subscription in subscriptions:
        subscription.receive()

    # Loop through all subscription until no subscription has a message pending.
    while True:
        received_sth = False
        for subscription in subscriptions:
            try:
                subscription.receive(block=False)
            except NoMessage:
                continue
            received_sth = True
        if not received_sth:
            break


def sync_receive(subscriptions, msg_classes):
    """Receive from subscribers in synchronization."""
    rcvrs = [subscribe_to_arrays(i, j)
             for i, j in zip(subscriptions, msg_classes)]
    for msgs in itertools.izip(*rcvrs):
        if None not in msgs:
            yield msgs


class NoMessage(Exception):
    pass


class ZmqSubscription(object):

    def __init__(self, url, ctx=None, prefix=''):
        self.url = url
        self.ctx = ctx if ctx is not None else zmq.Context()
        self.prefix = prefix

        self.socket = self.ctx.socket(zmq.SUB)
        self.socket.setsockopt(zmq.SUBSCRIBE, prefix)
        self.socket.connect(url)

    def receive(self, block=True):
        if block:
            msg = self.socket.recv()
        else:
            try:
                msg = self.socket.recv(zmq.NOBLOCK)
            except zmq.ZMQError:
                raise NoMessage()
        return msg


class ZmqPublishment(object):

    def __init__(self, url, ctx=None, prefix=''):
        self.url = url
        self.ctx = ctx if ctx is not None else zmq.Context()
        self.prefix = prefix

        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.bind(url)

    def send(self, msg):
        self.socket.send(msg)


class UdpSubscription(object):

    def __init__(self, host, port, msg_size):
        self.host = host
        self.port = port
        self.msg_size = msg_size

        self.socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.host, self.port))

    def receive(self, block=True):
        self.socket.setblocking(1 if block else 0)
        try:
            data = self.socket.recv(self.msg_size)
        except socket.error:
            # Will only be raised if block is 0.
            raise NoMessage()
        return data


class UdpPublishment(object):

    def __init__(self, host, port, msg_size):
        self.host = host
        self.port = port
        self.msg_size = msg_size

        self.socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, msg):
        self.socket.sendto(msg, (self.host, self.port))


class ArrayMessage(object):
    """Class to represent arrays send over a network.

    Parameters
    ----------

    status : integer
        Indicates the status of the message. This field is application
        specific.

    data : array_like
        Numpy array that is to be sent.

    count : integer, optional, default: None
        Running counter over messages. If None, counts up an internal counter.


    Attributes
    ----------

    id : iterator
        Iterator over integers to be used as default message ids.

    rowsize :
        Size of one row of the data array. That is, each array has to have a
        size of (n, rowsize).

    header_format : string
        Format (according to the struct module) on how the message is encoded.
    """

    header_format = '<BIBBI'
    ids = itertools.count(0)

    def __init__(self, status, data, count=None):
        self.status = status
        self.data = data
        if self.data.ndim == 1:
            self.data = self.data.reshape((1, self.data.shape[0]))

        if self.data.shape[1] != self.rowsize:
            raise ValueError('array wrongly shaped %s' % str(self.data.shape))

        if self.data.size == 0:
            raise ValueError('array is empty')

        self.count = self.ids.next() if count is None else count

    def tostring(self):
        """Return a string representing the current message."""
        header = struct.pack(
            self.header_format, self.module, self.count, self.status,
            self.rowsize, self.data.shape[0])
        return header + self.data.tostring()

    @classmethod
    def header_length(cls):
        return struct.calcsize(cls.header_format)

    @classmethod
    def fromstring(cls, string):
        """Return an ArrayMessage object decoded from the given string."""
        header_length = cls.header_length()
        header, data = string[:header_length], string[header_length:]
        _, count, status, rowsize, n_rows = struct.unpack(
            cls.header_format, header)
        arr = np.fromstring(data, dtype='float64')
        arr.shape = arr.shape[0] / cls.rowsize, cls.rowsize
        return cls(status, arr, count)

    @classmethod
    def last_from_subscriber(cls, subscriber):
        """Yield messages from the given subscriber until no more messages are
        available."""
        msg = None
        while True:
            try:
                msg = subscriber.receive(block=False)
            except NoMessage:
                if msg is None:
                    # We have not yet gotten a message and thus
                    # have to wait.
                    msg = subscriber.receive()
                continue
            yield cls.fromstring(msg)

    @classmethod
    def empty_subscriber(cls, subscriber):
        """Receive all messages from the given subscriber and return them as a
        list."""
        msgs = []
        while True:
            try:
                pkg = subscriber.receive(block=False)
            except NoMessage:
                break
            msg = cls.fromstring(pkg)
            msgs.append(msg)
        return msgs


class MicroArrayMessage(ArrayMessage):

    startup_time = time.time()
    header_format = '<Bf'

    def tostring(self):
        """Return a string representing the current message."""

        header = struct.pack(
            self.header_format, self.status, time.time() - self.startup_time)
        return header + self.data.tostring()

    @classmethod
    def fromstring(cls, string):
        """Return an ArrayMessage object decoded from the given string."""
        header_length = cls.header_length()
        header, data = string[:header_length], string[header_length:]
        status, timestamp = struct.unpack(cls.header_format, header)
        arr = np.fromstring(data, dtype='float32')
        arr.shape = 1, cls.rowsize
        return cls(status, arr, timestamp)
