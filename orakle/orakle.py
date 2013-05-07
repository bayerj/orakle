# -*- coding: utf-8 -*-


import contextlib
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
def publish_arrays(socket, msg_class):
    """Publish arrays encoded by `msg_class` to `socket`."""
    while True:
        arr = (yield)
        if arr.size == 0:
            msg = msg_class(1, arr)
            log('received empty array, sending bad status message')
        else:
            msg = msg_class(0, arr)
        socket.send(msg.tostring())


@coroutine
def subscribe_to_arrays(socket, msg_class):
    """Yield arrays encoded by `msg_class` from `socket`."""
    (yield)
    while True:
        data = socket.recv()
        msg = msg_class.fromstring(data)
        if msg.status == 1:
            log('received bad status message')
            yield None
        yield msg.data


def sync_sockets(sockets, msg_classes):
    """Receive messages given by `msg_classes` published at `sockets` until all
    sources are somewhat in sync."""
    assert len(sockets) == len(msg_classes)

    # Wait until all sockets are sending.
    for socket in sockets:
        socket.recv()

    # Loop through all sockets until no socket has a message pending.
    while True:
        received_sth = False
        for socket in sockets:
            try:
                socket.recv(zmq.NOBLOCK)
            except zmq.ZMQError:
                continue
            received_sth = True
        if not received_sth:
            break


def sync_receive(sockets, msg_classes):
    """Receive from sockets in synchronization."""
    rcvrs = [subscribe_to_arrays(i, j) for i, j in zip(sockets, msg_classes)]
    for msgs in itertools.izip(*rcvrs):
        if not None in msgs:
            yield msgs


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
    def fromstring(cls, string):
        """Return an ArrayMessage object decoded from the given string."""
        header_length = struct.calcsize(cls.header_format)
        header, data = string[:header_length], string[header_length:]
        _, count, status, rowsize, n_rows = struct.unpack(
            cls.header_format, header)
        arr = np.fromstring(data, dtype='float64')
        arr.shape = arr.shape[0] / cls.rowsize, cls.rowsize
        return cls(status, arr, count)

    @classmethod
    def lastfromsocket(cls, socket):
        """Yield messages from the given socket until no more messages are
        available."""
        msg = None
        while True:
            try:
                msg = socket.recv(zmq.NOBLOCK)
            except zmq.ZMQError:
                if msg is None:
                    # We have not yet gotten a message and thus
                    # have to wait.
                    msg = socket.recv()
                continue
            yield cls.fromstring(msg)

    @classmethod
    def emptysocket(cls, socket):
        """Receive all messages from the given socket and return them as a
        list."""
        msgs = []
        while True:
            try:
                pkg = socket.recv(zmq.NOBLOCK)
            except zmq.ZMQError:
                break
            msg = cls.fromstring(pkg)
            msgs.append(msg)
        return msgs
