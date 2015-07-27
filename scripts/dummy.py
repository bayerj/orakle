# -*- coding: utf-8 -*-

"""Usage:
    dummy.py serve --port=<p> --rowsize=<r> [--type=<t>] [--delay=<d>]
        [--amount-rows=<a>]
        [--carrier=<c>]
    dummy.py listen --host=<h> --port=<p> --rowsize=<r>  [--carrier=<c>]



Options:
    --host=<h>          Host to send to/listen to. [default: 127.0.0.1]
    --port=<p>          Port to serve on/ read from.
    --rowsize=<r>       Size of the rows of the messages.
    --delay=<d>         Delay between each message in seconds. [default: 0.01]
    --type=<t>          Type of data to send: 'zeros', 'incremental', 'random'
                        [default: zeros]
    --amount-rows=<a>   Amount of rows in each message. [default: 1]
    --carrier=<c>       Carrier to use, one of 'zeromq' or 'udp'.
                        [default: zeromq]
"""


import itertools
import sys

import docopt
import numpy as np
import zmq

import orakle


def main(args):
    if args['serve']:
        return serve(args)
    elif args['listen']:
        return listen(args)


def arrmsg_class(args):
    rowsize = int(args['--rowsize'])
    ArrMsg = type(
        'DummyArrayMessage', (orakle.MicroArrayMessage,),
        {'rowsize': rowsize,
         'module': 1})
    return ArrMsg


def listen(args):
    ArrMsg = arrmsg_class(args)

    if args['--carrier'] == 'zeromq':
        ctx = zmq.Context()
        url = '%(--host)s:%(--port)s' % args
        print 'connecting to %s' % url
        subscription = orakle.ZmqSubscription(
            url=url, ctx=ctx, prefix='')
    elif args['--carrier'] == 'udp':
        print 'Listenting with udp at %(--host)s:%(--port)s' % args
        subscription = orakle.UdpSubscription(
            host=args['--host'], port=int(args['--port']),
            msg_size=ArrMsg.header_length() + int(args['--rowsize']) * 4)

    arrs = orakle.subscribe_to_arrays(subscription, ArrMsg)

    orakle.sync_subscriptions([subscription], [ArrMsg])
    print 'synced'

    try:
        for i, arr in enumerate(arrs):
            print '%i messages received \r' % i,
            sys.stdout.flush()
    except KeyboardInterrupt:
        return 1
    print ''


def serve(args):
    n_cols = int(args['--amount-rows'])
    rowsize = int(args['--rowsize'])
    ArrMsg = arrmsg_class(args)

    if args['--carrier'] == 'zeromq':
        url = 'tcp://*:%s' % args['--port']
        print 'binding with zeromq to %s' % url
        publishment = orakle.ZmqPublishment(url=url)
    elif args['--carrier'] == 'udp':
        print 'binding with udp to %(--host)s:%(--port)s' % args
        publishment = orakle.UdpPublishment(
            args['--host'], int(args['--port']),
            msg_size=ArrMsg.header_length() + rowsize * 4)

    consumer = orakle.publish_arrays(publishment, ArrMsg)

    try:
        for i in itertools.count():
            with orakle.durate(float(args['--delay'])):
                data = np.random.random((n_cols, rowsize))
                consumer.send(data)
                print '%i messages sent \r' % i,
                sys.stdout.flush()
    except KeyboardInterrupt:
        return 1
    except Exception, e:
        raise e
    print ''


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    print args
    sys.exit(main(args))
