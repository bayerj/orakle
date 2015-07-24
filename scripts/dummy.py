# -*- coding: utf-8 -*-

"""Usage:
    dummy.py serve --port=<p> --rowsize=<r> [--type=<t>] [--delay=<d>]
        [--amount-rows=<a>]
    dummy.py listen --url=<u> --rowsize=<r> [--port=<p>]
    dummy.py listen --url=<u> --rowsize=<r>



Options:
    --port=<p>          Port to serve on/ read from.
    --rowsize=<r>       Size of the rows of the messages.
    --delay=<d>         Delay between each message in seconds. [default: 0.01]
    --type=<t>          Type of data to send: 'zeros', 'incremental', 'random'
                        [default: zeros]
    --amount-rows=<a>   Amount of rows in each message. [default: 1]
    --url=<u>           Address to listen at.
"""


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
        'DummyArrayMessage', (orakle.ArrayMessage,),
        {'rowsize': rowsize,
         'module': 1})
    return ArrMsg


def listen(args):
    ctx = zmq.Context()
    socket = ctx.socket(zmq.SUB)
    socket.setsockopt(zmq.SUBSCRIBE, "")
    url = '%(--url)s:%(--port)s' % args
    print 'connecting to %s' % url
    socket.connect(url)
    ArrMsg = arrmsg_class(args)
    arrs = orakle.subscribe_to_arrays(socket, ArrMsg)

    orakle.sync_sockets([socket], [ArrMsg])
    print 'synced'

    for arr in arrs:
        print arr.shape


def serve(args):
    n_cols = int(args['--amount-rows'])
    rowsize = int(args['--rowsize'])
    ArrMsg = arrmsg_class(args)

    ctx = zmq.Context()
    socket = ctx.socket(zmq.PUB)
    url = 'tcp://*:%s' % args['--port']
    print 'binding to %s' % url
    socket.bind(url)

    consumer = orakle.publish_arrays(socket, ArrMsg)

    try:
        while True:
            with orakle.durate(int(args['--delay'])):
                data = np.random.random((n_cols, rowsize))
                consumer.send(data)
                print '.'
    except KeyboardInterrupt:
        return 1
    except Exception, e:
        raise e


if __name__ == '__main__':
    args = docopt.docopt(__doc__)
    print args
    sys.exit(main(args))
