#!env python
import argparse
import re
import shlex
import signal
import sys

import gevent
import gevent.socket
import redis.connection

from reinferio import jobs


# Patch redis connections to use gevent sockets.
redis.connection.socket = gevent.socket

DEFAULT_REDIS = 'localhost:6379'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    _arg = parser.add_argument
    _arg('--redis', metavar='PATH|HOST:PORT', type=str, default=DEFAULT_REDIS,
         action='store', help='Redis endpoint specified as host:port - '
         'default: %s' % DEFAULT_REDIS)
    _arg('--flushdb', action='store_const', default=False, const=True)
    _arg('jobs', metavar='JOBTYPE_USERDATA_:ARGS', type=str, nargs='*')
    args = parser.parse_args()

    endpoint = args.redis.split(':', 1)
    if len(endpoint) == 2:
        job_queue = jobs.connect_to_queue(endpoint[0], int(endpoint[1]))
    else:
        job_queue = jobs.connect_to_unix_socket_queue(endpoint[0])

    if args.flushdb:
        job_queue.redis_connection.flushall()

    if len(args.jobs) == 0:
        sys.exit(0)

    job_defs = map(
        lambda s: re.match(r'(\w+)(?:\[([^\]]+)\])?(?::(.*)$)?', s).groups(),
        args.jobs)
    job_ids = map(lambda j: job_queue.push(j[0],
                                           userdata=j[1],
                                           args=shlex.split(j[2])).job_id,
                  job_defs)
    id_to_args = {ji: job_defs[i][2] for (i, ji) in enumerate(job_ids)}

    progress = job_queue.subscribe_to_jobs(job_ids)
    gevent.signal(signal.SIGINT, progress.close)

    def subscriber():
        for notification in progress:
            ji = notification.job_id
            blob = id_to_args[ji]
            if notification.status == jobs.STATUS_DONE:
                print 'DONE "%s" (id: %s)' % (blob, ji)
            elif notification.status == jobs.STATUS_FAILED:
                print 'FAIL "%s" (id: %s): \'%s\'' % \
                    (blob, ji, notification.message)
            else:
                print 'PROG "%s" (id: %s): \'%s\'' % \
                    (blob, ji, notification.message)
    subscriber_greenlet = gevent.spawn(subscriber)
    subscriber_greenlet.join()

    print ''
    print ''
    print 'FINAL STATUS'
    for ji in job_ids:
        snapshot = job_queue.fetch_snapshot(ji)
        print '=================================='
        print 'JOB "%s"(%s)' % (id_to_args[ji], ji)
        print '\tType        : ' + snapshot.job_type
        print '\tStatus      : ' + snapshot.status
        print '\tArgs        : ' + str(snapshot.args)
        print '\tMessage     : ' + snapshot.message
        print '\tProgress    : ' + snapshot.progress
        print '\tUserdata    : ' + snapshot.userdata
        print '\tTime created: ' + snapshot.time_created
        print '\tTime updated: ' + snapshot.time_updated
