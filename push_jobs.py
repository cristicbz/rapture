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
from datetime import datetime


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

    def job_from_def(job_def):
        count, job_type, userdata, args = job_def
        count = int(count or '1')
        assert count >= 1
        job_ids = []
        for i_job in xrange(count):
            job_ids.append(job_queue.push(job_type,
                                          userdata=userdata,
                                          args=shlex.split(
                                              args + '-%d' % i_job)).job_id)
        return job_ids

    def_regex = re.compile(r'(?:(\d+)@)?(\w+)(?:\[([^\]]+)\])?(?::(.*)$)?')
    job_defs = map(lambda s: def_regex.match(s).groups(), args.jobs)
    job_ids = sum(map(job_from_def, job_defs), [])
    id_to_args = {jid: job_queue.fetch_snapshot(jid).job_id for jid in job_ids}

    progress = job_queue.subscribe_to_jobs(job_ids)
    gevent.signal(signal.SIGINT, progress.close)

    def subscriber():
        for notification in progress:
            ji = notification.job_id
            blob = id_to_args[ji]
            if notification.status == jobs.STATUS_DONE:
                print 'DONE "%s" (id: %s): prog=\'%s\' log=\'%s\'' % \
                    (blob, ji, notification.progress, notification.run_log)
            elif notification.status == jobs.STATUS_FAILED:
                print 'FAIL "%s" (id: %s): prog=\'%s\' log=\'%s\'' % \
                    (blob, ji, notification.progress, notification.run_log)
            else:
                print 'PROG "%s" (id: %s): prog=\'%s\' log=\'%s\'' % \
                    (blob, ji, notification.progress, notification.run_log)
    subscriber_greenlet = gevent.spawn(subscriber)
    subscriber_greenlet.join()

    def format_time(t):
        return datetime.fromtimestamp(t).isoformat()

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
        print '\tProgress    : ' + snapshot.progress
        print '\tUserdata    : ' + snapshot.userdata
        print '\tTime created: ' + format_time(snapshot.time_created)
        print '\tTime updated: ' + format_time(snapshot.time_updated)
        print '\tRun log     : ' + snapshot.run_log
