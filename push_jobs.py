#!env python

import argparse
import gevent
import gevent.socket
import jobs
import redis.connection
import signal
import sys

# Patch redis connections to use gevent sockets.
redis.connection.socket = gevent.socket

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--reddis-server', metavar='ENDPOINT', type=str,
                        nargs='?', default='localhost')
    parser.add_argument('--flushdb', action='store_const', default=False,
                        const=True)
    parser.add_argument(
        'jobs', metavar='JOBTYPE:DATA', type=str, nargs='*')
    args = parser.parse_args()

    job_queue = jobs.JobQueue(args.reddis_server)
    if args.flushdb:
        job_queue.redis_connection.flushall()

    if len(args.jobs) == 0:
        sys.exit(0)

    job_defs = map(lambda s: s.split(':', 1), args.jobs)
    job_ids = map(lambda j: job_queue.push(*j).job_id, job_defs)
    id_to_blob = {ji: job_defs[i][1] for (i, ji) in enumerate(job_ids)}

    (progress, close) = job_queue.subscribe_to_jobs(job_ids)
    gevent.signal(signal.SIGINT, close)

    def subscriber():
        for notification in progress():
            ji = notification.job_id
            blob = id_to_blob[ji]
            if notification.status == jobs.STATUS_DONE:
                print 'DONE %s(%s)' % (blob, ji)
            elif notification.status == jobs.STATUS_FAILED:
                print 'FAIL %s(%s)' % (blob, ji)
            else:
                print 'PROG %s(%s) = \'%s\'' % \
                    (blob, ji, notification.progress)

    subscriber_greenlet = gevent.spawn(subscriber)
    subscriber_greenlet.join()
