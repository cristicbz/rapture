#!/usr/bin/env python

import argparse
import gevent
import gevent.socket
import gevent.subprocess
import jobs
import re
import redis.connection
import shlex
import signal
import sys

# Patch redis connections to use gevent sockets.
redis.connection.socket = gevent.socket

SLAVE_SUCCESS = 0
SLAVE_FAILURE = 1
SLAVE_PROGRESS = 2
SLAVE_HEARTBEAT_TTL = 4


def info(message, *args):
    sys.stderr.write(('[INFO] %s\n' % message) % tuple(args))


def fatal(message, *args):
    sys.stderr.write(('[FATAL] %s\n' % message) % tuple(args))
    sys.exit(-1)


def run_command(command, arg):
    line = command + [arg]
    try:
        slave = gevent.subprocess.Popen(line, stdout=gevent.subprocess.PIPE,
                                        stderr=gevent.subprocess.PIPE)
    except OSError, e:
        def generator():
            errmsg = 'popen failed %r - %s' % (line, e.message)
            yield SLAVE_FAILURE, 'ERROR: %s:' % errmsg
            fatal(errmsg)
        return generator

    def generator():
        while True:
            line = None
            with gevent.Timeout(2, False):
                line = slave.stdout.readline()

            if line is None:
                slave.kill()
                yield SLAVE_FAILURE, \
                    'ERROR task timed out; stderr:\n%s' % \
                    slave.stderr.read()
                return

            if line == '':
                timed_out = False
                with gevent.Timeout(1, False):
                    errc = slave.wait()
                    if errc == 0:
                        yield SLAVE_SUCCESS, None
                    elif errc < 0:
                        yield SLAVE_FAILURE, 'ERROR signal %d; stderr:\n%s' % \
                            (-errc, slave.stderr.read())
                    else:
                        yield SLAVE_FAILURE, \
                            'ERROR exit with %d; stderr:\n%s' % \
                            (errc, slave.stderr.read())
                if timed_out:
                    slave.kill()
                    yield SLAVE_FAILURE, 'ERROR: Interleaved timeout.'
                return
            yield SLAVE_PROGRESS, line[:-1]

    return generator


def worker(job_queue, job_type, command):
    info('Worker for %s up.', job_type)
    while True:
        try:
            job_id = job_queue.pop(job_type)
        except gevent.GreenletExit:
            return

        job_meta = job_queue.fetch_snapshot(job_id)
        job_queue.publish_progress(job_id)
        info('worker: Started %s/%s.', job_type, job_id)
        for msg, text in run_command(command, job_meta.blob)():
            if msg == SLAVE_SUCCESS:
                job_queue.resolve(job_id)
                info('worker: Success %s/%s.', job_type, job_id)
            elif msg == SLAVE_FAILURE:
                job_queue.fail(job_id, text)
                info('worker: Failure %s/%s: %s', job_type, job_id, text)
            else:
                job_queue.publish_progress(job_id, text)
                info('worker: Progress %s/%s: %s', job_type, job_id, text)


def signal_handler(job_queue, greenlets):
    job_queue._redis.connection_pool.disconnect()
    for g in greenlets:
        g.kill()
    info('signal_handler: SIGINT caught, terminating greenlets...')


def parse_mappings(job_queue, mappings):
    regex = re.compile(r'^(?:(\d+)@)?([a-zA-Z_]\w*):(.+)$')
    workers = []

    def make_n_workers(n, job_type, command):
        return n * [lambda: worker(job_queue, job_type, command)]

    for i_match, match in enumerate(map(regex.match, mappings)):
        if not match:
            print "invalid mapping '%s'" % args.mapping[i_match]
            print 'note: format is [NWORKERS@]JOBTYPE:COMMAND and JOBTYPE is'\
                ' an alphanumeric (and \'_\') identifier which must not begin'\
                ' with a digit.'
            sys.exit(1)

        (n_workers, job_type, command) = match.groups()
        n_workers = int(n_workers) if n_workers else 1
        command = shlex.split(command)
        info('main: Creating %d worker(s) for %s, with command %r',
             n_workers, job_type, command)

        workers.extend(make_n_workers(n_workers, job_type, command))
        job_type = None

    return workers


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--reddis-server', metavar='ENDPOINT', type=str, nargs='?',
        default='localhost')
    parser.add_argument(
        'mapping', metavar='NWORKERS@JOBTYPE:COMMAND', type=str, nargs='+')
    args = parser.parse_args()

    job_queue = jobs.JobQueue(args.reddis_server)

    workers = map(gevent.spawn, parse_mappings(job_queue, args.mapping))
    gevent.signal(signal.SIGINT, lambda: signal_handler(job_queue, workers))
    info('main: Spawned %d greenlets. Waiting on jobs...', len(workers))
    gevent.joinall(workers)

    info('main: Clean exit.')
