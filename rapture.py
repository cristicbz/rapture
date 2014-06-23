#!env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division
from collections import namedtuple

import argparse
import logging
import os
import re
import shlex
import signal
import sys
import time

import gevent
import gevent.socket
import gevent.subprocess
import redis.connection

from gevent import Timeout
from itertools import imap, izip
import reinferio.jobs as jobs

DEFAULT_REDIS = 'localhost:6379'
DEFAULT_QUEUE_OPTIONS = {
    'heartbeat_secs': 60.0,
    'auto_heartbeat': False,
    'timeout_secs': -1.0,
}

redis.connection.socket = gevent.socket
log = logging.getLogger()


class JobRunner(object):
    EVENT_SUCCESS, EVENT_FAILURE, EVENT_PROGRESS = 0, 1, 2
    ERR_HEARTBEAT = '[RAPTURE] Heartbeat interval exceeded; stderr:\n%s'
    ERR_TIMEOUT = '[RAPTURE] Overall runtime limit exceeded; stderr:\n%s'
    ERR_SIGNAL = '[RAPTURE] Killed by signal %d; stderr follows:\n%s'
    ERR_NONZERO_EXIT = '[RAPTURE] Non-zero exit code %d; stderr follows:\n%s'
    ERR_POPEN_FAIL = '[RAPTURE] Could not start subprocess %s with' \
                     ' arguments %s. Reason: %s'

    Event = namedtuple('Event', ['event_type', 'message'])

    def __init__(self, options, command, job_metadata):
        self._command_path = command
        self._metadata = job_metadata
        self._options = options
        self._global_timeout_seconds = options['timeout_secs']
        self._heartbeat_seconds = options['heartbeat_secs']
        self._auto_heartbeat = options['auto_heartbeat']
        self._running = False
        self._done = False
        self._subprocess = None
        self._stdout_still_open = True
        self._time_started = None

    def iterevents(self):
        return iter(self.next_event, None)

    def next_event(self):
        # Start the subprocess if not already running.
        if self._done:
            return
        elif not self._running:
            try:
                self._start_subprocess()
            except OSError, os_error:
                return self._event_popen_fail(os_error)

        # Grab next progress line from process or wait for it to exit if stdout
        # is closed.
        try:
            global_timeout = Timeout.start_new(self._timeout_at - time.time())
            heartbeat_timeout = Timeout.start_new(self._heartbeat_seconds)

            if self._stdout_still_open:
                progress_line = self._subprocess.stdout.readline()
                if progress_line:
                    return self._event_progress(progress_line)
                else:
                    self._stdout_still_open = False
                    return self.next_event()
            else:
                stderr_log = self._subprocess.communicate()[1]
                return self._event_subprocess_exit(stderr_log)
        except Timeout, timeout:
            if timeout == global_timeout:
                return self._event_overall_timeout()
            elif timeout == heartbeat_timeout:
                return self._event_heartbeat_timeout()
            else:
                raise
        finally:
            heartbeat_timeout.cancel()
            global_timeout.cancel()

    def _start_subprocess(self):
        assert not self._running and not self._done
        self._subprocess = gevent.subprocess.Popen(
            self._command_path + self._metadata.args,
            stdout=gevent.subprocess.PIPE, stderr=gevent.subprocess.PIPE,
            close_fds=True, preexec_fn=os.setsid)

        if self._global_timeout_seconds != -1:
            self._timeout_at = time.time() + self._global_timeout_seconds
        else:
            self._timeout_at = float('+inf')

        self._running = True

    def _finalise(self):
        assert self._running and not self._done
        self._running = False
        self._done = True

    def _kill_and_grab_stderr(self):
        assert self._running and not self._done
        # os.killpg instead of kill, so that all child processes of our child
        # are also killed. In particular, if we use kill, communicate may end
        # up hanging forever (or until the process terminates on its own).
        os.killpg(self._subprocess.pid, signal.SIGKILL)
        self._finalise()
        return self._subprocess.communicate()[1]

    def _event_overall_timeout(self):
        assert self._running and not self._done
        return JobRunner.Event(
            JobRunner.EVENT_FAILURE,
            JobRunner.ERR_TIMEOUT % self._kill_and_grab_stderr())

    def _event_progress(self, progress_line):
        assert self._running and not self._done
        return JobRunner.Event(JobRunner.EVENT_PROGRESS, progress_line[:-1])

    def _event_heartbeat_timeout(self):
        assert self._running and not self._done
        if self._auto_heartbeat:
            return JobRunner.Event(JobRunner.EVENT_PROGRESS, '')
        else:
            return JobRunner.Event(
                JobRunner.EVENT_FAILURE,
                JobRunner.ERR_HEARTBEAT % self._kill_and_grab_stderr())

    def _event_subprocess_exit(self, stderr_log):
        error_code = self._subprocess.returncode
        assert error_code is not None

        self._finalise()
        if error_code == 0:
            return JobRunner.Event(JobRunner.EVENT_SUCCESS, None)
        elif error_code < 0:
            return JobRunner.Event(
                JobRunner.EVENT_FAILURE,
                JobRunner.ERR_SIGNAL % (-error_code, stderr_log))
        else:
            return JobRunner.Event(
                JobRunner.EVENT_FAILURE,
                JobRunner.ERR_NONZERO_EXIT % (error_code, stderr_log))

    def _event_popen_fail(self, os_error):
        assert not self._done
        self._done = True
        return JobRunner.Event(
            JobRunner.EVENT_FAILURE, JobRunner.ERR_POPEN_FAIL %
            (self._command_path, self._metadata.args, os_error))


def worker(job_queue, job_type, queue_options, command):
    log.info('worker: Greenlet for %s up.', job_type)
    while True:
        try:
            job_id = job_queue.pop(job_type)
        except gevent.GreenletExit:
            break

        job_metadata = job_queue.fetch_snapshot(job_id)
        runner = JobRunner(queue_options, command, job_metadata)
        job_queue.publish_progress(job_id)
        log.info('worker: Started %s/%s.', job_type, job_id)
        for event in runner.iterevents():
            if event.event_type == JobRunner.EVENT_SUCCESS:
                job_queue.resolve(job_id)
                log.info('worker: Succeded %s/%s.', job_type, job_id)
            elif event.event_type == JobRunner.EVENT_FAILURE:
                job_queue.fail(job_id, event.message)
                log.info('worker: Failed %s/%s: %s',
                         job_type, job_id, event.message)
            else:
                assert event.event_type == JobRunner.EVENT_PROGRESS
                job_queue.publish_progress(job_id, event.message)
                log.info('worker: Progress %s/%s: %s',
                         job_type, job_id, event.message)


def signal_handler(job_queue, greenlets):
    job_queue._redis.connection_pool.disconnect()
    for greenlet in greenlets:
        greenlet.kill()
    log.info('signal_handler: SIGINT caught, terminating greenlets...')


def validate_queue_options(options):
    assert len(options) == len(DEFAULT_QUEUE_OPTIONS)
    assert all(key in DEFAULT_QUEUE_OPTIONS for key in options.iterkeys())

    timeout, heartbeat = options['timeout_secs'], options['heartbeat_secs']

    if timeout < .1 and timeout != -1:
        print('error: queue timeout must be greater than .1 or equal to -1')
        sys.exit(1)

    if heartbeat < .1:
        print('error: queue heartbeat lower than .1 second not allowed')
        sys.exit(1)


def parse_option_value(option_type, value_string):
    if option_type is bool:
        value_string = value_string.lower()
        parsed = value_string in ('y', '1', 'yes', 'true')
        if not parsed and value_string not in ('n', '0', 'no', 'false'):
            raise ValueError()
    else:
        parsed = option_type(value_string)

    return parsed


def parse_queue_options(options_string):
    options = DEFAULT_QUEUE_OPTIONS.copy()
    if not options_string:
        return options

    splitter = shlex.shlex(options_string, posix=True)
    splitter.whitespace += ','
    splitter.whitespace_split = True

    for assignment in splitter:
        try:
            # No equal sign in assignment is equivalent to 'key=True'.
            key_and_value = assignment.split('=', 1)
            if len(key_and_value) == 1:
                key, value = key_and_value[0], 'True'
            else:
                key, value = key_and_value

            option_type = type(options[key])
            options[key] = parse_option_value(option_type, value)
        except ValueError:
            print("invalid value '%s' for %s option '%s'" %
                  (value, option_type.__name__, key))
            sys.exit(1)
        except KeyError:
            print('unknown queue option \'%s\'' % key)
            print('valid options are: %s' % ' '.join(options.iterkeys()))
            sys.exit(1)

    validate_queue_options(options)
    return options


def parse_mappings(job_queue, mappings):
    regex = re.compile(r'^(?:(\d+)@)?([a-zA-Z_]\w*)(?:\[([^\]]*)\])?:(.+)$')

    for mapping, match in izip(mappings, imap(regex.match, mappings)):
        if not match:
            print("invalid mapping '%s'" % mapping)
            print('note: format is [NWORKERS@]JOBTYPE[[OPTIONS]]:COMMAND and'
                  ' JOBTYPE is an alphanumeric (and \'_\') identifier which'
                  ' must not begin with a digit.')
            sys.exit(1)

        (n_workers, job_type, options_string, command) = match.groups()

        n_workers = int(n_workers) if n_workers else 1
        command = shlex.split(command)
        queue_options = parse_queue_options(options_string)

        log.info("main: Requested %d '%s' workers with command '%s' and"
                 " options %s", n_workers, job_type, command, queue_options)

        for _ in xrange(n_workers):
            yield (job_queue, job_type, queue_options, command)


def parse_hostport(hostport):
    RX = r'^(?P<host>[A-Za-z0-9-_.]+):(?P<port>[0-9]+)$'
    match = re.match(RX, hostport)
    if not match:
        log.error('string "%s" is not of form host:port' % hostport)
        sys.exit(1)
    groups = match.groupdict()
    groups['port'] = int(groups['port'])
    return groups


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(levelname)s] %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description='Rapture is a language agnostic distributed task queue '
        'built on top of Redis and written in Python.')
    _arg = parser.add_argument
    _arg('--redis-tcp', metavar='HOST:PORT', type=str, default='',
         action='store', help='Redis TCP endpoint specified as host:port - '
         'default: %s' % DEFAULT_REDIS)
    _arg('--redis-unix-socket', metavar='PATH', type=str, default='',
         action='store', help='Redis endpoint as unix socket path.')
    _arg('mapping', metavar='N@JOBTYPE|OPTIONS|:COMMAND', type=str, nargs='+',
         help='Job specification - can be specified multiple times.\n'
         'For example \'4@parse[auto_heartbeat=yes]:/bin/parser\' '
         'would run the command /bin/parser up to a maximum of 4 '
         'times in parallel with the job type identified as \'parse\', using '
         'automatic heartbeats. COMMAND can also contain fixed arguments')
    args = parser.parse_args()
    if (args.redis_unix_socket != '') and (args.redis_tcp != ''):
        print('error: both TCP and unix socket endpoints specified, please'
              ' choose one or the other')
        sys.exit(1)

    try:
        if args.redis_unix_socket:
            job_queue = jobs.connect_to_unix_socket_queue(
                args.redis_unix_socket)
        else:
            endpoint = args.redis_tcp or DEFAULT_REDIS
            job_queue = jobs.connect_to_queue(**parse_hostport(endpoint))
    except redis.exceptions.ConnectionError, error:
        print('error: Could not connect to job queue: %s' % error)
        sys.exit(1)

    worker_args = parse_mappings(job_queue, args.mapping)
    workers = [gevent.spawn(worker, *argstuple) for argstuple in worker_args]

    gevent.signal(signal.SIGINT, lambda: signal_handler(job_queue, workers))

    log.info('main: Spawned %d greenlets. Waiting on jobs...', len(workers))
    gevent.joinall(workers)

    log.info('main: Clean exit.')
