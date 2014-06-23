#!env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division
import os
import sys
os.chdir(os.path.dirname(os.path.realpath(sys.argv[0])))

# Monkey patching thread is a workaround for this issue: http://bit.ly/1svAkvU
import gevent.monkey
if 'threading' in sys.modules:
    del sys.modules['threading']
gevent.monkey.patch_thread()

# Enable reinferio and rapture imports.
sys.path.append('../')

import binascii
import pprint
import shlex
import signal
import time
import unittest

from gevent.subprocess import Popen, PIPE
from gevent import Timeout

import reinferio.jobs as jobs
import rapture


REDIS_COMMAND = 'redis-server'
REDIS_WORKING_DIR = 'redis'
REDIS_CONF = 'redis.conf'
RAPTURE_COMMAND = 'env python ../rapture.py'

ENV_REDIS_COMMANDLINE = ''
ENV_REDIS_ENDPOINT = ''


def pipe_fd(fd_from, fd_to):
    def greenlet():
        while True:
            line = fd_from.readline()
            if line == '':
                break
            else:
                fd_to.write(line)
    return gevent.spawn(greenlet)


def is_nonzero_snap(snapshot, progress, code, errlog=''):
    ################################## WARNING THIS IS WRONG WRONG WRONG
    ################################## INTENTIONALLY BREAKING TESTS
    ################################## GRAB NEXT COMMIT
    ################################## ADDITIONAL LINE TO TEST AGAIN
    return snapshot.status == jobs.STATUS_DONE and \
        snapshot.message == \
        rapture.JobRunner.ERR_NONZERO_EXIT % (code, errlog) and \
        snapshot.progress == progress


def is_signal_snap(snapshot, progress, sig, errlog=''):
    return snapshot.status == jobs.STATUS_FAILED and \
        snapshot.message == rapture.JobRunner.ERR_SIGNAL % (sig, errlog) and \
        snapshot.progress == progress


def is_pending_snap(snapshot):
    return snapshot.status == jobs.STATUS_PENDING and \
        snapshot.message == '' and snapshot.progress == ''


def is_success_snap(snapshot, progress):
    return snapshot.status == jobs.STATUS_DONE and \
        snapshot.message == '' and snapshot.progress == progress


def is_heartbeat_timeout_snap(snapshot, progress='', errlog=''):
    return snapshot.status == jobs.STATUS_FAILED and \
        snapshot.message == rapture.JobRunner.ERR_HEARTBEAT % errlog and \
        snapshot.progress == progress


def is_overall_timeout_snap(snapshot, progress='', errlog=''):
    return snapshot.status == jobs.STATUS_FAILED and \
        snapshot.message == rapture.JobRunner.ERR_TIMEOUT % errlog and \
        snapshot.progress == progress


class RaptureHarness(object):
    def setUp(self):
        self.rapture_process = None
        self.redis_process = None
        self.redis_endpoint = None
        self.jobs = None

    def tearDown(self):
        if self.rapture_running():
            self.interrupt_rapture()

        if self.redis_running():
            self.stop_redis()

    def start_redis(self):
        assert self.redis_process is None
        assert self.redis_endpoint is None
        assert self.jobs is None
        if not ENV_REDIS_COMMANDLINE:
            sockfile = binascii.hexlify(os.urandom(8)) + '.sock'
            redis_commandline = [REDIS_COMMAND, REDIS_CONF,
                                 '--unixsocket ' + sockfile,
                                 '--unixsocketperm 755']
            self.redis_endpoint = 'unix-socket=' + \
                os.path.join(REDIS_WORKING_DIR, sockfile)
        else:
            redis_commandline = shlex.split(ENV_REDIS_COMMANDLINE)
            self.redis_endpoint = ENV_REDIS_ENDPOINT
            assert(len(self.redis_endpoint.split(':')) >= 2)

        self.redis_process = Popen(redis_commandline, close_fds=True,
                                   preexec_fn=os.setsid, cwd=REDIS_WORKING_DIR)
        for n_attempt in xrange(4):
            try:
                protocol, endpoint = self.redis_endpoint.split('=', 1)
                if protocol == 'unix-socket':
                    self.jobs = \
                        jobs.connect_to_unix_socket_queue(endpoint)
                else:
                    host, port = endpoint.split(':', 1)
                    self.jobs = jobs.connect_to_queue(host, port)
            except:
                if n_attempt == 3:
                    raise
                else:
                    time.sleep(.1)

    def stop_redis(self):
        assert not self.rapture_running()
        self.jobs.disconnect()
        try:
            os.killpg(self.redis_process.pid, signal.SIGINT)
        except:
            pass
        self.redis_process.wait()
        self.redis_process, self.redis_endpoint, self.jobs = None, None, None

    def start_rapture(self, args):
        assert not self.rapture_running()
        socket = self.redis_endpoint or '/inexistent/redis/endpoint'

        self.rapture_process = Popen(shlex.split(RAPTURE_COMMAND) +
                                     ['--redis-' + socket] + args,
                                     close_fds=True, preexec_fn=os.setsid,
                                     stderr=PIPE, stdout=PIPE)

        # forward both stderr & stdout to stdout for them to be captured by
        # nosetests
        pipe_fd(self.rapture_process.stderr, sys.stdout)
        pipe_fd(self.rapture_process.stdout, sys.stdout)
        return self.rapture_process

    def wait_for_rapture(self):
        assert self.rapture_running()
        proc, self.rapture_process = self.rapture_process, None
        with Timeout(5, False):
            proc.wait()
            return 'errcode-%d' % proc.returncode
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()
        return 'unresponsive'

    def interrupt_rapture(self):
        assert self.rapture_running()
        with Timeout(.1, False):
            self.rapture_process.wait()
            proc, self.rapture_process = self.rapture_process, None
            return 'already-dead-errcode-%d' % proc.returncode

        os.killpg(self.rapture_process.pid, signal.SIGINT)
        return self.wait_for_rapture()

    def rapture_running(self):
        return self.rapture_process is not None

    def redis_running(self):
        return self.redis_process is not None

    def assert_job_list(self, definitions, max_wait=5, interval=.1):
        ids = map(lambda d: self.jobs.push(d[0], **d[1]), definitions)

        done = False
        expire_at = time.time() + max_wait
        while not done:
            done = True
            time.sleep(interval)
            for i, job_id, job_def in zip(xrange(len(ids)), ids, definitions):
                snapshot = self.jobs.fetch_snapshot(job_id)
                expect = job_def[2]
                expect_args = job_def[3]
                if snapshot.status == jobs.STATUS_PENDING and \
                        expect != is_pending_snap:
                    done = False
                    break
                assmsg = '\nJob Index: %d\nSnapshot:\n %s\nExpect:\n %s' % \
                    (i, pprint.pformat(dict(snapshot._asdict())),
                     pprint.pformat(expect_args))
                self.assertTrue(expect(snapshot, **expect_args), assmsg)

            if time.time() > expire_at:
                break
        self.assertTrue(done, 'Time limit exceeded %ss' % max_wait)


class SingleRaptureTestCase(unittest.TestCase, RaptureHarness):
    setUp = RaptureHarness.setUp
    tearDown = RaptureHarness.tearDown


class CleanExitTests(SingleRaptureTestCase):
    def test_empty_mappings_aborts(self):
        self.start_rapture([])
        self.assertEqual(self.wait_for_rapture(), 'errcode-2')

    def test_help_clean_exit(self):
        self.start_rapture(['-h'])
        self.assertEqual(self.wait_for_rapture(), 'errcode-0')

    def test_interrupt_clean_exit(self):
        self.start_redis()
        self.start_rapture(['s:jobs/success.sh'])
        time.sleep(.1)
        self.assertEqual(self.interrupt_rapture(), 'errcode-0')


class IntegrationTests(SingleRaptureTestCase):
    def test_heartbeat_timeout(self):
        self.start_redis()
        self.start_rapture(
            ['2@suc[heartbeat_secs=.1, auto_heartbeat=0]:jobs/success.sh',
             '2@aut[heartbeat_secs=.1, auto_heartbeat=1]:jobs/success.sh'])

        self.assert_job_list(
            [('suc', {'args': ['.11', '1', 'a']}, is_heartbeat_timeout_snap,
              dict(errlog='success-a-stderr-begin\n')),
             ('suc', {'args': ['.12', '1', 'b']}, is_heartbeat_timeout_snap,
              dict(errlog='success-b-stderr-begin\n')),
             ('suc', {'args': ['.09', '1', 'c']}, is_success_snap,
              dict(progress='success-c-1')),
             ('aut', {'args': ['.11', '2', 'd']}, is_success_snap,
              dict(progress='success-d-2')),
             ('aut', {'args': ['.12', '2', 'e']}, is_success_snap,
              dict(progress='success-e-2')),
             ('aut', {'args': ['.09', '2', 'f']}, is_success_snap,
              dict(progress='success-f-2'))],
            max_wait=1.5, interval=.1)

        self.assertEqual(self.interrupt_rapture(), 'errcode-0')

    def test_overall_timeout(self):
        self.start_redis()
        self.start_rapture(
            ['2@suc[heartbeat_secs=.15, auto_heartbeat=0, '
             'timeout_secs=.29]:jobs/success.sh',
             '2@aut[heartbeat_secs=.1, auto_heartbeat=1, '
             'timeout_secs=.29]:jobs/success.sh'])

        self.assert_job_list(
            [('suc', {'args': ['.1', '4', 'a']}, is_overall_timeout_snap,
              dict(progress='success-a-2', errlog='success-a-stderr-begin\n')),
             ('suc', {'args': ['.2', '2', 'b']}, is_heartbeat_timeout_snap,
              dict(progress='', errlog='success-b-stderr-begin\n')),
             ('aut', {'args': ['.2', '1', 'c']}, is_success_snap,
              dict(progress='success-c-1')),
             ('aut', {'args': ['.1', '4', 'd']}, is_overall_timeout_snap,
              dict(progress='success-d-2', errlog='success-d-stderr-begin\n'))
             ], max_wait=1.5, interval=.1)

        self.assertEqual(self.interrupt_rapture(), 'errcode-0')

    def test_exit_modes(self):
        self.start_redis()
        self.start_rapture(
            ['qui:jobs/quiet.sh .2',
             '2@suc:jobs/success.sh .2',
             '2@seg:jobs/segfault.sh .2',
             'trm:jobs/term.sh .2',
             'non:jobs/nonzero.sh .2'])

        self.assert_job_list(
            [('qui', {}, is_success_snap, dict(progress='')),
             ('suc', {'args': ['0', 'a']}, is_success_snap, dict(progress='')),
             ('suc', {'args': ['1', 'b']}, is_success_snap,
              dict(progress='success-b-1',)),
             ('suc', {'args': ['2', 'c']}, is_success_snap,
              dict(progress='success-c-2',)),
             ('seg', {'args': ['0', 'd']}, is_signal_snap,
              dict(progress='', sig=signal.SIGSEGV,
                   errlog='segfault-d-stderr-begin\nsegfault-d-stderr-end\n')),
             ('seg', {'args': ['1', 'e']}, is_signal_snap,
              dict(progress='segfault-e-1', sig=signal.SIGSEGV,
                   errlog='segfault-e-stderr-begin\nsegfault-e-stderr-end\n')),
             ('trm', {'args': ['0', 'f']}, is_signal_snap,
              dict(progress='', sig=signal.SIGTERM,
                   errlog='term-f-stderr-begin\nterm-f-stderr-end\n')),
             ('trm', {'args': ['1', 'g']}, is_signal_snap,
              dict(progress='term-g-1', sig=signal.SIGTERM,
                   errlog='term-g-stderr-begin\nterm-g-stderr-end\n')),
             ('non', {'args': ['0', 'h']}, is_nonzero_snap,
              dict(progress='', code=42,
                   errlog='nonzero-h-stderr-begin\nnonzero-h-stderr-end\n')),
             ('non', {'args': ['1', 'i']}, is_nonzero_snap,
              dict(progress='nonzero-i-1', code=42,
                   errlog='nonzero-i-stderr-begin\nnonzero-i-stderr-end\n')),
             ('xxx', {}, is_pending_snap, {})],
            max_wait=1.5, interval=.1)

        self.assertEqual(self.interrupt_rapture(), 'errcode-0')


if __name__ == '__main__':
    ENV_REDIS_COMMANDLINE = os.environ.get('REDIS_COMMANDLINE', None)
    ENV_REDIS_ENDPOINT = os.environ.get('REDIS_ENDPOINT', None)
    unittest.main()
