#!env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division

# Monkey patching thread is a workaround for this issue: http://bit.ly/1svAkvU
import sys
import gevent.monkey
if 'threading' in sys.modules:
    del sys.modules['threading']
gevent.monkey.patch_thread()

# Enable reinferio and rapture imports.
sys.path.append('../')

import binascii
import os
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
    return snapshot.status == jobs.STATUS_FAILED and \
        snapshot.message.startswith(
            rapture.JobRunner.ERR_NONZERO_EXIT % (code, errlog)) and \
        snapshot.progress == progress


def is_signal_snap(snapshot, progress, sig, errlog=''):
    return snapshot.status == jobs.STATUS_FAILED and \
        snapshot.message.startswith(
            rapture.JobRunner.ERR_SIGNAL % (sig, errlog)) and \
        snapshot.progress == progress


def is_pending_snap(snapshot):
    return snapshot.status == jobs.STATUS_PENDING and \
        snapshot.message == '' and \
        snapshot.progress == ''


def is_success_snap(snapshot, progress):
    return snapshot.status == jobs.STATUS_DONE and \
        snapshot.message == '' and snapshot.progress == progress


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
        sockfile = binascii.hexlify(os.urandom(8)) + '.sock'
        self.redis_process = Popen([REDIS_COMMAND, REDIS_CONF,
                                    '--unixsocket ' + sockfile,
                                    '--unixsocketperm 755'],
                                   close_fds=True, preexec_fn=os.setsid,
                                   cwd=REDIS_WORKING_DIR)
        self.redis_endpoint = os.path.join(REDIS_WORKING_DIR, sockfile)
        for n_attempt in xrange(4):
            try:
                self.jobs = \
                    jobs.connect_to_unix_socket_queue(self.redis_endpoint)
            except:
                if n_attempt == 3:
                    raise
                else:
                    time.sleep(.1)

    def stop_redis(self):
        assert not self.rapture_running()
        self.jobs.disconnect()
        os.killpg(self.redis_process.pid, signal.SIGINT)
        self.redis_process.wait()
        try:
            os.remove(self.redis_endpoint)
        except OSError:
            pass
        self.redis_process, self.redis_endpoint, self.jobs = None, None, None

    def start_rapture(self, args):
        assert not self.rapture_running()
        socket = self.redis_endpoint or '/inexistent/redis/endpoint'

        self.rapture_process = Popen(shlex.split(RAPTURE_COMMAND) +
                                     ['--redis-unix-socket=' + socket] + args,
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
    def test_exit_modes(self):
        self.start_redis()
        self.start_rapture(
            ['qui:jobs/quiet.sh .2',
             '2@suc:jobs/success.sh .2',
             '2@seg:jobs/segfault.sh .2',
             'trm:jobs/term.sh .2',
             'non:jobs/nonzero.sh .2'])

        ids = [self.jobs.push('qui'),
               self.jobs.push('suc', args=['0', 'a']),
               self.jobs.push('suc', args=['1', 'b']),
               self.jobs.push('suc', args=['2', 'c']),
               self.jobs.push('seg', args=['0', 'd']),
               self.jobs.push('seg', args=['1', 'e']),
               self.jobs.push('trm', args=['0', 'f']),
               self.jobs.push('trm', args=['1', 'g']),
               self.jobs.push('non', args=['0', 'h']),
               self.jobs.push('non', args=['1', 'i']),
               self.jobs.push('xxx')]

        out = [(is_success_snap, ''),
               (is_success_snap, ''),
               (is_success_snap, 'success-b-1'),
               (is_success_snap, 'success-c-2'),
               (is_signal_snap, '', signal.SIGSEGV,
                'segfault-d-stderr-begin\nsegfault-d-stderr-end'),
               (is_signal_snap, 'segfault-e-1', signal.SIGSEGV,
                'segfault-e-stderr-begin\nsegfault-e-stderr-end'),
               (is_signal_snap, '', signal.SIGTERM,
                'term-f-stderr-begin\nterm-f-stderr-end'),
               (is_signal_snap, 'term-g-1', signal.SIGTERM,
                'term-g-stderr-begin\nterm-g-stderr-end'),
               (is_nonzero_snap, '', 42,
                'nonzero-h-stderr-begin\nnonzero-h-stderr-end'),
               (is_nonzero_snap, 'nonzero-i-1', 42,
                'nonzero-i-stderr-begin\nnonzero-i-stderr-end'),
               (is_pending_snap,)]

        time.sleep(1.5)

        for job_id, expect in zip(ids, out):
            snapshot = self.jobs.fetch_snapshot(job_id)
            assmsg = '\nSnapshot:\n %s\nExpect:\n %s' % (snapshot, expect[2:])
            self.assertTrue(expect[0](snapshot, *expect[1:]), assmsg)


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.realpath(sys.argv[0])))
    unittest.main()
