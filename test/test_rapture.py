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


def start_redis():
    sockfile = binascii.hexlify(os.urandom(8)) + '.sock'
    proc = Popen([REDIS_COMMAND, REDIS_CONF, '--unixsocket ' + sockfile,
                  '--unixsocketperm 755'],
                 close_fds=True, preexec_fn=os.setsid, cwd=REDIS_WORKING_DIR)
    time.sleep(.1)
    return proc, os.path.join(REDIS_WORKING_DIR, sockfile)


def terminate_redis(proc, endpoint):
    os.killpg(proc.pid, signal.SIGINT)
    proc.wait()
    try:
        os.remove(endpoint)
    except OSError:
        pass


def start_rapture(redis_sockfile, mappings):
    proc = Popen(shlex.split(RAPTURE_COMMAND) +
                 ['--redis-unix-socket=' + redis_sockfile] + mappings,
                 close_fds=True, preexec_fn=os.setsid,
                 stderr=PIPE, stdout=PIPE)

    # forward both stderr & stdout to stdout for them to be captured by
    # nosetests
    gevent.spawn(forward_output, proc.stderr, sys.stdout)
    gevent.spawn(forward_output, proc.stdout, sys.stdout)
    return proc


def forward_output(fd_from, fd_to):
    while True:
        line = fd_from.readline()
        if line == '':
            break
        else:
            fd_to.write(line)


def wait_for_rapture(proc, terminate=False):
    # Process is already dead.
    if terminate:
        with Timeout(.1, False):
            proc.wait()
            return 'already-dead-errcode-%d' % proc.returncode

        os.killpg(proc.pid, signal.SIGINT)

    with Timeout(5, False):
        proc.wait()
        return 'errcode-%d' % proc.returncode
    os.killpg(proc.pid, signal.SIGKILL)
    proc.wait()
    return 'unresponsive'


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


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.rapture = None
        self.redis_process, self.redis_endpoint = start_redis()
        self.job_queue = jobs.connect_to_unix_socket_queue(self.redis_endpoint)

    def tearDown(self):
        assert self.redis_process is not None
        if self.rapture:
            wait_for_rapture(self.rapture, terminate=True)
        self.job_queue.disconnect()
        terminate_redis(self.redis_process, self.redis_endpoint)
        self.redis_endpoint = None
        self.redis_process = None
        self.rapture = None

    def start_rapture(self, mappings):
        assert self.rapture is None
        assert self.redis_process is not None
        self.rapture = start_rapture(self.redis_endpoint, mappings)

    def stop_rapture(self):
        assert self.rapture is not None
        self.assertEqual(wait_for_rapture(self.rapture, True), 'errcode-0')
        self.rapture = None

    def test_empty_mappings_aborts(self):
        self.start_rapture([])
        self.assertEqual(wait_for_rapture(self.rapture), 'errcode-2')

    def test_help_clean_exit(self):
        self.start_rapture(['-h'])
        self.assertEqual(wait_for_rapture(self.rapture), 'errcode-0')

    def test_interrupt_clean_exit(self):
        self.start_rapture(['s:jobs/success.sh'])
        time.sleep(.5)
        self.stop_rapture()

    def test_exit_modes(self):
        self.start_rapture(
            ['qui:jobs/quiet.sh .2',
             '2@suc:jobs/success.sh .2',
             '2@seg:jobs/segfault.sh .2',
             'trm:jobs/term.sh .2',
             'non:jobs/nonzero.sh .2'])

        ids = [self.job_queue.push('qui'),
               self.job_queue.push('suc', args=['0', 'a']),
               self.job_queue.push('suc', args=['1', 'b']),
               self.job_queue.push('suc', args=['2', 'c']),
               self.job_queue.push('seg', args=['0', 'd']),
               self.job_queue.push('seg', args=['1', 'e']),
               self.job_queue.push('trm', args=['0', 'f']),
               self.job_queue.push('trm', args=['1', 'g']),
               self.job_queue.push('non', args=['0', 'h']),
               self.job_queue.push('non', args=['1', 'i']),
               self.job_queue.push('xxx')]

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
            snapshot = self.job_queue.fetch_snapshot(job_id)
            assmsg = '\nSnapshot:\n %s\nExpect:\n %s' % (snapshot, expect[2:])
            self.assertTrue(expect[0](snapshot, *expect[1:]), assmsg)


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.realpath(sys.argv[0])))
    unittest.main()
