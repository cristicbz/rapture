#!env python
# -*- coding: utf-8 -*-
from __future__ import print_function, division

# Monkey patching thread is a workaround for this issue: http://bit.ly/1svAkvU
import gevent.monkey
gevent.monkey.patch_thread()

# Enable reinferio and rapture imports.
import sys
sys.path.append('../')

import binascii
import os
import shlex
import signal
import time
import unittest

from gevent.subprocess import Popen
from gevent import Timeout

import reinferio.jobs
import rapture


REDIS_COMMAND = 'redis-server'
REDIS_WORKING_DIR = 'redis'
REDIS_CONF = 'redis.conf'
RAPTURE_COMMAND = 'env python ../rapture'


def start_redis():
    sockfile = binascii.hexlify(os.urandom(8)) + '.sock'
    proc = Popen([REDIS_COMMAND, REDIS_CONF, '--unixsocket ' + sockfile,
                  '--unixsocketperm 755'],
                 close_fds=True, preexec_fn=os.setsid, cwd=REDIS_WORKING_DIR)
    return proc, os.path.join(REDIS_WORKING_DIR, sockfile)


def terminate_redis(proc, endpoint):
    os.killpg(proc.pid, signal.SIGINT)
    proc.wait()
    try:
        os.remove(endpoint)
    except OSError:
        pass


def start_rapture(redis_sockfile, mappings):
    return Popen(shlex.split(RAPTURE_COMMAND) +
                 ['--redis-unix-socket=' + redis_sockfile] + mappings,
                 close_fds=True, preexec_fn=os.setsid)


def wait_for_rapture(proc, terminate=False):
    # Process is already dead.
    if terminate:
        if proc.poll() is not None:
            errcode = proc.wait()
            return 'already-dead-errcode-%d' % errcode

        os.killpg(proc.pid, signal.SIGINT)

    with Timeout(5):
        errcode = proc.wait()
        return 'errcode-%d' % errcode
    os.killpg(proc.pid, signal.SIGKILL)
    return 'unresponsive'


class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.rapture = None
        self.redis_process, self.redis_endpoint = start_redis()

    def tearDown(self):
        assert self.redis_process is not None
        if self.rapture:
            wait_for_rapture(self.rapture, terminate=True)
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


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.realpath(sys.argv[0])))
    unittest.main()
