import argparse
import sys
import atexit
import os

from subprocess import Popen

sys.path.append('../')

import reinferio.jobs

DEFAULT_REDIS_COMMAND = 'redis-server'
DEFAULT_RAPTURE_COMMAND = '../rapture'

redis_command = DEFAULT_REDIS_COMMAND
rapture_command = DEFAULT_RAPTURE_COMMAND


def startup_rapture(queue_mappings):
   proc = Popen([rapture_command] + '--redis' queue_mappings


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    _arg = parser.add_argument
    _arg('--redis-command', metavar='REDIS', type=str,
         default=DEFAULT_REDIS_COMMAND, action='store',
         help='Binary name/path to use to start up redis. default: ' +
         DEFAULT_REDIS_COMMAND)
    _arg('--rapture-command', metavar='RAPTURE', type=str,
         default=DEFAULT_RAPTURE_COMMAND, action='store',
         help='Binary name/path to use to start up rapture. default: ' +
         DEFAULT_RAPTURE_COMMAND)
    args = parser.parse_args()

    redis_command = arg.redis_command
    rapture_command = arg.rapture_command

