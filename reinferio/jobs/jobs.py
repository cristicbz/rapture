# -*- coding: utf-8 -*-
import base64
from collections import namedtuple
import json
import os

import redis


INPROGRESS_KEY = 'jobs:inprogress'
DATA_KEY_PREFIX = 'jobs:data:'
QUEUE_KEY_PREFIX = 'jobs:queue:'
FIELD_QUEUE_KEY = 'queue_id'
FIELD_STATUS = 'status'
FIELD_ARGS = 'args'
FIELD_PROGRESS = 'progress'
FIELD_RUN_LOG = 'run_log'
FIELD_USERDATA = 'userdata'
FIELD_TIME_UPDATED = 'time_updated'
FIELD_TIME_CREATED = 'time_created'

STATUS_LENGTH = 1
STATUS_PENDING = 'P'
STATUS_FAILED = 'F'
STATUS_DONE = 'D'

ERROR_HEARTBEAT = 'error-heartbeat'
ERROR_NONZERO_EXIT = 'error-nonzero-exit'
ERROR_ORPHANED = 'error-orphaned'
ERROR_START_PROCESS = 'error-start-process'
ERROR_SIGNAL = 'error-signal'
ERROR_TIMEOUT = 'error-timeout'


SCRIPT_CONSTANTS = {'f_args': FIELD_ARGS,
                    'f_run_log': FIELD_RUN_LOG,
                    'f_progress': FIELD_PROGRESS,
                    'f_queue_id': FIELD_QUEUE_KEY,
                    'f_status': FIELD_STATUS,
                    'f_userdata': FIELD_USERDATA,
                    'f_time_created': FIELD_TIME_CREATED,
                    'f_time_updated': FIELD_TIME_UPDATED,
                    'status_done': STATUS_DONE,
                    'status_failed': STATUS_FAILED,
                    'status_pending': STATUS_PENDING}

SCRIPT_NEW = """
local data_key, queue_key = KEYS[1], KEYS[2]
local args, userdata, time_created = ARGV[1], ARGV[2], ARGV[3]
local rv = redis.call('hsetnx', data_key,
                      '%(f_status)s', '%(status_pending)s')
if rv == 0 then
    return 0
end
redis.call('hmset', data_key,
           '%(f_queue_id)s', queue_key,
           '%(f_args)s', args,
           '%(f_run_log)s', '',
           '%(f_progress)s', '',
           '%(f_userdata)s', userdata,
           '%(f_time_updated)s', time_created,
           '%(f_time_created)s', time_created)
redis.call('lpush', queue_key, data_key)
return 1
""" % SCRIPT_CONSTANTS

SCRIPT_RESOLVE = """
local data_key, inprogress_key = KEYS[1], KEYS[2]
local run_log, timestamp = ARGV[1], ARGV[2]
local rv = redis.call('hget', data_key, '%(f_status)s')
if rv == nil then
    return {err='inexistent job'}
elseif rv ~= '%(status_pending)s' then
    return rv
end
redis.call('hmset', data_key,
           '%(f_status)s', '%(status_done)s',
           '%(f_run_log)s', run_log,
           '%(f_time_updated)s', timestamp)
redis.call('lrem', inprogress_key, 1, data_key)
redis.call('publish', data_key, '%(status_done)s')
return '%(status_done)s'
""" % SCRIPT_CONSTANTS

SCRIPT_FAIL = """
local data_key, inprogress_key = KEYS[1], KEYS[2]
local run_log, timestamp = ARGV[1], ARGV[2]
local rv = redis.call('hget', data_key, '%(f_status)s')
if rv == nil then
    return {err='inexistent job'}
elseif rv ~= '%(status_pending)s' then
    return rv
end
redis.call('hmset', data_key,
           '%(f_status)s', '%(status_failed)s',
           '%(f_run_log)s', run_log,
           '%(f_time_updated)s', timestamp)
redis.call('lrem', inprogress_key, 1, data_key)
redis.call('publish', data_key, '%(status_failed)s')
return '$(status_failed)s'
""" % SCRIPT_CONSTANTS

SCRIPT_PROGRESS = """
local data_key, progress, timestamp = KEYS[1], ARGV[1], ARGV[2]
local rv = redis.call('hget', data_key, '%(f_status)s')
if rv == nil then
    return {err='inexistent job'}
elseif rv ~= '%(status_pending)s' then
    return rv
end
if progress ~= '' then
    redis.call('hmset', data_key,
               '%(f_time_updated)s', timestamp,
               '%(f_progress)s', progress)
    redis.call('publish', data_key, '%(status_pending)s' .. progress)
else
    redis.call('hmset', data_key, '%(f_time_updated)s', timestamp)
end
return '$(status_pending)s'
""" % SCRIPT_CONSTANTS


JobSnapshot = namedtuple(
    'JobSnapshot',
    ' '.join(('job_id', 'job_type',
              FIELD_STATUS, FIELD_ARGS, FIELD_RUN_LOG, FIELD_PROGRESS,
              FIELD_USERDATA, FIELD_TIME_CREATED, FIELD_TIME_UPDATED)))
ProgressNotifcation = namedtuple(
    'ProgressNotifcation', ' '.join(('job_id', FIELD_STATUS, FIELD_PROGRESS)))


def connect_to_queue(host='localhost', port=6379, db=0, password=None):
    return JobQueue(redis.StrictRedis(host, port, db, password))


def connect_to_unix_socket_queue(socket_path, db=0, password=None):
    return JobQueue(redis.StrictRedis(unix_socket_path=socket_path,
                                      db=db, password=password))


class JobQueue(object):
    def __init__(self, redis_client):
        self._redis = redis_client

        self._script_new = self._redis.register_script(SCRIPT_NEW)
        self._script_resolve = self._redis.register_script(SCRIPT_RESOLVE)
        self._script_fail = self._redis.register_script(SCRIPT_FAIL)
        self._script_progress = self._redis.register_script(SCRIPT_PROGRESS)

    def disconnect(self):
        self._redis.connection_pool.disconnect()

    def push(self, job_type=None, args=None, userdata=None, job=None):
        assert (job is None) != (job_type is None)
        job = job or make_new_job(job_type, args, userdata, self.timestamp())
        assert_valid_job(job)
        assert job.status == STATUS_PENDING
        self._script_new(keys=[id_to_data_key(job.job_id),
                               type_to_queue_key(job.job_type)],
                         args=[json.dumps(job.args),
                               job.userdata,
                               str(job.time_created)])
        return job

    def pop(self, job_type, timeout=0):
        assert valid_job_type(job_type)
        key = self._redis.brpoplpush(type_to_queue_key(job_type),
                                     INPROGRESS_KEY, timeout)
        return data_key_to_id(key) if key else None

    def fetch_snapshot(self, job_id):
        job_id = ensure_job_id(job_id)
        (queue_id, status, args, run_log,
         progress, userdata, time_created, time_updated) = \
            self._redis.hmget(
                id_to_data_key(job_id),
                FIELD_QUEUE_KEY, FIELD_STATUS, FIELD_ARGS,
                FIELD_RUN_LOG, FIELD_PROGRESS, FIELD_USERDATA,
                FIELD_TIME_CREATED, FIELD_TIME_UPDATED)
        assert queue_id
        args = json.loads(args)
        time_created = float(time_created)
        time_updated = float(time_updated)
        job = JobSnapshot(job_id, queue_key_to_type(queue_id), status,
                          args, run_log or '{}',
                          progress or '', userdata or '',
                          time_created or 0.0, time_updated or 0.0)
        assert_valid_job(job)
        return job

    def timestamp(self):
        # TODO(cristicbz): Synch and cache timestamps? Non-trivial of course.
        seconds, microseconds = map(float, self._redis.time())
        return seconds + microseconds * 1e-6

    def publish_progress(self, job_id, progress=''):
        job_id = ensure_job_id(job_id)
        status = self._script_progress(keys=[id_to_data_key(job_id)],
                                       args=[progress, str(self.timestamp())])
        return status == STATUS_PENDING

    def subscribe_to_jobs(self, job_list):
        pubsub = self._redis.pubsub()
        initials = []
        for job_id in job_list:
            job_id = ensure_job_id(job_id)
            data_key = id_to_data_key(job_id)
            pubsub.subscribe(data_key)
            snapshot = self.fetch_snapshot(job_id)
            initials.append(ProgressNotifcation(
                job_id,
                snapshot.status,
                snapshot.progress
                if snapshot.status == STATUS_PENDING else None))

            if snapshot.status in (STATUS_FAILED, STATUS_DONE):
                pubsub.unsubscribe(data_key)

        return ProgressListener(pubsub, initials.__iter__())

    def resolve(self, job_id, run_log=None):
        run_log = run_log or '{}'
        assert valid_run_log(run_log)
        return self._script_resolve(
            keys=[id_to_data_key(ensure_job_id(job_id)), INPROGRESS_KEY],
            args=[run_log, str(self.timestamp())]) == STATUS_DONE

    def fail(self, job_id, run_log=None):
        run_log = run_log or '{}'
        assert valid_run_log(run_log)
        return self._script_fail(
            keys=[id_to_data_key(ensure_job_id(job_id)), INPROGRESS_KEY],
            args=[run_log, str(self.timestamp())]) == STATUS_FAILED

    def monitor_inprogress(self):
        key = self._redis.brpoplpush(INPROGRESS_KEY, INPROGRESS_KEY)
        return data_key_to_id(key)

    def debug_fetch_inprogress_ids(self):
        return map(data_key_to_id, self._redis.lrange(INPROGRESS_KEY, 0, -1))

    def debug_fetch_all_ids(self):
        return map(data_key_to_id, self._redis.keys(DATA_KEY_PREFIX + '*'))

    @property
    def redis_connection(self):
        return self._redis


class ProgressListener(object):
    def __init__(self, pubsub, initials):
        self._pubsub = pubsub
        self._listen = pubsub.listen()
        self._initials = initials

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def close(self):
        self._pubsub.unsubscribe()

    def next(self):
        try:
            return self._initials.next()
        except StopIteration:
            pass

        msg = self._listen.next()
        msgt, channel, data = msg['type'], msg['channel'], msg['data']
        job_id = data_key_to_id(channel)
        assert valid_job_id(job_id)
        if msgt == 'message':
            status, message = data[:STATUS_LENGTH], data[STATUS_LENGTH:]
            if status == STATUS_DONE or status == STATUS_FAILED:
                self._pubsub.unsubscribe(channel)
                return ProgressNotifcation(job_id, status, None)
            else:
                return ProgressNotifcation(job_id, status, message)
        else:
            return self.next()


def make_error_run_log(error, returncode=None, stderr=None, command=None,
                       exception=None, signal=None):
    all_args = locals()

    def check_args(**expected_types):
        nones = []
        for key, value in all_args.iteritems():
            if key in expected_types:
                assert isinstance(value, expected_types[key]), \
                    '%s must be set for %s' % (key, error)
            else:
                assert value is None, '%s cannot be set for %s' % (key, error)
                nones.append(key)

        for none in nones:
            del all_args[none]
    if error == ERROR_HEARTBEAT or error == ERROR_TIMEOUT:
        check_args(error=basestring, stderr=basestring)
    elif error == ERROR_NONZERO_EXIT:
        check_args(error=basestring, returncode=int, stderr=basestring)
    elif error == ERROR_ORPHANED:
        check_args(error=basestring)
    elif error == ERROR_START_PROCESS:
        check_args(error=basestring, command=basestring, exception=basestring)
    elif error == ERROR_SIGNAL:
        check_args(error=basestring, signal=int, stderr=basestring)
    else:
        assert False, 'Invalid error error %s' % error

    return json.dumps(all_args)


def make_success_run_log(stderr):
    return json.dumps(locals())


def make_new_job(job_type, args=None, userdata=None, timestamp=0.0):
    args = args or []
    job = JobSnapshot(base64.urlsafe_b64encode(os.urandom(18)), job_type,
                      STATUS_PENDING, args, '{}', '', userdata or '',
                      timestamp, timestamp)
    assert_valid_job(job)
    return job


def assert_valid_job(job):
    assert valid_job_id(job.job_id)
    assert valid_job_type(job.job_type)
    assert valid_args(job.args)
    assert valid_status(job.status)
    assert valid_run_log(job.run_log)
    assert valid_progress(job.progress)
    assert valid_userdata(job.progress)
    assert valid_time(job.time_created)
    assert valid_time(job.time_updated)


def valid_time(timestamp):
    return type(timestamp) == float and timestamp > 0


def valid_run_log(run_log):
    try:
        json.loads(run_log)
    except ValueError:
        return False
    return True


def valid_progress(progress):
    return isinstance(progress, basestring)


def valid_job_type(job_type):
    return isinstance(job_type, basestring)


def valid_args(args):
    return isinstance(args, list) and \
        all([isinstance(arg, basestring) for arg in args])


def valid_status(status):
    return status in (STATUS_PENDING, STATUS_FAILED, STATUS_DONE)


def valid_job_id(job_id):
    return isinstance(job_id, basestring)


def valid_userdata(userdata):
    return isinstance(userdata, basestring)


def type_to_queue_key(job_type):
    return QUEUE_KEY_PREFIX + job_type


def queue_key_to_type(queue_id):
    assert queue_id.startswith(QUEUE_KEY_PREFIX)
    return queue_id[len(QUEUE_KEY_PREFIX):]


def id_to_data_key(job_id):
    return DATA_KEY_PREFIX + job_id


def data_key_to_id(data_key):
    assert data_key.startswith(DATA_KEY_PREFIX)
    return data_key[len(DATA_KEY_PREFIX):]


def ensure_job_id(job_or_id):
    if isinstance(job_or_id, basestring):
        assert valid_job_id(job_or_id)
        return job_or_id
    else:
        return job_or_id.job_id
