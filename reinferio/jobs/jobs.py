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
FIELD_MESSAGE = 'message'
FIELD_USERDATA = 'userdata'
FIELD_TIME_UPDATED = 'time_updated'
FIELD_TIME_CREATED = 'time_created'

STATUS_LEN = 1
STATUS_PENDING = 'P'
STATUS_FAILED = 'F'
STATUS_DONE = 'D'

SCRIPT_CONSTANTS = {'f_args': FIELD_ARGS,
                    'f_message': FIELD_MESSAGE,
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
           '%(f_progress)s', '',
           '%(f_userdata)s', userdata,
           '%(f_time_updated)s', time_created,
           '%(f_time_created)s', time_created)
redis.call('lpush', queue_key, data_key)
return 1
""" % SCRIPT_CONSTANTS

SCRIPT_RESOLVE = """
local data_key, inprogress_key, timestamp = KEYS[1], KEYS[2], ARGV[1]
local rv = redis.call('hget', data_key, '%(f_status)s')
if rv == nil then
    return {err='inexistent job'}
elseif rv ~= '%(status_pending)s' then
    return 0
end
redis.call('hset', data_key, '%(f_status)s', '%(status_done)s')
redis.call('publish', data_key, '%(status_done)s')
redis.call('lrem', inprogress_key, 1, data_key)
redis.call('hset', data_key, '%(f_time_updated)s', timestamp)
return 1
""" % SCRIPT_CONSTANTS

SCRIPT_FAIL = """
local data_key, inprogress_key = KEYS[1], KEYS[2]
local message, timestamp = ARGV[1], ARGV[2]
local rv = redis.call('hget', data_key, '%(f_status)s')
if rv == nil then
    return {err='inexistent job'}
elseif rv ~= '%(status_pending)s' then
    return 0
end
redis.call('hmset', data_key,
           '%(f_status)s', '%(status_failed)s',
           '%(f_message)s', message,
           '%(f_time_updated)s', timestamp)
redis.call('publish', data_key, '%(status_failed)s' .. message)
redis.call('lrem', inprogress_key, 1, data_key)
return 1
""" % SCRIPT_CONSTANTS

SCRIPT_PROGRESS = """
local data_key, progress, timestamp = KEYS[1], ARGV[1], ARGV[2]
local rv = redis.call('hget', data_key, '%(f_status)s')
if rv == nil then
    return {err='inexistent job'}
elseif rv ~= '%(status_pending)s' then
    return 0
end
redis.call('hmset', data_key,
           '%(f_time_updated)s', timestamp,
           '%(f_progress)s', progress)
if progress ~= '' then
    redis.call('publish', data_key, '%(status_pending)s' .. progress)
end
return 1
""" % SCRIPT_CONSTANTS


JobSnapshot = namedtuple(
    'JobSnapshot',
    ' '.join(('job_id', 'job_type',
              FIELD_STATUS, FIELD_ARGS, FIELD_MESSAGE, FIELD_PROGRESS,
              FIELD_USERDATA, FIELD_TIME_CREATED, FIELD_TIME_UPDATED)))
ProgressNotifcation = namedtuple(
    'ProgressNotifcation', ' '.join(('job_id', FIELD_STATUS, FIELD_MESSAGE)))


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
                               job.time_created])
        return job

    def pop(self, job_type, timeout=0):
        assert valid_job_type(job_type)
        key = self._redis.brpoplpush(type_to_queue_key(job_type),
                                     INPROGRESS_KEY, timeout)
        return data_key_to_id(key) if key else None

    def fetch_snapshot(self, job_id):
        job_id = ensure_job_id(job_id)
        (queue_id, status, args, message,
         progress, userdata, time_created, time_updated) = \
            self._redis.hmget(
                id_to_data_key(job_id),
                FIELD_QUEUE_KEY, FIELD_STATUS, FIELD_ARGS,
                FIELD_MESSAGE, FIELD_PROGRESS, FIELD_USERDATA,
                FIELD_TIME_CREATED, FIELD_TIME_UPDATED)
        assert queue_id
        args = json.loads(args)
        job = JobSnapshot(job_id, queue_key_to_type(queue_id), status,
                          args, message or '', progress or '', userdata or '',
                          time_created or '0.0', time_updated or '0.0')
        assert_valid_job(job)
        return job

    def timestamp(self):
        # TODO(cristicbz): Synch and cache timestamps? Non-trivial of course.
        return '.'.join(map(str, self._redis.time()))

    def publish_progress(self, job_id, progress=''):
        job_id = ensure_job_id(job_id)
        self._script_progress(keys=[id_to_data_key(job_id)],
                              args=[progress, self.timestamp()])

    def subscribe_to_jobs(self, job_list):
        pubsub = self._redis.pubsub()
        for job_id in job_list:
            job_id = ensure_job_id(job_id)
            pubsub.subscribe(id_to_data_key(job_id))

        def close():
            pubsub.unsubscribe()

        def generator():
            for msg in pubsub.listen():
                msgt, channel, data = msg['type'], msg['channel'], msg['data']
                job_id = data_key_to_id(channel)
                assert valid_job_id(job_id)
                if msgt == 'message':
                    status, message = data[:STATUS_LEN], data[STATUS_LEN:]
                    if status == STATUS_DONE or status == STATUS_FAILED:
                        pubsub.unsubscribe(channel)
                    yield ProgressNotifcation(job_id, status, message)

        return (generator, close)

    def resolve(self, job_id):
        job_id = ensure_job_id(job_id)
        self._script_resolve(keys=[id_to_data_key(job_id), INPROGRESS_KEY],
                             args=[self.timestamp()])

    def fail(self, job_id, message):
        job_id = ensure_job_id(job_id)
        self._script_fail(keys=[id_to_data_key(job_id), INPROGRESS_KEY],
                          args=[message, self.timestamp()])

    def monitor_inprogress(self):
        key = self._redis.brpoplpush(INPROGRESS_KEY, INPROGRESS_KEY)
        return data_key_to_id(key)

    @property
    def redis_connection(self):
        return self._redis


def make_new_job(job_type, args=None, userdata=None, timestamp='0.0'):
    args = args or []
    job = JobSnapshot(base64.b64encode(os.urandom(18)), job_type,
                      STATUS_PENDING, args, '', '', userdata or '',
                      timestamp, timestamp)
    assert_valid_job(job)
    return job


def assert_valid_job(job):
    assert valid_job_id(job.job_id)
    assert valid_job_type(job.job_type)
    assert valid_args(job.args)
    assert valid_status(job.status)
    assert valid_message(job.message)
    assert valid_progress(job.progress)
    assert valid_userdata(job.progress)


def valid_message(message):
    return isinstance(message, basestring)


def valid_progress(progress):
    return isinstance(progress, basestring)


def valid_job_type(job_type):
    return isinstance(job_type, basestring)


def valid_args(args):
    return (type(args) == list) and \
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
