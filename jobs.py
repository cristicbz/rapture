import base64
import os
import redis

from collections import namedtuple

INPROGRESS_KEY = 'jobs:inprogress'
DATA_KEY_PREFIX = 'jobs:data:'
QUEUE_KEY_PREFIX = 'jobs:queue:'
FIELD_QUEUE_KEY = 'queue_id'
FIELD_STATUS = 'status'
FIELD_BLOB = 'blob'
FIELD_PROGRESS = 'progress'
FIELD_MESSAGE = 'message'
FIELD_TIMESTAMP = 'timestamp'

STATUS_LEN = 1
STATUS_PENDING = 'P'
STATUS_FAILED = 'F'
STATUS_DONE = 'D'

SCRIPT_CONSTANTS = {'f_status': FIELD_STATUS, 'f_queue_id': FIELD_QUEUE_KEY,
                    'f_blob': FIELD_BLOB, 'status_pending': STATUS_PENDING,
                    'status_done': STATUS_DONE, 'f_progress': FIELD_PROGRESS,
                    'f_timestamp': FIELD_TIMESTAMP, 'f_message': FIELD_MESSAGE,
                    'status_failed': STATUS_FAILED}

SCRIPT_NEW = """
local data_key, queue_key = KEYS[1], KEYS[2]
local blob = ARGV[1]
local rv = redis.call('hsetnx', data_key,
                      '%(f_status)s', '%(status_pending)s')
if rv == 0 then
    return 0
end
redis.call('hmset', data_key, '%(f_queue_id)s', queue_key, '%(f_blob)s', blob,
           '%(f_timestamp)s', 0, '%(f_progress)s', '')
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
redis.call('hset', data_key, '%(f_timestamp)s', timestamp)
return 1
""" % SCRIPT_CONSTANTS

SCRIPT_FAIL = """
local data_key, inprogress_key, message = KEYS[1], KEYS[2], ARGV[1]
local rv = redis.call('hget', data_key, '%(f_status)s')
if rv == nil then
    return {err='inexistent job'}
elseif rv ~= '%(status_pending)s' then
    return 0
end
redis.call('hmset', data_key,
           '%(f_status)s', '%(status_failed)s',
           '%(f_message)s', message)
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
redis.call('hset', data_key, '%(f_timestamp)s', timestamp)
if progress ~= '' then
    redis.call('publish', data_key, '%(status_pending)s' .. progress)
end
return 1
""" % SCRIPT_CONSTANTS


JobSnapshot = namedtuple(
    'JobSnapshot', 'job_id job_type status blob message progress timestamp')
ProgressNotifcation = namedtuple('ProgressNotifcation',
                                 'job_id status message')


class JobQueue(object):

    def __init__(self, endpoint, db=None):
        if type(endpoint) == str:
            self._redis = redis.StrictRedis(endpoint, db=(db or 0))
        else:
            assert db is None
            self._redis = endpoint

        self._script_new = self._redis.register_script(SCRIPT_NEW)
        self._script_resolve = self._redis.register_script(SCRIPT_RESOLVE)
        self._script_fail = self._redis.register_script(SCRIPT_FAIL)
        self._script_progress = self._redis.register_script(SCRIPT_PROGRESS)

    def push(self, job_type=None, blob='', job=None):
        assert (job is None) != (job_type is None)
        job = job or make_new_job(job_type, blob)
        assert_valid_job(job)
        assert job.status == STATUS_PENDING
        self._script_new(keys=[id_to_data_key(job.job_id),
                               type_to_queue_key(job.job_type)],
                         args=[job.blob])
        return job

    def pop(self, job_type, timeout=0):
        assert valid_job_type(job_type)
        key = self._redis.brpoplpush(type_to_queue_key(job_type),
                                     INPROGRESS_KEY, timeout)
        return data_key_to_id(key) if key else None

    def fetch_snapshot(self, job_id):
        job_id = ensure_job_id(job_id)
        (queue_id, status, blob, message, progress, timestamp) = \
            self._redis.hmget(
                id_to_data_key(job_id),
                FIELD_QUEUE_KEY, FIELD_STATUS, FIELD_BLOB,
                FIELD_MESSAGE, FIELD_PROGRESS, FIELD_TIMESTAMP)
        assert queue_id
        job = JobSnapshot(job_id, queue_key_to_type(queue_id), status,
                          blob, message or '', progress or '', timestamp or 0)
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
                          args=[message])

    def monitor_inprogress(self):
        key = self._redis.brpoplpush(INPROGRESS_KEY, INPROGRESS_KEY)
        return data_key_to_id(key)

    @property
    def redis_connection(self):
        return self._redis


def make_new_job(job_type, blob=''):
    job = JobSnapshot(base64.b64encode(os.urandom(18)), job_type,
                      STATUS_PENDING, blob, '', '', '0.0')
    assert_valid_job(job)
    return job


def assert_valid_job(job):
    assert valid_job_id(job.job_id)
    assert valid_job_type(job.job_type)
    assert valid_blob(job.blob)
    assert valid_status(job.status)
    assert valid_message(job.message)
    assert valid_progress(job.progress)


def valid_message(message):
    return type(message) == str


def valid_progress(progress):
    return type(progress) == str


def valid_job_type(job_type):
    return type(job_type) == str


def valid_blob(blob):
    return type(blob) == str


def valid_status(status):
    return status in (STATUS_PENDING, STATUS_FAILED, STATUS_DONE)


def valid_job_id(job_id):
    return type(job_id) == str


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
    if type(job_or_id) == str:
        assert valid_job_id(job_or_id)
        return job_or_id
    else:
        return job_or_id.job_id
