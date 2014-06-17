#!/usr/bin/env python
import gevent
from gevent import monkey
monkey.patch_all()

import redis
from reinferio import jobs

done = False


def monitor(job_queue):
    print 'MONITOR UP'
    while not done:
        job_id = job_queue.monitor_inprogress()
        meta = job_queue.fetch_snapshot(job_id)
        print 'MON: %s/%s' % (meta.job_type, meta.args)
        gevent.sleep(.1)

if __name__ == '__main__':
    job_queue = jobs.connect_to_queue()
    gevent.spawn(lambda: monitor(job_queue)).join()
