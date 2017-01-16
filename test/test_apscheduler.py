#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/16 12:43
"""
from apscheduler.schedulers.blocking import BlockingScheduler

import datetime
import logging
logging.basicConfig()

def aps_test(str):
    print datetime.datetime.now().strftime("%Y:%m:%d--%H:%M:%S"), str

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    scheduler.add_job(aps_test, args=("循环任务",), trigger='interval', seconds=5)
    # scheduler.add_job(aps_test, trigger="cron", second="*/5", hour="*")
    scheduler.add_job(aps_test, args=("一次性任务",), next_run_time=datetime.datetime.now())
    scheduler.add_job(aps_test, args=("定时任务",), trigger="cron", second="*/10")
    scheduler.start()
