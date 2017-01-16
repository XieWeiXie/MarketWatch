#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/16 13:37
"""
from crawler.market.remark import MarketWatch
from conf.message import logger
from conf.mail import SendEmail
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import time

def spider_index():
    today = datetime.datetime.now().strftime("%Y:%m:%d")
    marketwatch_time = time.time()
    marketwatch = MarketWatch()
    marketwatch.main()
    logger.info("MarketWatch site crawl Done!\n")
    marketwatch_end = time.time()
    subject = u"美股数据抓取信息"
    message = u"美股抓取信息抓取开始日期：%s 耗时：%s\n" % (today, (marketwatch_end - marketwatch_time))
    SendEmail(subject=subject, message=message)

# scheduler = BlockingScheduler()
# scheduler.add_job(func=spider_index, trigger='interval', )


if __name__ == "__main__":
    spider_index()


