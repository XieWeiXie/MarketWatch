#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2016/12/28 13:02
"""
import requests
import re
from conf.config import HOST, DB, PORT, COLLECTION
from lxml import etree
from conf.config import marketwatch_config
from pymongo import MongoClient


class MarketCrawler(object):

    category = "marketwatch"

    def __init__(self):
        # db setting
        self.collection = MongoClient(str(HOST), PORT)[DB][COLLECTION]

        # Field
        self.field = ["Name", "Market", "Ticker", ]

        # headers
        self.headers = marketwatch_config["headers"]
        pass

    def get_tick_from_db(self):
        coll = self.collection
        ticks = coll.find({"code": {"$in": [re.compile("_NY_EQ"), re.compile("_NQ_EQ")]}}, {"tick": 1, "_id": 0})
        code_tick = [one["tick"] for one in ticks]
        return code_tick

    def get_url(self, tick="FISV", keyword="INCOME_ANNUAL_URL"):
        """
        comment by paul.xie
        :param tick:
        :param keyword:
        :return:
        """
        url = marketwatch_config[keyword].format(tick)
        return url

    def download(self):
        """
        comment by paul.xie
        :return:
        """
        url = self.get_url()
        try:
            raw_html = requests.get(url, headers=self.headers)
            response = raw_html.text
            return response
        except:
            print ("error")

    def parse(self):
        seletor = etree.HTML(self.download())
        
        pass


if __name__ == "__main__":
    marketwatch = MarketCrawler()
    url = marketwatch.get_url("FISV", "INCOME_QUARTER_URL")
    print (url)
    marketwatch.download()
