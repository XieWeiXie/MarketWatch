#!/usr/bin/env python
# encoding: utf-8

"""
__author__: Wuxiaoshen
__software__: PyCharm
__project__:Learn_Scrapy
__file__: remark
__time__: 2017/1/2 13:22
"""
from __future__ import absolute_import

import pymongo
import requests
import re
import time
import hashlib
from lxml import etree
from pprint import pprint
from datetime import datetime
from pymongo import MongoClient
from conf.model import Base, Item, Values
from conf.config import (COLL_BASE, COLL_ITEMS, COLL_VALUES,
                         DB, DB_MARKET, COLLECTION, HOST, PORT, PROXIES)
from multiprocessing.dummy import Pool as ThreadPool
from conf.config import marketwatch_config, USER_AGENT
from random import random, choice
from conf.message import logger
from pymongo import errors


class MarketWatch(object):
    category = "marketwatch"

    def __init__(self):
        self.base_url = "http://www.marketwatch.com/investing/Stock/{}/financials"
        self.coll_base = MongoClient(HOST, PORT)[DB_MARKET][COLL_BASE]
        self.coll_values = MongoClient(HOST, PORT)[DB_MARKET][COLL_VALUES]
        self.coll_items = MongoClient(HOST, PORT)[DB_MARKET][COLL_ITEMS]
        self.coll = MongoClient(HOST, PORT)[DB][COLLECTION]
        self.type = ["Annual", "Quarter"]
        self.keys = ["Income Statement", "Balance Sheet", "Cash Flow Statement"]
        self.url_keys = ["INCOME_ANNUAL_URL", "BALANCE_ANNUAL_URL", "CASH_ANNUAL_URL", "INCOME_QUARTER_URL", "BALANCE_QUARTER_URL","CASH_QUARTER_URL"]
        self.logger = logger
        self.user_agent = choice(USER_AGENT)
        self.coll_base.create_index("ticker", unique=True)
        self.coll_items.create_index([("md5", pymongo.ASCENDING)], unique=True)
        self.coll_values.create_index([("md5_values", pymongo.ASCENDING)], unique=True)
        self.proxies = {"http": PROXIES}

    def urls_ticker(self, ticker):
        try:
            one_tick_urls = [marketwatch_config[url].format(ticker) for url in self.url_keys]
            return one_tick_urls
        except Exception as e:
            self.logger.info("Get conf error: type<{}>, msg<{}>".format(e.__class__, e))

    def ticker_from_db(self):
        coll = self.coll
        try:
            tickers = coll.find({"code": {"$in": [re.compile("_NY_EQ"), re.compile("_NQ_EQ")]}}, {"tick": 1, "_id": 0})
            code_ticker = [ticker["tick"] for ticker in tickers]
            return code_ticker
        except Exception as e:
            self.logger.info("Get ticker from mongodb error: type<{}>, msg<{}>".format(e.__class__, e))
        return ""

    def fetch(self, ticker, number):
        url = self.urls_ticker(ticker)[number]
        html = requests.session()
        html.headers.update({"User-Agent": self.user_agent})
        html.proxies = self.proxies
        try:
            response = html.get(url)
            if response.status_code == requests.codes.ok and response.url == url:
                return response.content
        except Exception as e:
            self.logger.info("Get html error: type<{}>, msg<{}>".format(e.__class__, e))
        pass

    def get_md5(self, dict_item, number=None):
        md = hashlib.md5()
        if number:
            temp_item = ("_".join("%s:%s" % (key, value) for key, value in dict_item.items() if key not in ("ct")) + "_" + str(number)).encode("utf-8")
            md.update(temp_item)
        else:
            temp_item = "_".join("%s:%s" % (key, value) for key, value in dict_item.items() if key not in ("ct")).encode("utf-8")
            md.update(temp_item)
        return md.hexdigest()

    def parse(self, key, type=None):
        """

        :param key: ticker
        :param type:  year or quarter
        :return:
        """
        type=None
        for number in range(6):
            if number < 3:
                type = self.type[0]
            else:
                type = self.type[1]
            response = self.fetch(key, number)
            if isinstance(response, str):
                selector = etree.HTML(response)
            else:
                return
            temp_factor = self.keys[number % 3]
            # 公司代码和名称
            market_and_ticker = selector.xpath('//div[@id="instrumentheader"]')[0]
            name = market_and_ticker.xpath("h1")[0].xpath("string()").strip("\r\n").strip()
            info = market_and_ticker.xpath('p')[0].xpath("string()").strip("\r\n").strip()
            market = info.split(":")[0]
            base_info = {
                "name": name,
                "market": market,
                "ticker": key,
                "key": key,
                "ct": datetime.now()
            }
            last_base = self.coll_base.find_one({"ticker": key, "name": name, "market": market})
            if not last_base:
                try:
                    self.coll_base.insert(base_info)
                except errors.DuplicateKeyError as e:
                    self.logger.info("Get pymongo error1: e.code<{}>,e.details<{}>".format(e.code, e.details))
                    pass

            # 获取表格title信息，包括财年，货币类型，货币单位
            if not selector.xpath("//table"):
                pass
            else:
                detail = ""
                fy = ""
                unit = ""
                currency = ""
                content_topRow = selector.xpath('//div/table[@class]//tr[@class="topRow"][1]')[0]
                content_topRow_one = content_topRow.xpath("th[1]/text()")
                if type == "Annual":
                    detail = content_topRow_one[0]
                    if detail != " ":
                        if "values" in detail:
                            pattern = re.compile(r"Fiscal year is (.*). All values (.*) (.*).")
                            fy, currency, unit = pattern.findall(detail)[0]
                        else:
                            pattern = re.compile(r"Fiscal year is (.*).")
                            fy = pattern.findall(detail)[0]
                            currency = None,
                            unit = None
                    else:
                        fy = None
                        currency = None
                        unit = None
                if type == "Quarter":
                    detail = content_topRow_one[0]
                    if detail != ' ':
                        fy = None
                        pattern = re.compile(r"All values (.*) (.*).")
                        currency, unit = pattern.findall(detail)[0]
                    else:
                        fy = None
                        currency = None
                        unit = None
                years_or_dates_temp = content_topRow.xpath("th[position()>1][position()<6]")
                years_or_dates = [one.text for one in years_or_dates_temp]

                # 获取items信息,提取层级关系
                items_list = []
                content_firstColumn = selector.xpath("//div/table[@class]//tbody/tr/td[1]")    # 第一列
                for one in content_firstColumn:
                    items = {}
                    try:
                        if one.xpath("a")!=[]:
                            items["item"] = one.xpath("a")[0].tail.strip("\r\t").strip()
                            items["serie"] = 1
                            items["level"] = "L1"
                            items["parent"] = None
                            items["type"] = type
                            items_list.append(items)
                        else:
                            if "mainRow" in one.getparent().get("class"):
                                items["item"] = one.text
                                items["serie"] = 1
                                items["level"] = "L1"
                                items["parent"] = None
                                items["type"] = type
                                items_list.append(items)
                            if "totalRow" in one.getparent().get("class"):
                                items["item"] = one.text
                                items["serie"] = 1
                                items["level"] = "L1"
                                items["parent"] = None
                                items["type"] = type
                                items_list.append(items)
                            if "partialSum" in one.getparent().get("class"):
                                items["item"] = one.text
                                items["serie"] = 1
                                items["level"] = "L1"
                                items["parent"] = None
                                items["type"] = type
                                items_list.append(items)
                            if "childRow" in one.getparent().get("class"):
                                items["item"] = one.text
                                items["serie"] = 2
                                items["level"] = "L2"
                                items["parent"] = None
                                items["type"] = type
                                items_list.append(items)
                            elif "rowLevel" in one.getparent().get("class"):
                                tag = one.getparent().get("class")
                                pattern = r"rowLevel-(\d+)"
                                number = re.findall(pattern, tag)[0]
                                items["item"] = one.text
                                items["serie"] = int(number)
                                items["level"] = "L" + str(number)
                                items["parent"] = None
                                items["type"] = type
                                items_list.append(items)
                    except Exception as e:
                        self.logger.info("Get xpath error: type<{}>, msg<{}>".format(e.__class__, e))

                length = len(items_list)
                new_items_list = []
                for one in range(0, length, 1):
                    try:
                        if items_list[one]["serie"] == 1:
                            new_items_list.append(items_list[one])
                        else:
                            j = one
                            while True:
                                j -= 1
                                if (items_list[one]["serie"] - items_list[j]["serie"]) == 1:
                                    items_list[one]["parent"] = items_list[j]["item"]
                                    new_items_list.append(items_list[one])
                                    break
                    except Exception as e:
                        self.logger.info("Get parent field error: type<{}>, msg<{}>".format(e.__class__, e))

                Ratios_items = [item["item"] for item in new_items_list]
                # pprint (Ratios_items)
                # print len(Ratios_items)
                count = 0
                for item in new_items_list:
                    item_info = {
                        "item": Ratios_items[count],#item["item"],
                        "serie": item["serie"],
                        "parent": item["parent"],
                        "type": type,
                        "level": item["level"],
                        "code": None,
                        "ct": datetime.now(),
                        "factor": temp_factor,
                        "ticker": key,
                    }
                    count += 1
                    # print item_info
                    md5_id = self.get_md5(item_info, count)
                    item_info["md5"] = md5_id
                    try:
                        last_one = self.coll_items.find_one({"md5": md5_id})
                        if not last_one:
                            self.coll_items.insert(item_info)
                            if item["parent"] is not None:
                                _id = self.coll_items.find_one({"item": item["parent"]})["_id"]
                                self.coll_items.update_one({"parent": item["parent"]}, {"$set": {"parent": _id}})
                    except errors.DuplicateKeyError as e:
                        self.logger.info("Get pymongo error2: e.code<{}>, e.datails<{}>".format(e.code, e.details))

                # values 信息
                content_columm = selector.xpath("//div/table[@class]//tbody/tr/td[position()>1][position()<6]")
                content_values = []
                for one in content_columm:
                    value = one.xpath('text()|span/text()')[0].lower()
                    if "(" in value:
                        value = "-" + (value[1:-1])
                    if "m" in value:
                        value = int(round(float(value[:-1]), 2) * 10 ** 6)
                    elif "b" in value:
                        value = int(round(float(value[:-1]), 2) * 10 ** 9)
                    elif "t" in value:
                        value = int(round(float(value[:-1]), 2) * 10 ** 12)
                    elif value == "-":
                        value = 0
                    elif "%" in value:
                        pass
                    elif "," in value:
                        value = "".join(value.split(","))
                    else:
                        try:
                            value = int(value)
                        except ValueError:
                            value = float(value)
                        finally:
                            pass
                    content_values.append(value)

                for i in range(len(years_or_dates)):
                    for j in range(len(Ratios_items)):
                        temp = content_values[i:len(content_values):len(years_or_dates)][j]
                        # print temp
                        values_info = {
                            "key": key,  # 股票简称 ： FISV
                            "item": Ratios_items[j],  # 属性：科目
                            "value": temp,  # 值
                            "type": type,  # 年度 or 季度
                            "year": None,  # 年份years_or_dates[i]
                            "date": None,  # 日期
                            "fy": fy,  # 区间
                            "currency": currency,  # 货币单位
                            "unit": unit,  # 单位
                            "detail": detail,  #
                            "ct": datetime.now(),
                            "factor": temp_factor
                        }

                        if type == "Annual":
                            values_info["year"] = years_or_dates[i]
                        else:
                            values_info["date"] = years_or_dates[i]
                        md5_values = self.get_md5(values_info, j)
                        values_info["md5_values"] = md5_values
                        # print values_info
                        last_data = self.coll_values.find_one({"md5_values": md5_values})
                        if not last_data:
                            try:
                                self.coll_values.insert(values_info)
                            except pymongo.errors.DuplicateKeyError as e:
                                self.logger.info("Get pymongo error3: e.code<{}>, e.datails<{}>".format(e.code, e.details))

    def main(self):
        thread_num = 4
        code_ticker = self.ticker_from_db()#[0:100]
        type = self.type
        all_url = [self.urls_ticker(ticker) for ticker in code_ticker]
        pool = ThreadPool(thread_num)
        for i in range(len(code_ticker)):
            pool.apply_async(self.parse, args=(code_ticker[i],))
            self.logger.info("MarketWatch Crawl the ticker is <{},{}>, total tickers<{}>".format(code_ticker[i], i, len(code_ticker)))
        wait_time = random()
        time.sleep(wait_time)
        pool.close()
        pool.join()

if __name__ == '__main__':
    A = MarketWatch()
    A.main()
    # A.parse("CEA")
