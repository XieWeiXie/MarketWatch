#!/usr/bin/env python
# encoding: utf-8

"""
__author__: Wuxiaoshen
__software__: PyCharm
__project__:Learn_Scrapy
__file__: remark
__time__: 2017/1/2 13:22
"""
import requests
import re
from lxml import etree
from mongoengine import *
from pprint import pprint
from datetime import datetime
connect("col")


class Base(Document):
    name = StringField(required=True)
    market = StringField(required=True)
    ticker = StringField(required=True)
    key = StringField(required=True)
    ct = DateTimeField(default=datetime.now(), required=True)
    pass


class Item(Document):
    item = StringField(required=True)
    serie = IntField(required=True)
    parent = StringField(default=None, required=True)
    type = StringField(required=True)
    code = StringField(default=None)
    level = IntField(required=True)
    ct = DateTimeField(default=datetime.now())
    pass


class Values(Document):
    year = StringField(required=True)
    fy = StringField(required=True)
    key = StringField(required=True)
    ct = DateTimeField(required=True)
    detail = StringField(required=True)
    item = StringField(required=True)
    date = StringField(required=True, default=None)
    type = StringField(required=True)
    unit = StringField(required=True)
    currency = StringField(required=True)
    value = IntField(required=True)
    pass


class MarketWatch(object):

    def __init__(self):
        self.base_url = "http://www.marketwatch.com/investing/Stock/{}/financials"
        pass

    def fetch(self, tick):
        url = self.base_url.format(tick)
        html = requests.session()
        response = html.get(url)
        if response.status_code == requests.codes.ok:
            return response.content
        pass

    def parse(self, key, type):
        reponse = self.fetch(key)
        selector = etree.HTML(reponse)

        # 公司代码和名称
        market_and_ticker = selector.xpath('//div[@id="instrumentheader"]')[0]
        name = market_and_ticker.xpath("h1")[0].xpath("string()").strip("\r\n").strip()
        info = market_and_ticker.xpath('p')[0].xpath("string()").strip("\r\n").strip()
        market = info.split(":")[0]
        base_info = Base(
            name=name,
            market=market,
            ticker=info,
            key=key,
            ct=datetime.now()
        )
        base_info.save()

        # 获取表格title信息，包括财年，货币类型，货币单位
        detail = ""
        fy = ""
        unit = ""
        currency = ""
        content_topRow = selector.xpath('//div/table[@class]//tr[@class="topRow"][1]')[0]
        content_topRow_one = content_topRow.xpath("th[1]/text()")
        if type == "Annual":
            detail = content_topRow_one[0]
            pattern = re.compile(r"Fiscal year is (.*). All values (.*) (.*).")
            fy, currency, unit = pattern.findall(detail)[0]
        if type == "Quarter":
            detail = content_topRow_one[0]
            fy = None
            pattern = re.compile(r"All values (.*) (.*).")
            currency, unit = pattern.findall(detail)[0]
        print(detail, "+", fy, "+", currency, "+", unit)
        years_or_dates = content_topRow.xpath("th[position()>1][position()<6]/text()")
        # pprint(content_topRow_one)
        # pprint(years_or_dates)

        # 获取items信息,提取层级关系
        items_list = []
        content_firstColumn = selector.xpath("//div/table[@class]//tbody/tr/td[1]")    # 第一列
        for one in content_firstColumn:
            items = {}
            if one.xpath("a")!=[]:
                items["item"] = one.xpath("a")[0].tail.strip("\r\t").strip()
                items["serie"] = 1
                items["level"] = "L1"
                items["parent"] = None
                items["type"] = type
                items_list.append(items)
            else:
                items["item"] = one.text
                if "mainRow" in one.getparent().get("class"):
                    items["serie"] = 1
                    items["level"] = "L1"
                    items["parent"] = None
                    items["type"] = type
                    items_list.append(items)
                if "childRow" in one.getparent().get("class"):
                    items["serie"] = 2
                    items["level"] = "L2"
                    items["parent"] = None
                    items["type"] = type
                    items_list.append(items)
                elif "rowLevel" in one.getparent().get("class"):
                    tag = one.getparent().get("class")
                    pattern = r"rowLevel-(\d+)"
                    number = re.findall(pattern, tag)[0]
                    items["serie"] = int(number)
                    items["level"] = "L" + str(number)
                    items["parent"] = None
                    items["type"] = type
                    items_list.append(items)
        length = len(items_list)
        new_items_list = []
        for one in range(0, length, 1):
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
        # try:
        #     for item in new_items_list:
        #         item_info = Item(
        #             item=item["item"],
        #             serie=item["serie"],
        #             parent=item["parent"],
        #             type=item["type"],
        #             level=item["level"],
        #             code=None,
        #             ct=datetime.now()
        #         )
        #         item_info.save()
        # except:
        #     pass
        # values 信息
        content_columm = selector.xpath("//div/table[@class]//tbody/tr/td[position()>1][position()<6]")
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
            else:
                try:
                    value = int(value)
                except ValueError:
                    print(value)
                    value = float(value)
            print(value)



if __name__ == '__main__':
    A = MarketWatch()
    b = A.parse("FISV", type="Annual")
    # pprint(b),print(len(b))
    pass