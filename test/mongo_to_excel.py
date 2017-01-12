#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/12 10:08
"""
from pymongo import MongoClient
from xlwt import Workbook
from xlwt import easyxf


class MongoToXls(object):

    def __init__(self, collection, name):
        self.work_book = collection
        self.sheet_name = name
        self.wb = Workbook()
        self.ws = [self.wb.add_sheet(one) for one in self.sheet_name]
        self.style = easyxf("align: vert centre, horiz center")
        pass

    def info(self, number):
        self.contents = list(self.work_book[number].find())
        self.headers = [key for key in self.contents[0].keys()]
        rows = len(self.contents)
        columns = len(self.headers)
        return rows, columns

    def write_header(self, number):
        ws = self.ws[number]
        _, columns = self.info(number)
        for i in range(0, columns):
            ws.write(0, i, self.headers[i], style=self.style)

    def write_content(self, number):
        ws = self.ws[number]
        rows, columns = self.info(number)
        if rows >= 65536:
            rows = 65535
        for j in range(1, rows + 1):
            for k in range(0, columns):
                ws.write(j, k, str(self.contents[j - 1][self.headers[k]]), style=self.style)

    def save(self):
        name = "_".join(self.sheet_name) +"2"+ str(".xls")
        self.wb.save(str(name))

collection1 = MongoClient()["db5"]["base"]
collection2 = MongoClient()["db5"]["items"]
collection3 = MongoClient()["db5"]["values"]
collection = [collection1, collection2, collection3]
name = ["base", "items", "values"]

A = MongoToXls(collection=collection, name=name)
for number in range(len(collection)):
    print A.info(number)
    A.write_header(number)
    A.write_content(number)
A.save()