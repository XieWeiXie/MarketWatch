#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/10 14:21
"""
import xlwt
from pymongo import MongoClient
from datetime import datetime
collection1 = MongoClient()["db5"]["base"]
collection2 = MongoClient()["db5"]["items"]
collection3 = MongoClient()["db5"]["values"]
wb = xlwt.Workbook()
wb.encoding = "utf-8"
ws_base = wb.add_sheet("base")
ws_items = wb.add_sheet("items")
ws_values = wb.add_sheet("values")


base_data = list(collection1.find())
length1 = len(base_data)
items_data = list(collection2.find())
length2 = len(items_data)
values_data = list(collection3.find())
length3 = len(values_data)

def write(sheet, data, r, c):
    value = [one for one in data.values()]
    map(lambda i:sheet.write(r, i, str(value[i])),[i for i in range(c)])

for row in range(0, length1):
    if length3 > 65536:
        length3 = 65536
    write(ws_base, base_data[row], row, 6)

for row in range(0, length2):
    if length3 > 65536:
        length3 = 65536
    write(ws_items, items_data[row], row, 11)

for row in range(0, length3):
    if length3 > 65536:
        length3 = 65536

    write(ws_values, values_data[row], row, 14)

wb.save("base_item_values_new.xls")