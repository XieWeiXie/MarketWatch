#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2016/12/28 12:49
"""
from pprint import pprint

from pymongo import MongoClient
import re


coll = MongoClient("192.168.100.20")["ada"]["base_stock"]
all_ticks = coll.find({"code": {"$in": [re.compile("_NY_EQ"), re.compile("_NQ_EQ")]}}, {"tick": 1, "_id": 0})
all = [one["tick"] for one in all_ticks]
print (len(all))
one = coll.find_one({"code": {"$in": [re.compile("_NY_EQ"), re.compile("_NQ_EQ")]}}, {"tick": 1, "_id": 0})
print (not one)
pprint(all)