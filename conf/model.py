#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/3 10:22
"""
from mongoengine import *
from datetime import datetime


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