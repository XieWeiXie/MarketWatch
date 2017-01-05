#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2016/12/28 12:55
"""

import sys
import socket


def make_dev_ip():
    """
    :return: the actual ip of the local machine.
        This code figures out what source address would be used if some traffic
        were to be sent out to some well known address on the Internet. In this
        case, a Google DNS server is used, but the specific address does not
        matter much.  No traffic is actually sent.
    """
    try:
        _socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        _socket.connect(('8.8.8.8', 80))
        address, port = _socket.getsockname()
        _socket.close()
        return address
    except socket.error:
        return '127.0.0.1'

_DEV_ENV = ['192.168.1.22',]

if make_dev_ip() in _DEV_ENV:
    HOST = '192.168.100.20'
    #HOST = "127.0.0.1"
    PORT = 27017
    DB = 'ada'
    COLLECTION = 'base_stock'
    COLL_BASE = "base"
    COLL_TEMPLATE = "items_template"
    COLL_ITEMS = "items"
    COLL_VALUES = "values"
    COLL = "marketwatch"
    PROXIES = "http://192.168.250.130:3128"

USER_AGENT = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.3319.102 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36',

    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0',
    'Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20120101 Firefox/29.0',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/29.0',

    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0b11pre) Gecko/20110128 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b11pre) Gecko/20110131 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b11pre) Gecko/20110129 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b11pre) Gecko/20110128 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; rv:2.0b11pre) Gecko/20110126 Firefox/4.0b11pre',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b10pre) Gecko/20110118 Firefox/4.0b10pre',
]

BASE_URL = "http://www.marketwatch.com/investing/Stock/{}/financials"

marketwatch_config = {
    # Income statement
    "INCOME_ANNUAL_URL": BASE_URL,
    "INCOME_QUARTER_URL": BASE_URL+"/income/quarter",

    # Balance Sheet
    "BALANCE_ANNUAL_URL": BASE_URL + "/balance-sheet",
    "BALANCE_QUARTER_URL": BASE_URL+"/balance-sheet/quarter",

    # Cash Flow Statement
    "CASH_ANNUAL_URL": BASE_URL+"/cash-flow",
    "CASH_QUARTER_URL": BASE_URL+"/cash-flow/quarter",

    # headers
    "headers": {
        "Host": "www.marketwatch.com",
    }
}