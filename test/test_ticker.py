#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/12 11:32
"""
import requests
from lxml import etree

url = 'http://www.marketwatch.com/investing/Stock/CBP/financials'
html = requests.session()
response = html.get(url).content
selector = etree.HTML(response)
one = selector.xpath('//div/table[@class]//tr[@class="topRow"][1]/th[position()>1][position()<6]')
years_or_dates = [i.text for i in one]
print years_or_dates,len(years_or_dates)
print years_or_dates[0]