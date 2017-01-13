#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/13 10:45
"""
from pprint import pprint

from pymongo import MongoClient

class Count(object):
    """
    code field count
    """
    def __init__(self, code, year=None, type=None, factor=None):
        self.code = code
        self.year = year
        self.type = type
        self.factor = factor
        self.collection = MongoClient()["db5"]["values"]
        pass

    def aggregation(self):
        query = [
            {
                "$match": {"key": self.code, "year": self.year, "type": self.type}
            },
            {
                "$group": {"_id": {"code": "$key", "year":"$year"}, "total": {"$sum":1}}
            }

        ]
        result = self.collection.aggregate(pipeline=query)
        return result

    def find(self):
        pass


if __name__ == "__main__":
    total = []
    for code in ["ACH", "IKGH", "ALN", "AMCN", "ATAI", "AXN", "ATV", "BIDU", "BITA", "BORN", "BSPM"]:
        for one in ["2011", '2012', '2013', '2014', "2015"]:
            A = Count(code=code, year=one, type="Annual", factor="Income Statement")
            result = A.aggregation()
            total.append(list(result))
    pprint(total)
    print "*"*80
    tota2 = []
    for code in ["BIDU", "CCM","CHA","CHL","CHU","CO"]:
        # for type in ["Annual", "Quarter"]:
        pipe = [
            {
                "$match": {"key": code, "type": "Annual"}
            },
            {
                "$group": {"_id": {"code": "$key", "type":"$type"}, "total": {"$sum":1}}
            }
        ]
        coll = Count(code=code, type="Annual").collection
        a = coll.aggregate(pipe)
        print (list(a))