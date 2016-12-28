# coding=UTF-8
# author: shuqing.zhou
from pyquery import PyQuery
import datetime

    def ais(self, url, type):
        '''
        年度IS表
        '''
        # url = "http://www.marketwatch.com/investing/Stock/{0}/financials".format(self.key)
        print url
        r = self.fetch(url)
        document = PyQuery(r.content)
        if document("#symbollookup") and not document("#instrumentheader"):
            self.is_available = False
            print r.url
            print "=" * 80
            return
        # try:
        instrument_name = document("#instrumentname").text().strip()
        instrument_ticker = document("#instrumentticker").text().strip()
        market = instrument_ticker.split(":")[0].strip()
        tick = instrument_ticker.split(":")[1].strip()
        print instrument_name, market, tick
        try:
            coll_base.insert_one({
                "key": self.key,
                "name": instrument_name,
                "market": market,
                "ticker": instrument_ticker,
                "ct": datetime.datetime.now()
            })
        except pymongo.errors.DuplicateKeyError:
            pass
        title = ""
        data = {}
        items = {}
        last_item = ""
        last_serie = 1
        for table in document("#maincontent .financials .crDataTable").items():
            if table("thead .rowTitle"):
                # 获取表格title信息，包括财年，货币类型，货币单位
                title += table("thead .rowTitle").eq(0).text().strip()
                pattern = re.compile(r"Fiscal year is (.*). All values (.*) (.*).")
                if pattern.findall(title):
                    fy, currency, unit = pattern.findall(title)[0]
                    fy = fy.lower().split("-")
                    for i in range(len(fy)):
                        fy[i] = MONTH[fy[i]]
                    fy = "-".join(fy)
                else:
                    fy, currency, unit = None, None, None
                print fy, currency, unit
            # 获取所有年份
            years = [year.text().strip() for year in table("thead .topRow th").items() if
                     year.attr("scope") == "col" and year.text().strip()]
            for year in years:
                data[year] = []
            for tr in table("tbody tr").items():
                tds = [td for td in tr("td").items()]
                # 数据类型
                item = tds[0].text().strip()
                class_list = tr.attr("class").strip().split(" ")
                # print class_list
                if "partialSum" in class_list or "mainRow" in class_list or "totalRow" in class_list:
                    level = "L1"
                    serie = 1
                else:
                    if class_list[0] == "childRow":
                        class_list[0] = "rowLevel-2"
                    level = "L{0}".format(class_list[0][-1:])
                    serie = int(class_list[0][-1:])
                item_dic = {
                    "item": item,
                    "level": level,
                    "serie": serie,
                    "code": None,
                    "parent": None
                }
                if serie == 1:
                    pass
                else:
                    if int(serie) == int(last_serie + 1):
                        item_dic["parent"] = last_item
                    elif serie == last_serie:
                        item_dic["parent"] = items[last_item]["parent"]
                    elif serie < last_serie:
                        while True:
                            parent_item = items[last_item]["parent"]
                            if not items[last_item]["parent"]:
                                break
                            elif items[parent_item]["serie"] == serie:
                                item_dic["parent"] = items[parent_item]["parent"]
                                break
                items[item] = item_dic
                last_serie = serie
                last_item = item
                for i in range(len(years) - 2, -1, -1):
                    print item
                    year = years[i]
                    value = tds[i + 1].text().strip().lower()
                    value = "".join(value.split(","))
                    print value
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
                            print value
                            value = float(value)
                    print year, value, item
                    data[year].append({
                        "year": year,
                        "level": level,
                        "serie": serie,
                        "item": item,
                        "value": value
                    })

        items_list = [v for v in items.values()]
        items_list.sort(key=lambda k: (k.get('level')))
        for item in items_list:
            if item["parent"] is not None:
                _id = coll_item.find_one({"item": item["parent"]})["_id"]
                item["parent"] = _id
            item["type"] = type
            try:
                coll_item.insert_one(item)
            except pymongo.errors.DuplicateKeyError:
                pass

        for year in data:
            for detail in data[year]:
                uid = populate_md5(detail["item"] + detail["year"] + self.key)
                unique = coll_values.find_one({"uid": uid})
                if unique and unique["value"] == detail["value"]:
                    coll.update_one({"_id": unique["_id"]}, {"$set": {
                        "key": self.key,
                        "item": detail["item"],
                        "value": detail["value"],
                        "uid": uid,
                        "type": type,
                        "year": detail["year"],
                        "date": None,
                        "fy": fy,
                        "currency": currency,
                        "unit": unit,
                        "detail": title,
                        "ct": datetime.datetime.now()
                    }})
                coll_values.insert_one({
                    "key": self.key,
                    "item": detail["item"],
                    "value": detail["value"],
                    "uid": uid,
                    "type": type,
                    "year": detail["year"],
                    "date": None,
                    "fy": fy,
                    "currency": currency,
                    "unit": unit,
                    "detail": title,
                    "ct": datetime.datetime.now()
                })