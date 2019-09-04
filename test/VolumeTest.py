# -*- coding: utf-8 -*-

from Futures.Config import Config
from Futures.DataUtil import DataUtil
from Futures.Util import *
from HModel.Volume import *
import os
import datetime as dt
import pandas as pd

conf = Config("test-conf.ini")

BASE_DIR = conf.prop.get("VOLUME", "BASE_DIR")
RESOURCE_DIR = conf.prop.get("VOLUME", "RESOURCE_DIR")
filename = os.path.join(BASE_DIR, RESOURCE_DIR, "history_data_for_h_model.csv")
print(type(conf.prop.get("VOLUME", "INTERVAL")))
data_util = DataUtil()
data = data_util.get_data_from_file(filename, 1)
print(data.limit(5))

t = dt.datetime.strptime("2017-08-15 09:01:35.17", "%Y-%m-%d %H:%M:%S.%f")
t_i = dt.datetime.strptime("2017-08-15 09:01:35.7", "%Y-%m-%d %H:%M:%S.%f")

print(t_i - t)

print("%12s: %d" % ("year", t.year))
print("%12s: %d" % ("month", t.month))
print("%12s: %d" % ("day", t.day))
print("%12s: %d" % ("hour", t.hour))
print("%12s: %d" % ("minute", t.minute))
print("%12s: %d" % ("second", t.second))
print("%12s: %d" % ("microsecond", t.microsecond))


cd = ClosingDates(dt.date(2019, 6, 21), "fri", 3)
print("date:", cd.get())
print("isClose in tw:", cd.is_closing())

print("date:", ClosingDates(dt.date(1998, 9, 21), "wed", 3).get())
print("isClose in tw:", ClosingDates(dt.date(1998, 9, 16), "fri", 3).is_closing())

ma_index = MovingAverage(dt.datetime(1998, 9, 1), dt.timedelta(days=1), 60)


def _calc(row):
    time = dt.datetime.strptime(row["Date"], "%Y/%m/%d")
    price = float(row["WeightedIndex"])
    ma_index.update(time, price, price)
    _, ma_value = ma_index.get("price")
    return ma_value


data_1 = data.select(additional_columns={"avg_index": _calc})
data_1.rows = data_1.rows[::-1]

print(data_1.limit(5))

vi = WeightedIndex(conf)
vi._load_data()
vi._calc_ma()
print(vi._data)


excel_file = os.path.join(BASE_DIR, RESOURCE_DIR, "history_data_for_h_model.xlsx")
excel_obj = pd.ExcelFile(excel_file)
main_sheet = pd.read_excel(excel_obj, sheet_name=0)
print()

