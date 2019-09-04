# -*- coding: utf-8 -*-

from Futures.Config import Config
from Futures.DataUtil import DataUtil
from Futures.Util import *
from HModel.Volume import *
import os
import datetime as dt

conf = Config("../test_resources/conf/test-conf.ini")
filename = conf.prop.get("VOLUME", "RESOURCE_FILENAME")
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


vi = WeightedIndex(conf)

vi.init()
# print(sum(row["VolumeIndicator"] for row in vi.get().rows[59:]))
# print("\n".join(map(str, vi.get().rows[59:85])))
res = vi.calculate()
print("vi data shape = (%d, %d)" % (len(vi._data.columns), len(vi._data.rows)), vi._data.columns)
print("vi tmp shape = (%d, %d)" % (len(vi._tmp.columns), len(vi._tmp.rows)), vi._tmp.columns)

print("res data shape = (%d, %d)" % (len(res._data.columns), len(res._data.rows)), res._data.columns)
print("res tmp shape = (%d, %d)" % (len(res._tmp.columns), len(res._tmp.rows)), res._tmp.columns)

print("\n\n\n")

import time

results = []
t0 = time.perf_counter()
for i in range(8):
    for j in range(10, 101, 10):
        res = vi.calculate(i+1, j)
        res_table = res.get()
        res_latest = res_table.rows[-1]
        reserve = res_latest["Reserve"]
        expectation = (reserve / res._INITIAL_RESERVE) ** (1 / 20)
        results += [(expectation, i, j)]

print(time.perf_counter() - t0)

high = 0
for res in results:
    print(res)
    if res[0] > high:
        high = res[0]
        best = res
print()
print(best)
