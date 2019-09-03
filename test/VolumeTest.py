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


# print(sum(row["VolumeIndicator"] for row in vi.get().rows[59:]))
# print("\n".join(map(str, vi.get().rows[59:85])))

res = vi.calculate()
res_table = res.get()
print(res_table)

# print(len(res._tmp.rows))

