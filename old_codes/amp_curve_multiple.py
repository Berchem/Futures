#! python3.6
import time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from load_database import Load
from MypseudoSQL import Table

"""
since 2017/1/1
get the average records per minute ()
get the close price every minute
calculate the amplitude % critical curve
this script would depend on day trading 
"""


class PseudoTable(Table):
    def __init__(self, columns):
        Table.__init__(self, columns)

    def parsing_datetime(self):
        def convert_datetime(row):
            return datetime.strptime(row["date"] + "T" + row["time"], "%Y/%m/%dT%H:%M:%S")

        def convert_date(row):
            return datetime.strptime(row["date"], "%Y/%m/%d")

        parsing = self. \
            select(keep_columns=['open', 'high', 'low', 'close', 'total_volume'],
                   additional_columns={"datetime": convert_datetime,
                                       "date": convert_date}). \
            order_by(lambda row: row["datetime"])

        result = PseudoTable(parsing.columns)
        result.rows = parsing.rows

        return result

    def one_day_filter(self, date):
        result = PseudoTable(self.columns)
        result.rows = list(filter(lambda row: row["date"] == date, self.rows))

        return result

    def ma_calc(self, column, m=5, n=5):
        time_scale = [row["datetime"] for row in self.rows][m * n - 1::n]
        series = [row[column] for row in self.rows]
        block_avg = [sum(series[i:i+n])/float(n) for i in range(0, len(series) // n * n, n)]
        moving_avg = [sum(block_avg[i:i+m])/float(m) for i in range(len(block_avg) - m + 1)]
        return time_scale, moving_avg


"""
use tick_9_2.db
select * from tick_min_log
"""

# connect sqlite
src = "data/tick_9_2.db"
db = Load(src)

# fetch data
raw_data = db.get_per_min(start="2017-01-01", drop_night_trade=True)
raw_close = db.get_per_min(start="2017-01-01", drop_night_trade=True, predicate="group by Date;")

# create pseudoSQL
raw_columns = ['date', 'time', 'open', 'high', 'low', 'close', 'total_volume']
data = PseudoTable(raw_columns)
for row in raw_data:
    data.insert(row)

# parsing date, time format to datetime
data = data.parsing_datetime()

close = PseudoTable(raw_columns)
for row in raw_close:
    close.insert(row)

close = close.parsing_datetime()

amp = data.select(["datetime", "date", "open", "close"], {"amp": lambda row: abs(row["open"] - row["close"])})

# print(amp)

# import pandas as pd
#
# df = pd.DataFrame(close.rows)
#
# date = df.date
#
# print(date)
# print(date.shift(1))
# print(df.to_dict(orient='record'))
close_adj = close.select(additional_columns={"date_adj": lambda row: row["date"]})
for i, row in enumerate(close_adj.rows[:-1]):
    row["date_adj"] = close.rows[i + 1]["date"]

keep = close.columns
keep.pop(keep.index("date"))
close = close_adj.select([], additional_columns={"date": lambda row: row["date_adj"],
                                                 "pre_close": lambda row: row["close"]})
print(close)
join = amp.join(close, True)

print(join.limit(1500))

# amp_1 = amp.join(close_1)
# print(amp.limit(5))
# print(close_1.limit(5))
# print(amp_1.limit(5))
# print(len(amp.rows), len(amp_1.rows))

# for m in [5, 35, 200]:
#     x, y = data.ma_calc("close", m, 5)
#     plt.plot(x, y)
#
# plt.legend([5, 35, 200])
# plt.xticks(rotation=45)
# plt.show()
