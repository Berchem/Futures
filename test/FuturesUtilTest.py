import unittest
from Futures.Config import Config
from Futures.SQLiteUtil import SQLiteUtil
from Futures.DataUtil import DataUtil


class FutureUtilTest(unittest.TestCase):
    conf = Config("../test_resources/conf/conf.properties")
    match_table = conf.prop.get("SQLITE", "MATCH_TABLE")
    commission_table = conf.prop.get("SQLITE", "COMMISSION_TABLE")
    up_down_5_table = conf.prop.get("SQLITE", "UP_DOWN_5_TABLE")
    database = conf.prop.get("SQLITE", "DATABASE")
    data_util = DataUtil()

    def test_high_low(self):
        match = self.data_util.get_data_from_sqlite(self.database, "test_table")
        first_row = match.rows[0]
        high = int(first_row["PRICE"])
        low = int(first_row["PRICE"])

        for row in match.where(lambda row: row["INFO_TIME"] < "8450057").rows:
            price = int(row["PRICE"])
            if price > high:
                high = price
            if price < low:
                low = price
            print "Time:", row["INFO_TIME"], "Price", price, "High", high, "Low", low



        # print match.select(["INFO_TIME", "MATCH_TIME"]).limit(10)

