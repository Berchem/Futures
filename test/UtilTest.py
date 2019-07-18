# -*- coding: UTF-8 -*-
import os
import unittest
from Futures.Util import *
from Futures.DataUtil import DataUtil
from MypseudoSQL import Table


class UtilTest(unittest.TestCase):
    data_util = DataUtil()
    test_resource_path = "../test_resources/futures_data_sample"

    def test_time_to_num(self):
        self.assertEqual(time_to_num("8450830"), 3150830)
        # 1 sec: 100 num
        self.assertEqual(time_to_num("8450930"), 3150930)
        # 1 min: 6000 num
        self.assertEqual(time_to_num("8460830"), 3156830)
        self.assertEqual(time_to_num("13300000"), 4860000)

    def test_num_to_time(self):
        self.assertEqual(num_to_time(3150830), "08450830")

    @staticmethod
    def ma_example(filename):
        # 24.py
        I020 = [line.strip('\n').split(",") for line in open(filename)]
        index_time = I020[0].index("INFO_TIME")
        index_price = I020[0].index("PRICE")
        I020 = I020[1:]
        MAarray = []
        MA = []
        STime = time_to_num('08450000')
        Cycle = 6000
        MAlen = 5

        for i in I020:
            time = i[index_time]
            price = int(i[index_price])
            if len(MAarray) == 0:
                MAarray += [price]
            else:
                if time_to_num(time) < STime + Cycle:
                    MAarray[-1] = price
                else:
                    if len(MAarray) == MAlen:
                        MAarray = MAarray[1:] + [price]
                    else:
                        MAarray += [price]
                    STime = STime + Cycle
            MAValue = float(sum(MAarray)) / len(MAarray)
            MA.extend([(time, MAValue)])
        return MA

    def test_MovingAverage(self):
        filename = os.path.join(self.test_resource_path, 'MATCH', 'Futures_20170815_I020.csv')
        # actual
        ma_list_example = self.ma_example(filename)
        # expect
        ma_obj = MovingAverage(5, 6000, "8450000")
        data = self.data_util.get_data_from_file(filename, True)
        ma_list = []
        for row in data.rows:
            ma_obj.update(row["INFO_TIME"], int(row["PRICE"]))
            ma_list.extend([ma_obj.get()])
        # assertion
        self.assertEqual(ma_list, ma_list_example)
        self.assertRaises(Exception, ma_obj.update, "123", 456)

    @staticmethod
    def high_low_price(filename):
        I020 = [line.strip('\n').split(",") for line in open(filename)]
        index_time = I020[0].index("INFO_TIME")
        index_price = I020[0].index("PRICE")
        I020 = I020[1:]
        high = int(I020[0][index_price])
        low = int(I020[0][index_price])
        high_low_list = [(I020[0][index_time], high, low)]
        for i in I020[1:]:
            price = int(i[index_price])
            if price > high:
                high = price
            if price < low:
                low = price
            high_low_list.extend([(i[index_time], high, low)])
        return high_low_list

    def test_HighLowPrice(self):
        filename = os.path.join(self.test_resource_path, 'MATCH', 'Futures_20170815_I020.csv')
        # actual
        high_low_list_example = self.high_low_price(filename)
        # expect
        data = self.data_util.get_data_from_file(filename, True)
        high_low_obj = HighLowPrice()
        high_low_list = []
        for row in data.rows:
            high_low_obj.update(row["INFO_TIME"], int(row["PRICE"]))
            high_low_list.extend([high_low_obj.get()])
        # assert
        self.assertEqual(high_low_list, high_low_list_example)
        self.assertRaises(Exception, high_low_obj.update, data.rows[0]["INFO_TIME"], 1)

    @staticmethod
    def sell_buy_volume(filename):
        I020 = [line.strip('\n').split(",") for line in open(filename)]
        index_time = I020[0].index("INFO_TIME")
        index_price = I020[0].index("PRICE")
        index_qty = I020[0].index("QTY")
        I020 = I020[1:]
        lastPrice = int(I020[0][index_price])
        outDesk = 0
        inDesk = 0
        sell_buy_list = [(I020[0][index_time], lastPrice, inDesk, outDesk)]
        for i in I020[1:]:
            price = int(i[index_price])
            qty = int(i[index_qty])
            if price > lastPrice:
                outDesk += qty
            if price < lastPrice:
                inDesk += qty
            lastPrice = price
            sell_buy_list.extend([(i[index_time], price, inDesk, outDesk)])
        return sell_buy_list

    def test_SellBuyVolume(self):
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        # actual
        sell_buy_list_example = self.sell_buy_volume(filename)
        # expect
        data = self.data_util.get_data_from_file(filename, 1)
        sell_buy_obj = SellBuyVolume()
        sell_buy_list = []
        for row in data.rows:
            sell_buy_obj.update(row["INFO_TIME"], int(row["PRICE"]), int(row["QTY"]))
            sell_buy_list += [sell_buy_obj.get()]
        # assert
        self.assertEqual(sell_buy_list, sell_buy_list_example)
        self.assertRaises(Exception, sell_buy_obj.update, data.rows[0]["INFO_TIME"], 1, 1)

    @staticmethod
    def ohlc_per_min(filename):
        I020 = [line.strip('\n').split(",") for line in open(filename)]
        index_time = I020[0].index("INFO_TIME")
        index_price = I020[0].index("PRICE")
        I020 = I020[1:]
        OHLC = []
        for MatchInfo in I020:
            MatchInfo[index_time] = MatchInfo[index_time].zfill(8)
            HMTime = MatchInfo[index_time][0:2] + MatchInfo[index_time][2:4]
            MatchPrice = int(MatchInfo[index_price])
            if len(OHLC) == 0:
                OHLC.append([HMTime, MatchPrice, MatchPrice, MatchPrice, MatchPrice])
            else:
                if HMTime == OHLC[-1][0]:
                    if MatchPrice > OHLC[-1][2]:
                        OHLC[-1][2] = MatchPrice
                    if MatchPrice < OHLC[-1][3]:
                        OHLC[-1][3] = MatchPrice
                    OHLC[-1][4] = MatchPrice
                else:
                    OHLC.append([HMTime, MatchPrice, MatchPrice, MatchPrice, MatchPrice])
            # print OHLC[-1]
        return OHLC

    def generate_example_result(self, filename, period):
        result = self.ohlc_per_min(filename)
        filter_list = []
        for i, record in enumerate(result):
            if i % period == 0:
                _time = record[0]
                _open = record[1]
                end = i + period if i + period < len(result) else len(result)
                _high = max(val[2] for val in result[i:end])
                _low = min(val[3] for val in result[i:end])
                _close = result[end - 1][-1]
                filter_list += [[_time, _open, _high, _low, _close]]
        return filter_list

    def generate_ohlc_result(self, filename, period, initial_time):
        data = self.data_util.get_data_from_file(filename, 1)
        ohlc_obj = OpenHighLowClose(period * 6000, initial_time)
        ohlc_table = Table(["time", "open", "high", "low", "close"])
        for row in data.rows:
            ohlc_obj.update(row["INFO_TIME"], int(row["PRICE"]))
            ohlc_table.insert(ohlc_obj.get())
        ohlc_group = ohlc_table.group_by(
            group_by_columns=["time"],
            aggregates={
                "open": lambda rows: rows[-1]["open"],
                "high": lambda rows: rows[-1]["high"],
                "low": lambda rows: rows[-1]["low"],
                "close": lambda rows: rows[-1]["close"]
            }
        ).order_by(lambda row: row["time"])
        return ohlc_group

    def test_OpenHighLowClose(self):
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        period = 65  # minute
        # actual
        ohlc_example = self.generate_example_result(filename, period)
        # expect
        ohlc_group = self.generate_ohlc_result(filename, period, "8450000")
        ohlc_list = [[row["time"][:4], row["open"], row["high"], row["low"], row["close"]] for row in ohlc_group.rows]
        # assert
        self.assertEqual(ohlc_list, ohlc_example)

        print "\n\n"
        for i in xrange(len(ohlc_list)):
            print "\nexpect: %s\nactual: %s" % (ohlc_list[i], ohlc_example[i])

    # def test_(self):
    #     from Futures.Config import Config
    #     path = os.path.dirname(self.test_resource_path)
    #     path = os.path.join(path, "conf", "conf.properties")
    #     conf = Config(path)
    #     database = conf.prop.get("SQLITE", "DATABASE")
    #     match_table_name = conf.prop.get("SQLITE", "MATCH_TABLE")
    #     data = self.data_util.get_data_from_sqlite(database, match_table_name)
    #     select = data.select(["DATE", "INFO_TIME", "PRICE"])
    #     for i in xrange(45308, 45313):
    #         print select.rows[i]
    #
    #     print time_to_num("")
    #     print time_to_num("000")
    #     print time_to_num("00000000")
    #     print time_to_num("24000000")


if __name__ == '__main__':
    unittest.main()
