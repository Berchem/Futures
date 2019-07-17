# -*- coding: UTF-8 -*-
import os
import unittest
from Futures.Util import *
from Futures.DataUtil import DataUtil


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
        I020 = [line.strip('\n').split(",") for line in open(filename)][1:]
        MAarray = []
        MA = []
        STime = time_to_num('08450000')
        Cycle = 6000
        MAlen = 5

        for i in I020:
            time = i[0]
            price = int(i[4])
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
        I020 = [line.strip('\n').split(",") for line in open(filename)][1:]
        high = int(I020[0][4])
        low = int(I020[0][4])
        high_low_list = [(I020[0][0], high, low)]
        for i in I020[1:]:
            price = int(i[4])
            if price > high:
                high = price
            if price < low:
                low = price
            high_low_list.extend([(i[0], high, low)])
        return high_low_list

    def test_high_low_price(self):
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
        I020 = [line.strip('\n').split(",") for line in open(filename)][1:]
        lastPrice = int(I020[0][4])
        outDesk = 0
        inDesk = 0
        sell_buy_list = [(I020[0][0], lastPrice, inDesk, outDesk)]
        for i in I020[1:]:
            price = int(i[4])
            qty = int(i[5])
            if price > lastPrice:
                outDesk += qty
            if price < lastPrice:
                inDesk += qty
            lastPrice = price
            sell_buy_list.extend([(i[0], price, inDesk, outDesk)])
        return sell_buy_list

    def test_sell_buy_volume(self):
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

    def test_mix_index(self):
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        data = self.data_util.get_data_from_file(filename, 1)
        ma_obj = MovingAverage(10, 6000, "8450000")
        high_low_obj = HighLowPrice()
        sell_buy_obj = SellBuyVolume()

        time_list = []
        current_price_list = []
        ma_10_list = []
        high_price_list = []
        low_price_list = []
        sell_list = []
        buy_list = []

        for row in data.rows:
            time = row["INFO_TIME"]
            price = int(row["PRICE"])
            volume = int(row["QTY"])

            ma_obj.update(time, price)
            high_low_obj.update(time, price)
            sell_buy_obj.update(time, price, volume)

            _, ma_value = ma_obj.get()
            _, high, low = high_low_obj.get()
            _, current_price, sell, buy = sell_buy_obj.get()

            time_list += [time]
            current_price_list += [current_price]
            high_price_list += [high]
            low_price_list += [low]
            sell_list += [sell]
            buy_list += [buy]

        # graph

    @staticmethod
    def ohlc_per_min(filename):
        I020 = [line.strip('\n').split(",") for line in open(filename)][1:]
        OHLC = []
        for MatchInfo in I020:
            MatchInfo[0] = MatchInfo[0].zfill(8)
            HMTime = MatchInfo[0][0:2] + MatchInfo[0][2:4]
            MatchPrice = int(MatchInfo[4])
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

    def test_ohlc(self):
        from MypseudoSQL import Table
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        data = self.data_util.get_data_from_file(filename, 1)
        ohlc_obj = OpenHighLowClose(6000)
        temp = Table(["time", "open", "high", "low", "close"])
        for row in data.rows:
            ohlc_obj.update(row["INFO_TIME"], row["PRICE"])
            # print ohlc_obj.get()
            temp.insert(ohlc_obj.get())

        print temp.where(lambda row: row["time"] == "08460000").rows[-1]

        # print temp.group_by(["time"], {
        #     "open": lambda rows: rows[-1]["open"],
        #     "high": lambda rows: rows[-1]["high"],
        #     "low": lambda rows: rows[-1]["low"],
        #     "close": lambda rows: rows[-1]["close"]
        # }).order_by(lambda row: row["time"]).limit(5)

        ohlc = self.ohlc_per_min(filename)
        print "\n\n"
        for i in xrange(5):
            print ohlc[i]


if __name__ == '__main__':
    unittest.main()
