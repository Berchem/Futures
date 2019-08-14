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

# -----------------------------------------------------------------
    @staticmethod
    def ma_price_example(filename, period, interval):
        # 24.py
        I020 = [line.strip('\n').split(",") for line in open(filename)]
        index_time = I020[0].index("INFO_TIME")
        index_price = I020[0].index("PRICE")
        I020 = I020[1:]
        MAarray = []
        MA = []
        STime = time_to_num('08450000')
        Cycle = period
        MAlen = interval

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

    @staticmethod
    def ma_volume_example(filename, period, interval):
        i020 = [line.strip('\n').split(",") for line in open(filename)]
        index_time = i020[0].index("INFO_TIME")
        index_volume = i020[0].index("AMOUNT")
        i020 = i020[1:]
        qty = []
        last_time = ""
        last_volume = 0
        result = []

        for i in i020:
            info_time = i[index_time].zfill(8)
            time = info_time[0:2] + info_time[2:4]
            volume = int(i[index_volume])

            if len(qty) == 0:
                qty += [0]
                last_time = time
                last_volume = volume

            else:
                if time == last_time:
                    qty[-1] = volume - last_volume

                else:
                    if len(qty) == interval:
                        # print(time, qty, q_ma, "WTF")
                        qty = qty[1:] + [0]

                    else:
                        qty += [0]

                    last_time = time
                    last_volume = volume
            q_ma = sum(qty) / len(qty)
            result.extend([(i[index_time], q_ma)])
        return result

    def test_MovingAverage(self):
        filename = os.path.join(self.test_resource_path, 'MATCH', 'Futures_20170815_I020.csv')
        period = 6000
        interval = 3
        # actual
        ma_price_example = self.ma_price_example(filename, period, interval)
        ma_volume_example = self.ma_volume_example(filename, period, interval)
        # expect
        ma_obj = MovingAverage("8450000", period, interval)
        data = self.data_util.get_data_from_file(filename, True)
        ma_price_list = []
        ma_volume_list = []
        for row in data.rows:
            ma_obj.update(row["INFO_TIME"], int(row["PRICE"]), int(row["AMOUNT"]))
            ma_price_list.extend([ma_obj.get("price")])
            ma_volume_list.extend([ma_obj.get("volume")])

        # assertion
        self.assertEqual(ma_price_list, ma_price_example)
        self.assertEqual(ma_volume_list, ma_volume_example)
        self.assertRaises(Exception, ma_obj.update, "123", 456)

# -----------------------------------------------------------------
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

    def generate_ohlc_example_result(self, filename, period):
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
        ohlc_obj = OpenHighLowClose(initial_time, period * 6000)
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
        period = 5 * 60  # minute
        # actual
        ohlc_example = self.generate_ohlc_example_result(filename, period)
        # expect
        ohlc_group = self.generate_ohlc_result(filename, period, "8450000")
        print(ohlc_group.limit(4))
        print(ohlc_example[:3])
        ohlc_list = [[row["time"][:4], row["open"], row["high"], row["low"], row["close"]] for row in ohlc_group.rows]
        # assert
        self.assertEqual(ohlc_list, ohlc_example)

    @staticmethod
    def close_price_example_64(filename):
        i020 = [i.strip("\n").split(",") for i in open(filename)]
        index_time = i020[0].index("INFO_TIME")
        index_price = i020[0].index("PRICE")
        i020 = i020[1:]

        close_price = []  # collecting close price every minute
        last_time = ""

        for match_info in i020:
            info_time = match_info[index_time].zfill(8)
            time = info_time[0:2] + info_time[2:4]
            match_price = int(match_info[index_price])

            if len(close_price) == 0:
                close_price += [match_price]
                last_time = time
            else:
                if time == last_time:
                    close_price[-1] = match_price
                elif time != last_time:
                    close_price += [match_price]
                    last_time = time

        return close_price

    def test_close_price(self):
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        close_price_example = self.close_price_example_64(filename)
        ohlc_group = self.generate_ohlc_result(filename, 1, "8450000")
        close_price_list = [row["close"] for row in ohlc_group.rows]
        self.assertEqual(close_price_list, close_price_example)

    @staticmethod
    def ohlc_by_ticks(filename):
        i020 = [i.strip("\n").split(",") for i in open(filename)]
        index_time = i020[0].index("INFO_TIME")
        index_price = i020[0].index("PRICE")
        i020 = i020[1:]
        TickMA200 = []
        TickOHLC = []
        for MatchInfo in i020:
            MatchTime = MatchInfo[index_time].zfill(8)
            MatchPrice = int(MatchInfo[index_price])

            # 將Tick相加
            TickMA200 += [MatchPrice]

            # 當tick200筆時，進行開高低收統計
            if len(TickMA200) == 200:
                TickOHLC += [[MatchTime, TickMA200[0], max(TickMA200), min(TickMA200), TickMA200[-1]]]
                TickMA200 = []
        return TickOHLC

    def test_OpenHighLowClose_by_ticks(self):
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        # actual
        ohlc_ticks_example = self.ohlc_by_ticks(filename)
        # expect
        data = self.data_util.get_data_from_file(filename, 1)
        ohlc_obj = OpenHighLowClose.ticks(200)
        ohlc_list = []
        for row in data.rows:
            ohlc_obj.update(row["INFO_TIME"], int(row["PRICE"]))
            ohlc_list += [list(ohlc_obj.get())]

        self.assertEqual(ohlc_list, ohlc_ticks_example)

# -----------------------------------------------------------------
    @staticmethod
    def volume_count_per_min(filename):
        I020 = [line.strip('\n').split(",") for line in open(filename)]
        index_time = I020[0].index("INFO_TIME")
        index_amount = I020[0].index("AMOUNT")
        I020 = I020[1:]
        Qty = []
        lastAmount = 0
        for i in I020:
            MatchInfo = i[index_time].zfill(8)
            # defined time ticks by minute
            HMTime = MatchInfo[0:2] + MatchInfo[2:4]
            MatchAmount = int(i[index_amount])
            # calculate every minute
            if len(Qty) == 0:
                Qty.append([HMTime, 0])
                lastAmount = MatchAmount
            else:
                if HMTime == Qty[-1][0]:
                    Qty[-1][1] = MatchAmount - lastAmount
                else:
                    Qty.append([HMTime, 0])
                    lastAmount = MatchAmount
        return Qty

    def generate_volume_count_example_result(self, filename, period):
        result = self.volume_count_per_min(filename)
        filter_list = []
        for i, ele in enumerate(result):
            if i % period == 0:
                _time = ele[0]
                end = i + period if i + period < len(result) else len(result)
                _volume = sum(j[1] for j in result[i:end])
                filter_list += [[_time, _volume]]
        return filter_list

    def generate_volume_count_result(self, filename, period):
        result = self.data_util.get_data_from_file(filename, 1)
        volume_count_obj = VolumeCount("8450000", period * 6000)
        selected = Table(["time", "volume"])
        for row in result.rows:
            volume_count_obj.update(row["INFO_TIME"], int(row["AMOUNT"]))
            selected.insert(volume_count_obj.get())
        group = selected.group_by(
            group_by_columns=["time"],
            aggregates={"vol": lambda rows: rows[-1]["volume"]}
        ).order_by(lambda row: row["time"])
        filter_list = [[row["time"][:4], row["vol"]] for row in group.rows]
        return filter_list

    def test_VolumeCount(self):
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        period = 1
        # actual
        volume_count_example = self.generate_volume_count_example_result(filename, period)
        # expect
        volume_count = self.generate_volume_count_result(filename, period)
        # assert
        self.assertEqual(volume_count_example, volume_count)
        self.assertRaises(Exception, VolumeCount("8450100", 6000).update, "8450000", 777)

# -----------------------------------------------------------------
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

# -----------------------------------------------------------------
    @staticmethod
    def average_volume(filename):
        data = [line.strip("\n").split(",") for line in open(filename)]
        index_time = data[0].index("INFO_TIME")
        index_amount = data[0].index("AMOUNT")
        index_buy_count = data[0].index("MATCH_BUY_CNT")
        index_sell_count = data[0].index("MATCH_SELL_CNT")
        data = data[1:]
        result = []
        for MatchInfo in data:
            MatchTime = MatchInfo[index_time]
            MatchAmount = int(MatchInfo[index_amount])
            MatchBCnt = int(MatchInfo[index_buy_count])
            MatchSCnt = int(MatchInfo[index_sell_count])
            avgB = float(MatchAmount) / MatchBCnt
            avgS = float(MatchAmount) / MatchSCnt
            result += [(MatchTime, avgB, avgS)]
        return result

    def test_AverageVolume(self):
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        # actual
        average_volume_example = self.average_volume(filename)
        # expect
        data = self.data_util.get_data_from_file(filename, 1)
        avg_vol_obj = AverageVolume()
        average_volume_list = []
        for row in data.rows:
            avg_vol_obj.update(row["INFO_TIME"], row["AMOUNT"], int(row["MATCH_BUY_CNT"]), int(row["MATCH_SELL_CNT"]))
            average_volume_list.append(avg_vol_obj.get())
        # assert
        self.assertEqual(average_volume_list, average_volume_example)
        self.assertRaises(Exception, avg_vol_obj.update, "8450000", 123)

# -----------------------------------------------------------------
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

    def test_SimpleSellBuyVolume(self):
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        # actual
        sell_buy_list_example = self.sell_buy_volume(filename)
        # expect
        data = self.data_util.get_data_from_file(filename, 1)
        sell_buy_obj = SimpleSellBuyVolume()
        sell_buy_list = []
        for row in data.rows:
            sell_buy_obj.update(row["INFO_TIME"], int(row["PRICE"]), int(row["QTY"]))
            sell_buy_list += [sell_buy_obj.get()]
        # assert
        self.assertEqual(sell_buy_list, sell_buy_list_example)
        self.assertRaises(Exception, sell_buy_obj.update, data.rows[0]["INFO_TIME"], 1, 1)

# -----------------------------------------------------------------
    def test_SellBuy(self): # TODO: see also 56-1, 56-2, 57
        match = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        updn = os.path.join(self.test_resource_path, "UpDn5", "Futures_20170815_I080.csv")

        match_table = self.data_util.get_data_from_file(match, 1)
        updn_table = self.data_util.get_data_from_file(updn, 1)

        print(match_table.select(["INFO_TIME", "PRICE", "QTY"]).limit(1))
        print(updn_table.limit(1))

# -----------------------------------------------------------------
    def diff_commission_volume(self, filename):
        data = self.data_util.get_data_from_file(filename, 1)
        results = [
            (
                row["INFO_TIME"],
                int(row["BUY_QTY"]) - int(row["SELL_QTY"])
            ) for row in data.rows]
        return results

    def avg_commission_volume(self, filename):
        data = self.data_util.get_data_from_file(filename, 1)
        results = [
            (
                row["INFO_TIME"],
                float(row["SELL_QTY"]) / float(row["SELL_ORDER"]),
                float(row["BUY_QTY"]) / float(row["BUY_ORDER"])
            ) for row in data.rows]
        return results

    def current_commission_volume(self, filename):
        data = self.data_util.get_data_from_file(filename, 1)
        results = []
        for i, row in enumerate(data.rows):
            if i == 0:
                pre_row = data.rows[0]
            else:
                pre_row = data.rows[i-1]
            s_val = float(row["SELL_QTY"]) - int(pre_row["SELL_QTY"])
            b_val = float(row["BUY_QTY"]) - int(pre_row["BUY_QTY"])
            results += [(row["INFO_TIME"], s_val, b_val)]
        return results

    def test_CommissionInfo(self):
        filename = os.path.join(self.test_resource_path, "COMMISSION", "Futures_20170815_I030.csv")
        data = self.data_util.get_data_from_file(filename, 1)
        # diff volume of commission
        diff_example = self.diff_commission_volume(filename)
        # avg volume
        avg_example = self.avg_commission_volume(filename)
        # current S/B volume
        current_example = self.current_commission_volume(filename)

        diff_list = []
        avg_list = []
        current_list = []
        commission_info = CommissionInfo()
        for row in data.rows:
            commission_info.update(
                time=row["INFO_TIME"],
                sell_volume=int(row["SELL_QTY"]),
                sell_count=int(row["SELL_ORDER"]),
                buy_volume=int(row["BUY_QTY"]),
                buy_count=int(row["BUY_ORDER"])
            )
            diff_list += [commission_info.get("Diff")]
            avg_list += [commission_info.get("AVG")]
            current_list += [commission_info.get("current")]

        self.assertEqual(diff_list, diff_example)
        self.assertEqual(avg_list, avg_example)
        self.assertEqual(current_list, current_example)

# -----------------------------------------------------------------
    def weighted_avg_prices(self, filename):
        data = self.data_util.get_data_from_file(filename, 1)
        results = []
        for row in data.rows:
            totalUpPrice = 0
            totalUpQty = 0
            totalDnPrice = 0
            totalDnQty = 0
            for j in range(1, 6):
                totalDnPrice += int(row["BUY_PRICE%d" % j]) * int(row["BUY_QTY%d" % j])
                totalDnQty += int(row["BUY_QTY%d" % j])
                totalUpPrice += int(row["SELL_PRICE%d" % j]) * int(row["SELL_QTY%d" % j])
                totalUpQty += int(row["SELL_QTY%d" % j])
            results += [(row["INFO_TIME"], float(totalUpPrice) / totalUpQty, float(totalDnPrice) / totalDnQty)]
        return results

    def test_WeightedAveragePrice(self):
        updn = os.path.join(self.test_resource_path, "UpDn5", "Futures_20170815_I080.csv")
        updn_table = self.data_util.get_data_from_file(updn, 1)

        # actual
        avg_price_example = self.weighted_avg_prices(updn)

        # expect
        avg_price_list = []
        weighted_avg_price = WeightedAveragePrice()
        cols = updn_table.columns
        sell_cols = [col for col in cols if "SELL" in col]
        buy_cols = [col for col in cols if "BUY" in col]
        for row in updn_table.rows:
            sell_prices = map(lambda col: float(row[col]), filter(lambda col: "PRICE" in col, sell_cols))
            sell_volumes = map(lambda col: float(row[col]), filter(lambda col: "QTY" in col, sell_cols))
            buy_prices = map(lambda col: float(row[col]), filter(lambda col: "PRICE" in col, buy_cols))
            buy_volumes = map(lambda col: float(row[col]), filter(lambda col: "QTY" in col, buy_cols))

            weighted_avg_price.update(
                time=row["INFO_TIME"],
                sell_pairs=list(zip(sell_prices, sell_volumes)),
                buy_pairs=list(zip(buy_prices, buy_volumes))
            )
            avg_price_list += [weighted_avg_price.get()]

        self.assertEqual(avg_price_list, avg_price_example)

# -----------------------------------------------------------------
    def test_(self):
        filename = os.path.join(self.test_resource_path, "MATCH", "Futures_20170815_I020.csv")
        data = self.data_util.get_data_from_file(filename, 1)
        print(data.limit(3))


if __name__ == '__main__':
    unittest.main()