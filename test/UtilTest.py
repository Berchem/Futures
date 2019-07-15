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
        MAlen = 10

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
        # actual
        filename = os.path.join(self.test_resource_path,'MATCH', 'Futures_20170815_I020.csv')
        ma_list_example = self.ma_example(filename)
        # expect
        ma_obj = MovingAverage(10, 6000, "8450000")
        data = self.data_util.get_data_from_file(filename, True)
        ma_list = []
        for row in data.rows:
            ma_obj.update(row["INFO_TIME"], int(row["PRICE"]))
            ma_list.extend([ma_obj.get()])
        # assertion
        self.assertEqual(ma_list, ma_list_example)


if __name__ == '__main__':
    unittest.main()
