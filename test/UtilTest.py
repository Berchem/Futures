import unittest
from Futures.Util import *


class UtilTest(unittest.TestCase):
    def test_time_to_num(self):
        self.assertEqual(time_to_num("8450830"), 3150830)
        self.assertEqual(time_to_num("13300000"), 4860000)

    def test_num_to_time(self):
        self.assertEqual(num_to_time(3150830), "08450830")


if __name__ == '__main__':
    unittest.main()
