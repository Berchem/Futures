import pythoncom

from Futures.Config import Config
from SKCOM.QuoteUtil import QueryUtil
import unittest
import time


conf = Config("../../test_resources/conf/test-conf.ini")


class SKCOMTest(unittest.TestCase):
    sk_util = QueryUtil(conf)
    sk_util.connect()

    def test_connect(self):
        conn = self.sk_util.connect()
        self.assertEqual(conn, 0)

    def test_disconnect(self):
        disconnect = self.sk_util.disconnect()
        self.assertEqual(disconnect, 0)

    def test_get_k_line_data(self):
        self.sk_util.pump_wait(wait_sec=.1, retry_limit=100)
        data = self.sk_util.get_k_line("TSEA")
        print(data[-1])
        self.assertTrue(len(data[0]) == 6)

    def test_get_quote_data(self):
        self.sk_util.pump_wait(wait_sec=.01, retry_limit=1000)
        data = self.sk_util.get_quote("MTX10")
        print(data[-1])
        self.assertTrue(len(data[-1].keys()) == 7)

    def test_get_history_ticks(self):
        self.sk_util.pump_wait(wait_sec=.01, retry_limit=1000)
        data = self.sk_util.get_history_ticks("MTX00")
        print(len(data), data[6000])

    def test_get_live_ticks(self):
        self.sk_util.pump_wait(wait_sec=.01, retry_limit=1000)
        data = self.sk_util.get_live_ticks("TX00")
        print(len(data), data)
