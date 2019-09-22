from Futures.Config import Config
from SKCOM.QueryUtil import QueryUtil
import unittest
import time


conf = Config("../../test_resources/conf/test-conf.ini")


class SKCOMTest(unittest.TestCase):
    sk_util = QueryUtil(conf)

    def test_connect(self):
        conn = self.sk_util.connect()
        self.assertEqual(conn, 0)

    def test_disconnect(self):
        disconnect = self.sk_util.disconnect()
        self.assertEqual(disconnect, 0)

    def test_(self):
        self.sk_util.connect()
        self.sk_util.pump_wait()
        data1 = self.sk_util.get_k_line("TSEA")
        print(len(data1), data1[-1])

        data2 = self.sk_util.get_k_line("TX00")
        print(len(data2), data2[-1])

        data3 = self.sk_util.get_k_line("MTX00")
        print(len(data3), data3[-1])
