from MypseudoSQL import Table
from Futures.DataUtil import DataUtil
import unittest


class DataUtilTest(unittest.TestCase):
    data_engine = DataUtil()

    def test_get_data_from_file(self):
        filename = "../TradeBookDownload/BooksSample/TickData/Futures_20170815_I020.csv"
        data = self.data_engine.get_data_from_file(filename, False)
        self.assertTrue(isinstance(data, Table))
        self.assertTrue(isinstance(data.columns, list))
        self.assertTrue(isinstance(data.rows, list))
        self.assertTrue(isinstance(data.rows[0], dict))

    def test_get_data_from_sqlite(self):
        database = "../test_resources/db.sqlite3"
        table_name = "test_table"
        data = self.data_engine.get_data_from_sqlite(database, table_name)
        self.assertTrue(isinstance(data, Table))
        self.assertTrue(isinstance(data.columns, list))
        self.assertTrue(isinstance(data.rows, list))
        self.assertTrue(isinstance(data.rows[0], dict))
