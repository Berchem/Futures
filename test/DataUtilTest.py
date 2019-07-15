from MypseudoSQL import Table
from Futures.DataUtil import DataUtil
from Futures.Config import Config
import unittest
import os


class DataUtilTest(unittest.TestCase):
    data_engine = DataUtil()
    conf = Config("../test_resources/conf/conf.properties")

    def test_get_data_from_file(self):
        filename = os.path.join((self.conf.prop.get("DEFAULT", "SRC_FOLDER")), "MATCH", "Futures_20170815_I020.csv")
        data = self.data_engine.get_data_from_file(filename, False)
        self.assertTrue(isinstance(data, Table))
        self.assertTrue(isinstance(data.columns, list))
        self.assertTrue(isinstance(data.rows, list))
        self.assertTrue(isinstance(data.rows[0], dict))

    def test_get_data_from_sqlite(self):
        database = self.conf.prop.get("SQLITE", "DATABASE")
        table_name = "test_table"
        data = self.data_engine.get_data_from_sqlite(database, table_name)
        self.assertTrue(isinstance(data, Table))
        self.assertTrue(isinstance(data.columns, list))
        self.assertTrue(isinstance(data.rows, list))
        self.assertTrue(isinstance(data.rows[0], dict))
