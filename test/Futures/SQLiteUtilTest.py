from Futures.SQLiteUtil import SQLiteUtil
from Futures.Util import read_csv
from Futures.Config import Config
import os
import unittest


class SQLiteUtilTest(unittest.TestCase):
    conf = Config('../test_resources/conf/conf.properties')
    sqlite_util = SQLiteUtil(conf.prop.get("SQLITE", "DATABASE"))
    table_name = "test_table"
    columns = ['DATE', 'INFO_TIME', 'MATCH_TIME', 'PROD', 'ITEM', 'PRICE', 'QTY', 'AMOUNT', 'MATCH_BUY_CNT', 'MATCH_SELL_CNT']

    # def test_close(self):
    #     self.sqlite_util.close()

    def test_drop_table(self):
        # drop table
        self.sqlite_util.drop_table(self.table_name)
        # fetch table list
        cursor = self.sqlite_util.conn.execute("select name from sqlite_master where type='table'")
        table_list = [result[0] for result in cursor.fetchall()]
        # assert
        self.assertFalse(self.table_name in table_list)

    def test_create_table(self):
        # drop table
        self.sqlite_util.drop_table(self.table_name)
        # create table
        self.sqlite_util.create_table(self.table_name, ["ind", "name", "interests"])
        # fetch table list
        cursor = self.sqlite_util.conn.execute("select name from sqlite_master where type='table'")
        table_list = [result[0] for result in cursor.fetchall()]
        # assert
        self.assertTrue(self.table_name in table_list)

    def test_write_sqlite_csv(self):
        filename = "../test_resources/futures_data_sample/MATCH/Futures_20170815_I020.csv"
        # drop table
        self.sqlite_util.drop_table(self.table_name)
        # create table and write a file
        self.sqlite_util.create_table(self.table_name, self.columns)
        self.sqlite_util.write_sqlite(filename, self.table_name)
        # fetch data
        cursor = self.sqlite_util.conn.execute("select * from %s" % self.table_name)
        result = cursor.fetchall()
        # load data
        data = read_csv(filename)
        # assert
        self.assertTrue(len(data) == len(result))

    def test_write_sqlite_folder(self):
        path_name = "../test_resources/futures_data_sample/MATCH"
        # drop table
        self.sqlite_util.drop_table(self.table_name)
        # create table and write a file
        self.sqlite_util.create_table(self.table_name, self.columns)
        self.sqlite_util.write_sqlite(path_name, self.table_name)
        # fetch data
        cursor = self.sqlite_util.conn.execute("select * from %s" % self.table_name)
        result = cursor.fetchall()
        # load data
        length = 0
        for filename in os.listdir(path_name):
            length += len(read_csv(os.path.join(path_name, filename)))
        # assert
        self.assertTrue(length == len(result))

    def test_get_columns(self):
        # query a table not exists
        columns = self.sqlite_util.get_columns("not_exists")
        self.assertIsNone(columns)
        # query a table
        self.test_write_sqlite_csv()
        columns = self.sqlite_util.get_columns(self.table_name)
        self.assertListEqual(columns, self.columns)

    def test_tables(self):
        self.test_write_sqlite_csv()
        self.assertIn(self.table_name, self.sqlite_util.tables())

    def test_scan(self):
        self.test_write_sqlite_csv()
        data = self.sqlite_util.scan("select * from {}".format(self.table_name))
        self.assertEqual(len(data), 45310)


if __name__ == '__main__':
    unittest.main()
