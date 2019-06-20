from Futures.SQLiteUtil import SQLiteUtil
from Futures.Util import read_csv
import os
import unittest


class SQLiteUtilTest(unittest.TestCase):
    sqlite_util = SQLiteUtil('../test_resources/db.sqlite3')
    table_name = "test_table"
    columns = ['INFO_TIME', 'MATCH_TIME', 'PROD', 'ITEM', 'PRICE', 'QTY', 'AMOUNT', 'MATCH_BUY_CNT', 'MATCH_SELL_CNT']

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
        self.sqlite_util.create_table(self.table_name, ["id", "name", "interests"])
        # fetch table list
        cursor = self.sqlite_util.conn.execute("select name from sqlite_master where type='table'")
        table_list = [result[0] for result in cursor.fetchall()]
        # assert
        self.assertTrue(self.table_name in table_list)

    def test_write_sqlite_csv(self):
        filename = "../TradeBookDownload/BooksSample/TickData/Futures_20170815_I020.csv"
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
        path_name = "../test_resources/futures_data_sample"
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

    def test_get(self):
        cursor = self.sqlite_util.conn.execute("select * from %s" % self.table_name)
        # result = cursor.fetchall()
        # data = [dict(zip(self.columns, row)) for row in result]
        # for i in xrange(5):
        #     print data[i]

        result = self.sqlite_util._get(self.table_name)
        data = [dict(zip(self.columns, row)) for row in result]
        for row in data:
            print row


if __name__ == '__main__':
    unittest.main()
