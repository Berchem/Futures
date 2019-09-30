from Futures.SQLiteUtil import SQLiteUtil
from Futures.Util import read_csv
from Futures.Config import Config
import os
import unittest


class SQLiteUtilTest(unittest.TestCase):
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    conf_path = os.path.join(BASE_DIR, "test_resources", "conf", "conf.properties")
    print(conf_path)
    conf = Config(conf_path)
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

    def test_csv_to_table(self):
        filename = "../test_resources/futures_data_sample/MATCH/Futures_20170815_I020.csv"
        # drop table
        self.sqlite_util.drop_table(self.table_name)
        # create table and write a file
        self.sqlite_util.create_table(self.table_name, self.columns)
        self.sqlite_util.write_csv_to_table(filename, self.table_name)
        # fetch data
        cursor = self.sqlite_util.conn.execute("select * from %s" % self.table_name)
        result = cursor.fetchall()
        # load data
        data = read_csv(filename)
        # assert
        self.assertTrue(len(data) == len(result))

    def test_folder_to_table(self):
        path_name = "../test_resources/futures_data_sample/MATCH"
        # drop table
        self.sqlite_util.drop_table(self.table_name)
        # create table and write a file
        self.sqlite_util.create_table(self.table_name, self.columns)
        self.sqlite_util.write_csv_to_table(path_name, self.table_name)
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
        self.test_csv_to_table()
        columns = self.sqlite_util.get_columns(self.table_name)
        self.assertListEqual(columns, ["index"] + self.columns)

    def test_tables(self):
        self.test_csv_to_table()
        self.assertIn(self.table_name, self.sqlite_util.tables())

    def test_scan(self):
        self.test_csv_to_table()
        data = self.sqlite_util.scan("select * from {}".format(self.table_name))
        self.assertEqual(len(data), 45310)

    def test_insert(self):
        table_name = "test_ohlc"
        columns = [
            "submit_time",
            "status",
            "program",
            "process",
            "item_code",
            "page",
            "k_line_type",
            "output_format",
            "trade_session",
            # "process_table",
            # "request_table",
            # "multiple"
        ]
        values = [
            "2019/09/30 16:26:00",
            "NEW",
            "Quote",
            "getKLine",
            "MTX00",
        ]
        self.sqlite_util.drop_table(table_name)
        self.sqlite_util.create_table(table_name, columns)
        self.sqlite_util.insert(table_name, columns[:-4], values)
        self.sqlite_util.insert(table_name, columns[:-4], values)

        rows = self.sqlite_util.scan("select * from {}".format(table_name))
        self.assertEqual(len(rows), 2)

    def test_update(self):
        table_name = "test_ohlc"
        self.test_insert()
        self.sqlite_util.update(table_name, "status", "RUNNING", "where \"index\" = 1")
        rows = self.sqlite_util.scan("select * from {}".format(table_name))
        columns = self.sqlite_util.get_columns(table_name)
        index_status = columns.index("status")
        self.assertEqual(rows[0][index_status], "RUNNING")
        self.assertEqual(rows[1][index_status], "NEW")

    def test_bulk_insert(self):
        table_name = "test_ohlc"
        columns = [
            "submit_time",
            "status",
            "program",
            "process",
            "item_code",
        ]
        values = [
            "2019/09/30 16:26:00",
            "NEW",
            "Quote",
            "getKLine",
            "MTX00",
        ]
        self.sqlite_util.drop_table(table_name)
        self.sqlite_util.create_table(table_name, columns)
        batch_size = 10000
        self.sqlite_util.bulk_insert(table_name, columns, [values for _ in range(batch_size)])
        rows = self.sqlite_util.scan("select * from {}".format(table_name))
        self.assertEqual(len(rows), batch_size)
        self.assertEqual(list(rows[-1][1:]), values)


if __name__ == '__main__':
    unittest.main()
