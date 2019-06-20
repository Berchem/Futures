from Futures.SQLiteUtil import SQLiteUtil
from Futures.Util import read_csv
import unittest


class SQLiteUtilTest(unittest.TestCase):
    sqlite_util = SQLiteUtil('../test_resources/db.sqlite3')

    def test_drop_table(self):
        table_name = "test_table"
        self.sqlite_util.create_table(table_name, ["id", "name", "interests"])
        self.sqlite_util.drop_table(table_name)

    def test_create_table_single(self):
        table_name = "sample_table"
        columns = ['INFO_TIME', 'MATCH_TIME', 'PROD', 'ITEM', 'PRICE', 'QTY', 'AMOUNT', 'MATCH_BUY_CNT', 'MATCH_SELL_CNT']
        filename = "../TradeBookDownload/BooksSample/TickData/Futures_20170815_I020.csv"

        self.sqlite_util.drop_table(table_name)

        self.sqlite_util.create_table(table_name, columns)
        self.sqlite_util.write_sqlite(filename, table_name)

        cursor = self.sqlite_util.conn.execute("select * from %s" % table_name)
        data = cursor.fetchall()
        print "\ncreate table test"
        print data[0]
        print data[1]
        print data[-1]
        print len(data)

    def test_get(self):
        self.sqlite_util.drop_table("bc_test")
        self.sqlite_util.conn.execute('create table if not exists bc_test'
                                      '(id int primary key,'
                                      'name text,'
                                      'friends int)')
        self.sqlite_util.conn.executemany('insert into bc_test values (?, ?, ?)', [(1, "Hero", 0),
                                                                                   (3, "John", 3),
                                                                                   (2, "May", 5)])
        self.sqlite_util.conn.commit()
        data = self.sqlite_util._get("bc_test")
        for row in data:
            print row

    def test_reader(self):
        filename = "../TradeBookDownload/BooksSample/TickData/Futures_20170815_I020.csv"
        data = read_csv(filename, True)
        print "\ncsv reader test"
        print data[0]
        print data[1]
        print data[-1]
        print len(data)


if __name__ == '__main__':
    unittest.main()
