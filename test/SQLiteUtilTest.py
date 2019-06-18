from Futures.SQLiteUtil import SQLiteUtil
import unittest


class SQLiteUtilTest(unittest.TestCase):
    sqlite_util = SQLiteUtil('../test_resources/db.sqlite3')

    def test_get(self):
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

    def test_drop_table(self):
        self.sqlite_util.conn.execute("drop table if exists bc_test")
        self.sqlite_util.conn.commit()


if __name__ == '__main__':
    unittest.main()
