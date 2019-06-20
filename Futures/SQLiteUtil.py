import os
import sqlite3
from Util import read_csv


class SQLite:
    __database = None
    conn = None

    def __init__(self, database):
        self.__database = database
        self.__get_connection()

    def __get_connection(self):
        self.conn = sqlite3.connect(self.__database)


class SQLiteImporter(SQLite):
    def create_table(self, sqlite_table_name, sqlite_columns):
        create_template = "create table if not exists {table} ({column})"
        columns = ",".join("{} text".format(col) for col in sqlite_columns)
        create_query = create_template.format(table=sqlite_table_name, column=columns)
        self.conn.execute(create_query)
        self.conn.commit()

    def write_sqlite(self, path_str, table_name):
        insert_template = "insert into {table} values ({column})"
        if os.path.isdir(path_str):
            pass
        else:
            filename = path_str
            data = read_csv(filename, with_header=True)
            columns = ",".join("?" for _ in xrange(len(data[0])))
            insert_query = insert_template.format(table=table_name, column=columns)
            self.conn.executemany(insert_query, data)
            self.conn.commit()

    def drop_table(self, table_name):
        self.conn.execute("drop table if exists %s" % table_name)
        self.conn.commit()

    def close(self):
        self.conn.close()


class SQLiteUtil(SQLiteImporter):
    def _get(self, table, **kwargs):
        query = "select * from %s" % table

        if any(filter(lambda k: k in ("start", "drop_night_trade", "predicate"), kwargs)):
            query += " where"
            subquery = []

            if 'start' in kwargs.keys():
                start = kwargs['start']
                subquery += ["(Date>='%s')" % start]

            if "drop_night_trade" in kwargs.keys():
                if kwargs["drop_night_trade"]:
                    subquery += ["(Time>='08:45:00') & (Time<='13:45:00')"]

            query += " " + " & ".join(subquery)

        if "predicate" in kwargs.keys():
            predicate = kwargs["predicate"]
            query += " " + predicate

        if "lim" in kwargs.keys():
            lim = kwargs['lim']
            query += " limit %d" % lim

        cursor = self.conn.execute(query)
        return cursor.fetchall()

    def get_each(self, table='tick_log', **kwargs):
        return self._get(table, **kwargs)

    def get_per_min(self, table='tick_min_log', **kwargs):
        return self._get(table, **kwargs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
