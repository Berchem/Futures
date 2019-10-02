import os
import sqlite3
from .Util import read_csv


class SQLite:
    def __init__(self, database):
        self.conn = None
        self.__database = database
        self.__get_connection()

    def __get_connection(self):
        # self.conn = sqlite3.connect(self.__database)
        self.conn = sqlite3.connect(self.__database, check_same_thread=False)

    def create_table(self, sqlite_table_name, sqlite_columns):
        create_template = "create table if not exists {table} ({column})"
        columns = "\"index\" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,"
        columns += ",".join("{} text".format(col) for col in sqlite_columns)
        create_query = create_template.format(table=sqlite_table_name, column=columns)
        self.conn.execute(create_query)
        self.conn.commit()

    def write_csv_to_table(self, path_str, table_name):
        insert_template = "insert into \"{table}\" ({columns}) values ({values})"
        if os.path.isdir(path_str):
            data = []
            for i, filename in enumerate(os.listdir(path_str)):
                data.extend(read_csv(os.path.join(path_str, filename), with_header=False if i == 0 else True))
        else:
            filename = path_str
            data = read_csv(filename, with_header=False)
        headers = data.pop(0)
        columns = ",".join('"{}"'.format(col) for col in headers)
        values = ",".join("?" for _ in headers)
        insert_query = insert_template.format(table=table_name, columns=columns, values=values)
        self.conn.executemany(insert_query, data)
        self.conn.commit()

    def drop_table(self, table_name):
        self.conn.execute("drop table if exists %s" % table_name)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def tables(self):
        cursor = self.conn.execute("select name from sqlite_master where type='table'")
        return [result[0] for result in cursor.fetchall()]

    def get_columns(self, table_name):
        cursor = self.conn.execute("select sql from sqlite_master where name='{}'".format(table_name))
        result = cursor.fetchone()
        if result is None:
            return result
        else:
            result = result[0]
            result = result.split(table_name)[1]
            result = result.\
                replace("(", "").\
                replace(")", "").\
                replace("\"", "").\
                replace("\'", "").\
                replace("`", "").\
                replace("\r", "").\
                replace("\n", "").\
                replace("\t", " ").\
                replace(")", "").\
                strip()
            result_list = result.split(",")
            columns = [col.strip().split(" ")[0] for col in result_list]
            return columns


class SQLiteUtil(SQLite):
    def scan(self, query):
        cursor = self.conn.execute(query)
        return cursor.fetchall()

    def update(self, table_name, column_name, value, condition=None):
        update_statement = "update \"{}\" set \"{}\" = \"{}\" {}"
        update_query = update_statement.format(table_name, column_name, value, "" if condition is None else condition)
        self.conn.execute(update_query)
        self.conn.commit()

    def insert(self, table_name, columns, values):
        insert_statement = "insert into {} ({}) values ({})"
        col = ",".join(map(lambda c: "'{}'".format(c), columns))
        val = ",".join(map(lambda v: "'{}'".format(v), values))
        insert_query = insert_statement.format(table_name, col, val)
        self.conn.execute(insert_query)
        self.conn.commit()

    def bulk_insert(self, table_name, columns, value_list):
        insert_statement = "insert into {} ({}) values ({})"
        col = ",".join(map(lambda c: "`{}`".format(c), columns))
        val = ",".join("?" for _ in columns)
        insert_query = insert_statement.format(table_name, col, val)
        self.conn.executemany(insert_query, value_list)
        self.conn.commit()
