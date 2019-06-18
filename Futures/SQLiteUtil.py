import sqlite3


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
        pass

    def write_sqlite(self, path_str, table_name):
        pass


class SQLiteUtil(SQLite):
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
