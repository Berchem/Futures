from .Util import read_csv
from .SQLiteUtil import SQLiteUtil
from MypseudoSQL import Table


class DataUtil:
    def __init__(self):
        self.__table = None
        self.__sqlite_util = None

    def get_data_from_file(self, filename, with_header):
        if with_header:  # True, file is with headers
            raw_data = read_csv(filename, with_header=(not with_header))
            self.__table = Table(raw_data[0])
            for row in raw_data[1:]:
                self.__table.insert(row)
        else:  # False, file is without headers
            raw_data = read_csv(filename, with_header=with_header)
            columns = ["col_{}".format(i) for i in range(len(raw_data[0]))]
            self.__table = Table(columns)
            for row in raw_data:
                self.__table.insert(row)
        return self.__table

    def get_data_from_sqlite(self, database, table_name, condition=None):
        self.__sqlite_util = SQLiteUtil(database)
        columns = self.__sqlite_util.get_columns(table_name)
        main_query = "select * from %s" % table_name
        query = main_query + (""if condition is None else " {}".format(condition))
        raw_data = self.__sqlite_util.scan(query)
        self.__table = Table(columns)
        for row in raw_data:
            self.__table.insert(row)
        return self.__table

    def get_data_from_url(self, url=None):
        pass

    def get_api_from_yuanta(self):
        pass

    def get_data_from_fastos(self):
        pass
