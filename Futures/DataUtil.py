from Util import read_csv
from SQLiteUtil import SQLiteUtil
from MypseudoSQL import Table


class DataUtil:
    __table = None
    __sqlit_util = None

    def __init__(self):
        pass

    def get_data_from_file(self, filename, with_header):
        if with_header:  # True, file is with headers
            raw_data = read_csv(filename, with_header=(not with_header))
            self.__table = Table(raw_data[0])
            for row in raw_data[1:]:
                self.__table.insert(row)
        else:  # False, file is without headers
            raw_data = read_csv(filename, with_header=with_header)
            columns = ["col_{}".format(i) for i in xrange(len(raw_data[0]))]
            self.__table = Table(columns)
            for row in raw_data:
                self.__table.insert(row)
        return self.__table

    def get_data_from_sqlite(self, database, table_name):
        self.__sqlit_util = SQLiteUtil(database)
        columns = self.__sqlit_util.get_columns(table_name)
        raw_data = self.__sqlit_util.scan("select * from %s" % table_name)
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
