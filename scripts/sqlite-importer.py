from Futures.DataUtil import DataUtil
from Futures.SQLiteUtil import SQLiteUtil
import os, sys


src_path = "../test_resources/futures_data_sample"
data_engine = DataUtil()
sqlite_util = SQLiteUtil("../test_resources/db.sqlite3")


def import_test_data_to_sqlite(table_name):
    sub_path = os.path.join(src_path, table_name)
    filename = os.listdir(sub_path)[0]
    table = data_engine.get_data_from_file(os.path.join(sub_path, filename), True)
    columns = table.columns
    tb_name = ("test_" + table_name).lower()
    sqlite_util.drop_table(tb_name)
    sqlite_util.create_table(tb_name, columns)
    sqlite_util.write_sqlite(sub_path, tb_name)


if __name__ == "__main__":
    tables = ["MATCH", "COMMISSION", "UpDn5"]
    for table in tables:
        import_test_data_to_sqlite(table)
