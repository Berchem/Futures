from Futures.DataUtil import DataUtil
from Futures.SQLiteUtil import SQLiteUtil
import os


src_path = "../test_resources/futures_data_sample"
data_engine = DataUtil()
sqlite_util = SQLiteUtil("../test_resources/db.sqlite3")


def insert_date(table_name):
    sub_path = os.path.join(src_path, table_name)
    for filename in os.listdir(sub_path):
        date = filename.split("_")[1]
        filename = os.path.join(sub_path, filename)
        table = data_engine.get_data_from_file(filename, 1)
        columns = table.columns
        if "DATE" not in columns:
            table.\
                select(additional_columns={"DATE": lambda row: date}).\
                select(["DATE"] + columns).\
                to_csv(filename)


def delete_date(table_name):
    sub_path = os.path.join(src_path, table_name)
    for filename in os.listdir(sub_path):
        filename = os.path.join(sub_path, filename)
        table = data_engine.get_data_from_file(filename, 1)
        columns = table.columns
        if "DATE" in columns:
            table. \
                select(columns[1:]). \
                to_csv(filename)


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
        # delete_date(table)
        insert_date(table)
        import_test_data_to_sqlite(table)
