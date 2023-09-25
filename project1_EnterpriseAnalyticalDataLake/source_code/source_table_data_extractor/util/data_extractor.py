import hashlib
import os

from config.config_manager import ConfigurationManager
from reader.file_reader import FileReader
from reader.sql_data_reader import SQLDataReader


class DataExtractor:
    def __init__(self):
        self.table_list = ConfigurationManager.get_table_list()
        self.sql_data_reader = SQLDataReader()
        self.target_path = ConfigurationManager.get_common_config("target_path")

    def save_data(self, table_name, file_data, file_extension):
        with open(os.path.join(self.target_path, table_name + f".{file_extension}"), 'w') as file:
            file.writelines(file_data)

    def populate_control_file(self, table_name, file_data):
        hash = hashlib.sha512(str(file_data).encode("utf-8")).hexdigest()
        file_header = str(file_data).split('\n')[0]
        column_count = str(file_header).count(",") + 1
        record_count = len(str(file_data).split('\n')) - 1
        data = f"filename={table_name}.csv,hash_algorithm=sha512,hash_value={hash},column_count={column_count}," \
               f"record_count={record_count}"
        self.save_data(table_name=table_name, file_data=data, file_extension="ctl")

    def extract_source_data(self):
        for table_name in self.table_list:
            file_data = self.sql_data_reader.get_table_data(table_name=table_name)
            self.save_data(table_name=table_name, file_data=file_data, file_extension="csv")
            # read the file data again to avid the hash code mismatch issue
            file_data = FileReader.read_file(os.path.join(self.target_path, table_name + ".csv"))
            self.populate_control_file(table_name=table_name, file_data=file_data)
