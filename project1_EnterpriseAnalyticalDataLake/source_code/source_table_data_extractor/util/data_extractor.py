from config.config_manager import ConfigurationManager
from reader.sql_data_reader import SQLDataReader

import os
import hashlib


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
        data = f"filename={table_name}.csv,hash_algorithm=sha512,hash_value={hash}," \
               f"record_count={len(file_data)}"
        self.save_data(table_name=table_name, file_data=data, file_extension="ctl")

    def extract_source_data(self):
        for table_name in self.table_list:
            file_data = self.sql_data_reader.get_table_data(table_name=table_name)
            self.save_data(table_name=table_name, file_data=file_data, file_extension="csv")
            self.populate_control_file(table_name=table_name, file_data=file_data)
