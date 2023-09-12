from config.config_manager import ConfigurationManager
from reader.sql_data_reader import SQLDataReader


class DataExtractor:
    def __init__(self):
        self.table_list = ConfigurationManager.get_table_list()
        self.sql_data_reader = SQLDataReader()
        self.target_path = ConfigurationManager.get_common_config("target_path")

    def extract_source_data(self):
        for table_name in self.table_list:
            self.sql_data_reader.extract_and_write_data(table_name=table_name, target_path=self.target_path)
