import sys

from config.config_manager import ConfigurationManager
from util.data_extractor import DataExtractor

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("config parameter is missing!!!!")
        sys.exit(0)
    else:
        ConfigurationManager.load_configuration(sys.argv[1])

    data_extractor = DataExtractor()
    data_extractor.extract_source_data()
