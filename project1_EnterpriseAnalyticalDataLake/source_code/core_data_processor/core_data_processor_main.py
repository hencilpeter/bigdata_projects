import sys

from util.util_core_data_processor import UtilCoreDataProcessor

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("as_of_date parameter is missing. e.g. usage spark-submit core_data_processor_main.py 20230927' ")

    data_processor = UtilCoreDataProcessor()
    data_processor.process_data(as_of_data=sys.argv[1])
