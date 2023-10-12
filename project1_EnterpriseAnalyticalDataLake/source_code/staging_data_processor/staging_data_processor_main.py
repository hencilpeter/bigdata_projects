from util.landing_files_processor import LandingFilesProcessor
import sys 

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("as_of_date parameter is missing. e.g. usage '/usr/bin/python3.6 staging_data_processor_main.py 20230927' ")
        sys.exit(0)
    
    landing_files_processor = LandingFilesProcessor(landing_path="/home/itv007175/data/landing/", hdfs_path='hdfs://m01.itversity.com:9000/user/itv007175/datalake/stage/' ,as_of_date=sys.argv[1])
    landing_files_processor.process()
