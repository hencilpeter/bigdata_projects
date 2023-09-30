from util.landing_files_processor import LandingFilesProcessor

if __name__ == '__main__':
    landing_files_processor = LandingFilesProcessor(landing_path="/home/itv007175/data/landing/", hdfs_path='hdfs://m01.itversity.com:9000/user/itv007175/datalake/stage/' ,as_of_date='20230927')
    landing_files_processor.process()
