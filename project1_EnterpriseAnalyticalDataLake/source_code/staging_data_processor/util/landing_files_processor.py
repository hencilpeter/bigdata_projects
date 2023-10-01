import glob
import logging
from datetime import datetime

from data.validation_result import ValidationResult
from data.validation_status import ValidationStatus
from factory.validation_handler_factory import ValidationHandlerFactory
from util.control_file_reader import ControlFileReader
import subprocess
import os

class LandingFilesProcessor:
    def __init__(self, landing_path, hdfs_path, as_of_date):
        self.landing_path = landing_path
        self.hdfs_path = hdfs_path
        self.as_of_date = as_of_date
        timestamp_now = datetime.now()
        logging.basicConfig(filename=__name__ + str(timestamp_now.strftime("%Y%m%d %H%M%S")) + ".log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        self.logger = logging.getLogger()

    def process(self):
        list_control_file = glob.glob(os.path.join(self.landing_path, self.as_of_date,"")  + "*.ctl")
        validation_handler = ValidationHandlerFactory.get_validation_handler_chain()
        for control_file in list_control_file:
            validation_parameters = ControlFileReader.get_parameters_from_controlfile(control_file)
            validation_result = ValidationResult()
            validation_handler.validate(validation_parameters, validation_result)

            if validation_result.get_major_status() is ValidationStatus.SUCCESS:
                landing_path_and_as_of_date = os.path.join(self.landing_path, self.as_of_date)
                exact_foldername = validation_parameters.get_filename()[len(landing_path_and_as_of_date)+1:len(control_file)-4]
                exact_filename = validation_parameters.get_filename()[len(landing_path_and_as_of_date)+1:len(control_file)]               
                hdfs_path_with_as_of_date = os.path.join(self.hdfs_path, exact_foldername,"as_of_date="+self.as_of_date,"")
                source_filename = os.path.join(landing_path_and_as_of_date, exact_filename)
                self.create_hdfs_folder(hdfs_path_with_as_of_date)
                self.copy_file_to_hdfs(source_filename, hdfs_path_with_as_of_date)

            # log the status
            self.add_file_process_log(file_name=validation_parameters.get_filename(),
                                      validation_result=validation_result)
    def copy_file_to_hdfs(self, source_filename, hdfs_path):
        subprocess.call(['hadoop fs -copyFromLocal {} {}'.format(source_filename, hdfs_path)], shell=True)
        print("file {} copied to hdfs...".format(source_filename))
    
    def create_hdfs_folder(self,hdfs_path):
        subprocess.call(['hadoop fs -mkdir -p {}'.format(hdfs_path)], shell=True)
        print("hdfs path {} created...".format(hdfs_path))
    
    def add_file_process_log(self, file_name: str, validation_result: ValidationResult):
        log_data = ''
        log_validation = validation_result.get_log()
        log_data += f"filename : {file_name} status: {validation_result.get_major_status()}"
        for item in log_validation:
            log_data += f"{item.get_combined_log()}"

        if validation_result.get_major_status() is ValidationStatus.SUCCESS:
            self.logger.info(log_data)
        else:
            self.logger.error(log_data)
