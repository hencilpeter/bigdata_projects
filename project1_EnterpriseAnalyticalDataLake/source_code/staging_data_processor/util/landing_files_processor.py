import glob
import logging
from datetime import datetime

from data.validation_result import ValidationResult
from data.validation_status import ValidationStatus
from factory.validation_handler_factory import ValidationHandlerFactory
from util.control_file_reader import ControlFileReader
import subprocess

class LandingFilesProcessor:
    def __init__(self, landing_path):
        self.landing_path = landing_path
        timestamp_now = datetime.now()
        logging.basicConfig(filename=__name__ + str(timestamp_now.strftime("%Y%m%d %H%M%S")) + ".log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        self.logger = logging.getLogger()

    def process(self):
        list_control_file = glob.glob(self.landing_path + "*.ctl")
        validation_handler = ValidationHandlerFactory.get_validation_handler_chain()
        for control_file in list_control_file:
            validation_parameters = ControlFileReader.get_parameters_from_controlfile(control_file)
            validation_result = ValidationResult()
            validation_handler.validate(validation_parameters, validation_result)

            if validation_result.get_major_status() is ValidationStatus.SUCCESS:
                pass  # copy the file from edge node to hdfs
                #subprocess.call(['hadoop fs -copyFromLocal /tmp/mike/test* hdfs:///user/edwaeadt/app'], shell=True)

            # log the status
            self.add_file_process_log(file_name=validation_parameters.get_filename(),
                                      validation_result=validation_result)

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
