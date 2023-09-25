import os

from data.validation_parameters import ValidationParameters
from util.file_reader import FileReader


class ControlFileReader:
    @staticmethod
    def get_parameters_from_controlfile(control_file_name):
        # file = open(control_file_name, "r")
        # data = file.read()
        data = FileReader.read_file(control_file_name)
        split_data = data.split(",")
        validation_parameter = ValidationParameters()
        file_path = os.path.dirname(os.path.abspath(control_file_name))
        for item in split_data:
            list_key_value = item.split("=")
            if list_key_value[0] == "filename":
                validation_parameter.set_filename(os.path.join(file_path, list_key_value[1]))
            elif list_key_value[0] == "hash_algorithm":
                validation_parameter.set_hash_algorithm(list_key_value[1])
            elif list_key_value[0] == "hash_value":
                validation_parameter.set_hash_value(list_key_value[1])
            elif list_key_value[0] == "column_count":
                validation_parameter.set_column_count(list_key_value[1])
            elif list_key_value[0] == "record_count":
                validation_parameter.set_record_count(list_key_value[1])

        # assign file data
        validation_parameter.set_filedata(FileReader.read_file(validation_parameter.get_filename()))

        return validation_parameter
