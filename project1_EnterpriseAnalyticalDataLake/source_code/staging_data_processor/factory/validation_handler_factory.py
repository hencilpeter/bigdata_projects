from validator.abstract_validator import AbstractValidator
from validator.hash_value_validator import HashValueValidator
from validator.column_count_validator import ColumnCountValidator
from validator.record_count_validator import RecordCountValidator
from validator.control_file_existence_validator import ControlFileExistenceValidator


class ValidationHandlerFactory:
    @staticmethod
    def get_validation_handler_chain():
        control_file_existence_validator = ControlFileExistenceValidator()
        hash_value_validator = HashValueValidator()
        column_count_validator = ColumnCountValidator()
        record_count_validator = RecordCountValidator()

        control_file_existence_validator.set_next(hash_value_validator).set_next(column_count_validator).\
            set_next(record_count_validator)

        return control_file_existence_validator



