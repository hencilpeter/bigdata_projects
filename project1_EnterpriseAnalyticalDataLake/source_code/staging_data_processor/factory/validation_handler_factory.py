from validator.column_count_validator import ColumnCountValidator
from validator.file_existence_validator import FileExistenceValidator
from validator.hash_value_validator import HashValueValidator
from validator.record_count_validator import RecordCountValidator


class ValidationHandlerFactory:
    @staticmethod
    def get_validation_handler_chain():
        file_existence_validator = FileExistenceValidator()
        hash_value_validator = HashValueValidator()
        column_count_validator = ColumnCountValidator()
        record_count_validator = RecordCountValidator()

        file_existence_validator.set_next(hash_value_validator).set_next(column_count_validator). \
            set_next(record_count_validator)

        return file_existence_validator
