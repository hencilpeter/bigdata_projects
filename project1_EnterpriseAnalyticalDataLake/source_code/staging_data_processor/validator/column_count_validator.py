from .abstract_validator import AbstractValidator

from data.validation_result import ValidationResult
from data.validation_parameters import ValidationParameters
from data.validation_status import ValidationStatus
from data.validation_log import ValidationLog


class ColumnCountValidator(AbstractValidator):

    def validate(self, validation_parameters: ValidationParameters, validation_result: ValidationResult):
        file_header = validation_parameters.get_filedata().split("\n")[0]
        file_column_count = len(file_header.split(","))
        if validation_parameters.column_count == file_column_count:
            validation_result.set_major_status(ValidationStatus.SUCCESS)
            validation_result.add_log(
                ValidationLog(validator_name="column count validation", validator_status=ValidationStatus.SUCCESS,
                              validator_log="column count matched"))
        else:
            validation_result.set_major_status(ValidationStatus.FAILED)
            validation_result.add_log(
                ValidationLog(validator_name="column count validation", validator_status=ValidationStatus.FAILED,
                              validator_log="column count does not matched"))

        return super().validate(validation_parameters, validation_result)
