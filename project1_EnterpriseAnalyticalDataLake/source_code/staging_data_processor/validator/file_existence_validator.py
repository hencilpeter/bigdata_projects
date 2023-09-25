import os

from data.validation_log import ValidationLog
from data.validation_parameters import ValidationParameters
from data.validation_result import ValidationResult
from data.validation_status import ValidationStatus
from .abstract_validator import AbstractValidator


class FileExistenceValidator(AbstractValidator):

    def validate(self, validation_parameters: ValidationParameters, validation_result: ValidationResult):
        filename = validation_parameters.get_filename()
        if not os.path.isfile(filename):
            validation_result.set_major_status(ValidationStatus.FAILED)
            validation_result.add_log(ValidationLog(validator_name="file existence validation",
                                                    validator_status=ValidationStatus.FAILED,
                                                    validator_log="file does not exist"))
        else:
            validation_result.set_major_status(ValidationStatus.SUCCESS)
            validation_result.add_log(
                ValidationLog(validator_name="file existence validation", validator_status=ValidationStatus.SUCCESS,
                              validator_log="file exist"))

        return super().validate(validation_parameters, validation_result)
