from .abstract_validator import AbstractValidator
from data.validation_result import ValidationResult
from data.validation_status import ValidationStatus
from data.validation_parameters import ValidationParameters
from data.validation_log import ValidationLog

class RecordCountValidator(AbstractValidator):
    def validate(self, validation_parameters: ValidationParameters, validation_result: ValidationResult):

        record_count = len(validation_parameters.get_filedata().split("\n")) - 1

        if record_count != validation_parameters.get_record_count():
            validation_result.set_major_status(ValidationStatus.FAILED)
            validation_result.add_log(ValidationLog(validator_name="record count validation",
                                                    validator_status=ValidationStatus.FAILED,
                                                    validator_log=f"record count does not match. expected {validation_parameters.get_record_count()}. actual {record_count}"))
        else:
            validation_result.set_major_status(ValidationStatus.SUCCESS)
            validation_result.add_log(ValidationLog(validator_name="record count validation",
                                                    validator_status=ValidationStatus.SUCCESS,
                                                    validator_log=f"record count matched. expected {validation_parameters.get_record_count()}. actual {record_count}"))
        return super().validate(validation_parameters, validation_result)
