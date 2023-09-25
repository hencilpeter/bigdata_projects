import hashlib

from data.validation_log import ValidationLog
from data.validation_parameters import ValidationParameters
from data.validation_result import ValidationResult
from data.validation_status import ValidationStatus
from .abstract_validator import AbstractValidator


class HashValueValidator(AbstractValidator):
    def validate(self, validation_parameters: ValidationParameters, validation_result: ValidationResult):
        hash_value = hashlib.sha512(str(validation_parameters.get_filedata()).encode("utf-8")).hexdigest()
        if hash_value == validation_parameters.get_hash_value():
            validation_result.set_major_status(ValidationStatus.SUCCESS)
            validation_result.add_log(ValidationLog(validator_name="hash value validation",
                                                    validator_status=ValidationStatus.SUCCESS,
                                                    validator_log="hash value matched."))
        else:
            validation_result.set_major_status(ValidationStatus.FAILED)
            validation_result.add_log(ValidationLog(validator_name="hash value validation",
                                                    validator_status=ValidationStatus.FAILED,
                                                    validator_log="hash value did not match."))

        return super().validate(validation_parameters, validation_result)
