from .validation_log import ValidationLog
from .validation_status import ValidationStatus


class ValidationResult:
    validation_major_satus = ValidationStatus.SUCCESS
    list_validation_log = []

    def set_major_status(self, validation_major_satus):
        if validation_major_satus > self.validation_major_satus:
            self.validation_major_satus = validation_major_satus

    def get_major_status(self):
        return self.validation_major_satus

    def add_log(self, validator_log: ValidationLog):
        self.list_validation_log.append(validator_log)

    def get_log(self):
        return self.list_validation_log
