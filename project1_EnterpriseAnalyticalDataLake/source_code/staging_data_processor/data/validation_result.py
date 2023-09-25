from validation_status import ValidationStatus
from validation_log import ValidationLog


class ValidationResult:
    validation_satus = ValidationStatus.SUCCESS
    list_validation_log = []

    def set_status(self, validation_satus):
        if validation_satus > self.validation_satus:
            self.validation_satus = validation_satus

    def get_status(self):
        return self.validation_satus

    def add_log(self, validator_log: ValidationLog):
        self.list_validation_log.append(validator_log)

    def get_log(self):
        return self.list_validation_log
