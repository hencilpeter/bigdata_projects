class ValidationLog:
    validator_name = None
    validator_status = None
    validator_log = None

    def __init__(self, validator_name, validator_status, validator_log):
        self.validator_name = validator_name
        self.validator_status = validator_status
        self.validator_log = validator_log

    def get_validator_name(self):
        return self.validator_name

    def get_validator_status(self):
        return self.validator_status

    def get_validator_log(self):
        return self.validator_log

    def get_combined_log(self):
        return f" validator name :  {self.get_validator_name()} , validation status : {self.get_validator_status()}, log : {self.get_validator_log()}"
