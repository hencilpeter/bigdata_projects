from abstract_validator import AbstractValidator


class RecordCountValidator(AbstractValidator):
    def set_next(self, validator: any) -> AbstractValidator:
        pass

    def validate(self):
        print("record count  validator")
