from abstract_validator import AbstractValidator


class ColumnCountValidator(AbstractValidator):
    def set_next(self, validator: any) -> AbstractValidator:
        pass

    def validate(self):
        print("column count file validator")
