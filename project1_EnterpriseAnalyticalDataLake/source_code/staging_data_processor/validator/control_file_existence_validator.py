from .abstract_validator import AbstractValidator


class ControlFileExistenceValidator(AbstractValidator):
    def set_next(self, validator: any) -> AbstractValidator:
        pass

    def validate(self):
        print("control file validator")

