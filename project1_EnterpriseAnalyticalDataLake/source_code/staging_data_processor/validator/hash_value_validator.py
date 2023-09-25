from abstract_validator import AbstractValidator


class HashValueValidator(AbstractValidator):
    def set_next(self, validator: any) -> AbstractValidator:
        pass

    def validate(self):
        print("hash value file validator")
