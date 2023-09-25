# design pattern source : https://refactoring.guru/design-patterns/chain-of-responsibility/python/example

from abc import ABC, abstractmethod

from data.validation_result import ValidationResult
from data.validation_status import ValidationStatus


class AbstractValidator(ABC):
    next_validator = None

    def set_next(self, next_validator: any) -> any:
        self.next_validator = next_validator
        return self.next_validator

    @abstractmethod
    def validate(self, request, validation_result: ValidationResult):
        if validation_result.get_major_status() is ValidationStatus.SUCCESS and self.next_validator is not None:
            self.next_validator.validate(request, validation_result)
