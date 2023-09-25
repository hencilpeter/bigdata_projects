# design pattern source : https://refactoring.guru/design-patterns/chain-of-responsibility/python/example

from abc import ABC, abstractmethod
from typing import Any, Optional


class AbstractValidator(ABC):

    @abstractmethod
    def set_next(self, validator: any) -> any:
        pass

    @abstractmethod
    def validate(self):
        pass
