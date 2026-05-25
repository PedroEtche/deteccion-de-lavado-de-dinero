from abc import ABC, abstractmethod
from datetime import date
from typing import Any, List

class JoinStrategy(ABC):
    """Abstract strategy for grouping batches of messages."""

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def join_batch(self, batch: List[Any]) -> List[Any]:
        raise NotImplementedError()


class NoStrategy(JoinStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def join_batch(self, batch: List[Any]) -> List[Any]:
        return batch

class BankMaxAmountStrategy(JoinStrategy):
    def __str__(self) -> str:
        return f"BankMaxAmountStrategy"

    def join_batch(self, batch: List[Any]) -> List[Any]:
        pass

class PaymentFormatAverageStrategy(JoinStrategy):
    def __str__(self) -> str:
        return "PaymentFormatAverageStrategy"
    
    def join_batch(self, batch: List[Any]) -> List[Any]:
        pass
            