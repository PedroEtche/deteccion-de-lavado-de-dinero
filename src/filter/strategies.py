from datetime import date
from typing import Any, List
from abc import ABC, abstractmethod

class FilterStrategy(ABC):
    """Abstract strategy for filtering batches of messages.

    Implementations should provide `filter_batch(batch)` which receives
    a list of messages and returns a filtered list.
    """

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def filter_batch(self, batch: List[Any]) -> List[Any]:
        raise NotImplementedError()


class NoStrategy(FilterStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def filter_batch(self, batch: List[Any]) -> List[Any]:
        return batch


class CurrencyStrategy(FilterStrategy):
    def __init__(self, target_currency: str) -> None:
        self.target_currency = target_currency

    def __str__(self) -> str:
        return f"CurrencyStrategy(target_currency={self.target_currency})"

    def filter_batch(self, batch: List[Any]) -> List[Any]:
        raise NotImplementedError("implement later")


class DateStrategy(FilterStrategy):
    def __init__(self, from_date: date, to_date: date) -> None:
        self.from_date = from_date
        self.to_date = to_date

    def __str__(self) -> str:
        return f"DateStrategy(from_date={self.from_date}, to_date={self.to_date})"

    def filter_batch(self, batch: List[Any]) -> List[Any]:
        raise NotImplementedError("implement later")



