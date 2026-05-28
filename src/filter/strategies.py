from typing import Any, List, Dict
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

class FilterStrategy(ABC):
    """Abstract strategy for filtering batches of messages.

    Implementations should provide `filter_batch(batch)` which receives
    a list of messages and returns a filtered list.
    """

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def filter_batch(self, batch) -> Dict[str, List[Any]]:
        raise NotImplementedError()


class NoStrategy(FilterStrategy):
    """A strategy that returns the input batch unchanged."""
    def __init__(self, output_queue: str) -> None:
        self.output_queue = output_queue

    def __str__(self) -> str:
        return "NoStrategy"

    def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
        return {self.output_queue: batch}


class CurrencyStrategy(FilterStrategy):
    def __init__(self, output_queue: str, target_currency: str) -> None:
        self.output_queue = output_queue
        self.target_currency = target_currency

    def __str__(self) -> str:
        return f"CurrencyStrategy(target_currency={self.target_currency}, output_queue={self.output_queue})"


    def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
        filtered = [ row for row in batch if row.payment_currency == self.target_currency ]

        if not filtered:
            return {}

        return {self.output_queue: filtered}


class FieldLessThanStrategy(FilterStrategy):
    def __init__(self, output_queue: str, field_name: str, threshold: float) -> None:
        self.output_queue = output_queue
        self.field_name = field_name
        self.threshold = threshold


    def __str__(self) -> str:
        return f"AmountLessThanStrategy(threshold={self.threshold}, output_queue={self.output_queue})"

    def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
        filtered = []

        for row in batch:
            value = getattr(row, self.field_name, None)
            
            if value is not None and value < self.threshold:
                filtered.append(row)

        if not filtered:
            return {}

        return { self.output_queue: filtered }


@dataclass(frozen=True)
class DateRangeRoute:
    from_date: datetime
    to_date: datetime
    queue: str

    def matches(self, value: datetime) -> bool:
        return self.from_date <= value <= self.to_date


class DateStrategy(FilterStrategy):
    def __init__(self, routes: List[DateRangeRoute]) -> None:
        self.routes = routes

    def __str__(self) -> str:
        return f"DateStrategy(routes={self.routes})"

    def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
        routed = defaultdict(list)
        for row in batch:
            row_date = row.date
            for route in self.routes:
                if route.matches(row_date):
                    routed[route.queue].append(row)

        return dict(routed)

