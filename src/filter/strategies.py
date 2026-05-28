from typing import Any, List, Dict, Set
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

_TIMESTAMP_FMT = "%Y/%m/%d %H:%M"

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


class AmountLessThanStrategy(FilterStrategy):
    def __init__(self, output_queue: str, threshold: float) -> None:
        self.output_queue = output_queue
        self.threshold = threshold

    def __str__(self) -> str:
        return f"AmountLessThanStrategy(threshold={self.threshold}, output_queue={self.output_queue})"

    def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
        filtered = [
            row for row in batch
            if row.amount_paid is not None and row.amount_paid < self.threshold
        ]

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


class PaymentFormatStrategy(FilterStrategy):
    def __init__(self, output_queue: str, formats: List[str]) -> None:
        self.output_queue = output_queue
        self.formats: Set[str] = set(formats)

    def __str__(self) -> str:
        return f"PaymentFormatStrategy(formats={sorted(self.formats)}, output_queue={self.output_queue})"

    def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
        filtered = [row for row in batch if row.payment_format in self.formats]
        if not filtered:
            return {}
        return {self.output_queue: filtered}


class DateStrategy(FilterStrategy):
    def __init__(self, routes: List[DateRangeRoute]) -> None:
        self.routes = routes

    def __str__(self) -> str:
        return f"DateStrategy(routes={self.routes})"

    def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
        routed = defaultdict(list)
        for row in batch:
            if row.timestamp is None:
                continue
            try:
                row_dt = datetime.strptime(row.timestamp, _TIMESTAMP_FMT)
            except ValueError:
                continue
            for route in self.routes:
                if route.matches(row_dt):
                    routed[route.queue].append(row)

        return dict(routed)

