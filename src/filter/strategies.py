from typing import Any, List, Dict, Optional
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
import hashlib

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
        return {self.output_queue[0]: batch}


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
class ShardConfig:
    by: str
    shards: int


@dataclass(frozen=True)
class DateRangeRoute:
    from_date: datetime
    to_date: datetime
    queue: str
    shard: Optional[ShardConfig] = None

    def matches(self, value: datetime) -> bool:
        return self.from_date <= value <= self.to_date

    def resolve_queue(self, row: Any) -> str:
        if self.shard is None:
            return self.queue

        shard_value = getattr(row, self.shard.by)
        hash = hashlib.md5( str(shard_value).encode()).hexdigest()
        shard_id = int(hash, 16) % self.shard.shards

        return f"{self.queue}_shard_{shard_id}"


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
                if not route.matches(row_date):
                    continue
                queue_name = route.resolve_queue(row)
                routed[queue_name].append(row)

        return dict(routed)
