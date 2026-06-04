from typing import Any, Callable, List, Dict
from abc import ABC, abstractmethod
import operator as op


_AMOUNT_OPS: Dict[str, Callable[[float, float], bool]] = {
    "<": op.lt,
    "=": op.eq,
    ">": op.gt,
}


class FilterStrategy(ABC):
    """Abstract strategy for filtering batches of messages.

    Implementations should provide `filter_batch(batch)` which receives
    a list of messages and returns a filtered list.
    """

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def filter_batch(self, batch) -> List[Any]:
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
        filtered = [
            row for row in batch if row.payment_currency == self.target_currency
        ]

        if not filtered:
            return []

        return filtered


class AmountComparisonStrategy(FilterStrategy):
    def __init__(self, operator: str, threshold: float) -> None:
        if operator not in _AMOUNT_OPS:
            raise ValueError(
                f"Unknown operator {operator!r}. Valid: {list(_AMOUNT_OPS)}"
            )
        self._op = _AMOUNT_OPS[operator]
        self.threshold = float(threshold)

    def __str__(self) -> str:
        return f"AmountComparisonStrategy(amount_paid {self._op} {self.threshold})"

    def filter_batch(self, batch: List[Any]) -> List[Any]:
        return [
            row for row in batch if self._op(float(row.amount_received), self.threshold)
        ]


# class FieldLessThanStrategy(FilterStrategy):
#     def __init__(self, output_queue: str, field_name: str, threshold: float) -> None:
#         self.output_queue = output_queue
#         self.field_name = field_name
#         self.threshold = threshold
#
#     def __str__(self) -> str:
#         return f"FieldLessThanStrategy(field={self.field_name}, threshold={self.threshold}, output_queue={self.output_queue})"
#
#     def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
#         filtered = [
#             row
#             for row in batch
#             if getattr(row, self.field_name, None) is not None
#             and getattr(row, self.field_name) < self.threshold
#         ]
#         if not filtered:
#             return {}
#         return {self.output_queue: filtered}
#
#
# class AmountLessThanStrategy(FieldLessThanStrategy):
#     """Specialization of FieldLessThanStrategy for `amount_paid`."""
#
#     def __init__(self, output_queue: str, threshold: float) -> None:
#         super().__init__(output_queue, "amount_paid", float(threshold))
#
#     def __str__(self) -> str:
#         return f"AmountLessThanStrategy(threshold={self.threshold}, output_queue={self.output_queue})"
#
#
#
#
# @dataclass(frozen=True)
# class ShardConfig:
#     by: str
#     shards: int
#
#
# @dataclass(frozen=True)
# class DateRangeRoute:
#     from_date: datetime
#     to_date: datetime
#     queue: str
#     shard: Optional[ShardConfig] = None
#
#     def matches(self, value: datetime) -> bool:
#         return self.from_date <= value <= self.to_date
#
#     def resolve_queue(self, row: Any) -> str:
#         if self.shard is None:
#             return self.queue
#
#         shard_value = getattr(row, self.shard.by)
#         hash = hashlib.md5(str(shard_value).encode()).hexdigest()
#         shard_id = int(hash, 16) % self.shard.shards
#
#         return f"{self.queue}_shard_{shard_id}"
#
#
# class PaymentFormatStrategy(FilterStrategy):
#     def __init__(self, output_queue: str, formats: List[str]) -> None:
#         self.output_queue = output_queue
#         self.formats: Set[str] = set(formats)
#
#     def __str__(self) -> str:
#         return f"PaymentFormatStrategy(formats={sorted(self.formats)}, output_queue={self.output_queue})"
#
#     def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
#         filtered = [row for row in batch if row.payment_format in self.formats]
#         if not filtered:
#             return {}
#         return {self.output_queue: filtered}
#
#
# class DateStrategy(FilterStrategy):
#     def __init__(self, routes: List[DateRangeRoute]) -> None:
#         self.routes = routes
#
#     def __str__(self) -> str:
#         return f"DateStrategy(routes={self.routes})"
#
#     def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
#         routed = defaultdict(list)
#         for row in batch:
#             if row.timestamp is None:
#                 continue
#             try:
#                 row_dt = datetime.strptime(row.timestamp, _TIMESTAMP_FMT)
#             except ValueError:
#                 continue
#             for route in self.routes:
#                 if not route.matches(row_dt):
#                     continue
#                 queue_name = route.resolve_queue(row)
#                 routed[queue_name].append(row)
#
#         return dict(routed)
#
#
# class HistoricalAverageFilterStrategy(FilterStrategy):
#     """Filter by historical average per payment_format.
#
#     - Expects control messages (from Join) with a batch of dicts like
#       {"payment_format": fmt, "average_amount": avg}
#     - Stores averages per client. Before calling `filter_batch`, the
#       service sets `strategy._current_client` to the incoming client id so
#       the strategy can pick the right averages.
#     """
#
#     def __init__(self, output_queue: str, threshold_multiplier: float = 0.01) -> None:
#         self.output_queue = output_queue
#         self.threshold_multiplier = float(threshold_multiplier)
#         # client -> {payment_format: average}
#         self.averages_by_client: Dict[str, Dict[str, float]] = {}
#
#     def __str__(self) -> str:
#         return f"HistoricalAverageFilterStrategy(output={self.output_queue}, thresh={self.threshold_multiplier})"
#
#     def update_averages(self, client: str, averages: List[Dict[str, Any]]) -> None:
#         mapping: Dict[str, float] = {}
#         for item in averages:
#             fmt = item.get("payment_format")
#             avg = item.get("average_amount")
#             if fmt is None or avg is None:
#                 continue
#             mapping[str(fmt)] = float(avg)
#         self.averages_by_client[client] = mapping
#
#     def clear_client(self, client: str) -> None:
#         self.averages_by_client.pop(client, None)
#
#     def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
#         client = getattr(self, "_current_client", None)
#         if client is None:
#             return {}
#
#         avg_map = self.averages_by_client.get(client, {})
#         if not avg_map:
#             return {}
#
#         filtered = []
#         for row in batch:
#             fmt = getattr(row, "payment_format", None)
#             amount = getattr(row, "amount_paid", None)
#             if fmt is None or amount is None:
#                 continue
#             avg = avg_map.get(str(fmt))
#             if avg is None:
#                 continue
#             if amount < (avg * self.threshold_multiplier):
#                 filtered.append(row)
#
#         if not filtered:
#             return {}
#
#         return {self.output_queue: filtered}
#
#
# class OriginNotEqualDestinationStrategy(FilterStrategy):
#     def __init__(self, output_queue: str) -> None:
#         self.output_queue = output_queue
#
#     def __str__(self) -> str:
#         return f"OriginNotEqualDestinationStrategy(output_queue={self.output_queue})"
#
#     def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
#         filtered = [
#             tx
#             for tx in batch
#             if tx.from_bank != tx.to_bank or tx.from_account != tx.to_account
#         ]
#         return {self.output_queue: filtered}
#
#
# class ScatterFilterStrategy(FilterStrategy):
#     """Filters scatter accounts and unrolls their transactions back into a flat list."""
#
#     def __init__(self, output_queue: str, threshold: int = 5) -> None:
#         self.output_queue = output_queue
#         self.threshold = threshold
#
#     def __str__(self) -> str:
#         return f"ScatterFilterStrategy(threshold={self.threshold}, output_queue={self.output_queue})"
#
#     def filter_batch(self, batch: List[Any]) -> Dict[str, List[Any]]:
#         filtered = []
#
#         for record in batch:
#             if record.get("unique_destinations", 0) > self.threshold:
#                 transactions = record.get("transactions", [])
#                 filtered.extend(transactions)
#
#         if not filtered:
#             return {}
#
#         return {self.output_queue: filtered}
