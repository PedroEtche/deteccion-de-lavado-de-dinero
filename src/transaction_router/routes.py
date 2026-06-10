from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional

from src.common.middleware import MessageMiddlewareExchangeRabbitMQ


@dataclass
class Route:
    """One destination pipeline. The router emits a sub-batch to `output`
    with the txs that pass all `filters`."""
    name: str
    output: str
    num_downstream_workers: int
    routing_strategy: str
    filters: List[Dict[str, Any]]
    exchange: Optional[MessageMiddlewareExchangeRabbitMQ] = field(default=None, init=False, repr=False)
    next_worker: int = field(default=1, init=False, repr=False)

    def matches(self, tx) -> bool:
        """tx is a TransactionRow instance (see src/common/communication/internal.py)."""
        for f in self.filters:
            if not _filter_matches(tx, f):
                return False
        return True

    def next_routing_key(self) -> str:
        """Round-robin: rota worker_1 .. worker_N en cada llamada."""
        key = f"worker_{self.next_worker}"
        self.next_worker = (self.next_worker % self.num_downstream_workers) + 1
        return key


def _filter_matches(tx, f: Dict[str, Any]) -> bool:
    """Evaluate one filter against one TransactionRow."""
    ftype = f.get("type")

    if ftype == "currency":
        return tx.payment_currency == f["value"]

    if ftype == "date_range":
        # `tx.date` is a @property on TransactionRow that parses the timestamp.
        tx_date = tx.date.date() if tx.date is not None else None
        if tx_date is None:
            return False
        return _parse_date(f["from"]) <= tx_date <= _parse_date(f["to"])

    if ftype == "payment_format_in":
        return tx.payment_format in f["values"]

    raise ValueError(f"Unknown filter type: {ftype!r}")


def _parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def build_routes(raw_routes: List[Dict[str, Any]]) -> List[Route]:
    routes = []
    for r in raw_routes:
        routes.append(
            Route(
                name=r["name"],
                output=r["output"],
                num_downstream_workers=int(r.get("num_downstream_workers", 1)),
                routing_strategy=r.get("routing_strategy", "round_robin").lower(),
                filters=list(r.get("filters", [])),
            )
        )
    return routes
