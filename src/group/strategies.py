import zlib
from abc import ABC, abstractmethod
from typing import Any, List, Tuple

class GroupStrategy(ABC):
    """Abstract strategy for grouping batches of messages."""

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        raise NotImplementedError()
    
    def get_eof_routes(self) -> List[str]:
        return []


class NoStrategy(GroupStrategy):
    """A strategy that returns the input batch unchanged."""
    def __init__(self, output_route: str):
        self.output_route = output_route

    def __str__(self) -> str:
        return "NoStrategy"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        return [(self.output_route, batch)]
    
    def get_eof_routes(self) -> List[str]:
        return [self.output_route]

class BankMaxAmountStrategy(GroupStrategy):
    def __init__(self, output_route: str):
        self.output_route = output_route

    def __str__(self) -> str:
        return "BankMaxAmountStrategy"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        max_per_bank = {}
        for tx in batch:
            bank = tx.from_bank
            amount = tx.amount_paid or 0.0
            current = max_per_bank.get(bank)

            if current is None or amount > current["amount_paid"]:
                max_per_bank[bank] = {
                    "from_bank": tx.from_bank,
                    "from_account": tx.from_account,
                    "amount_paid": amount,
                }

        return [(self.output_route, list(max_per_bank.values()))]
    
    def get_eof_routes(self) -> List[str]:
        return [self.output_route]
            
class PaymentFormatAverageStrategy(GroupStrategy):
    """Shards by payment_format so each aggregator owns all transactions for its
    formats. This guarantees each aggregator computes the correct full average."""
    def __init__(self, base_route: str, total_aggregators: int):
        self.base_route = base_route
        self.total_aggregators = total_aggregators

    def __str__(self) -> str:
        return "PaymentFormatAverageStrategy"

    def _get_shard_route(self, payment_format: str) -> str:
        h = zlib.crc32(str(payment_format).encode('utf-8'))
        shard_id = h % self.total_aggregators
        return f"{self.base_route}_{shard_id}"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        # Aggregate by payment_format within the batch, then route by format.
        # Sharding by format ensures one aggregator owns all partial sums for
        # a given format, so it can compute the correct global average.
        grouped = {}

        for tx in batch:
            fmt = tx.payment_format
            amount = tx.amount_paid or 0.0

            if fmt not in grouped:
                grouped[fmt] = {"total_amount": 0.0, "tx_quantity": 0}

            grouped[fmt]["total_amount"] += amount
            grouped[fmt]["tx_quantity"] += 1

        routed_batches = {}

        for fmt, stats in grouped.items():
            route = self._get_shard_route(str(fmt))
            if route not in routed_batches:
                routed_batches[route] = []
            routed_batches[route].append({
                "payment_format": fmt,
                "total_amount": stats["total_amount"],
                "tx_quantity": stats["tx_quantity"],
            })

        return [(route, b) for route, b in routed_batches.items()]
    
    def get_eof_routes(self) -> List[str]:
        return [f"{self.base_route}_{i}" for i in range(self.total_aggregators)]

class AccountPairCountStategy(GroupStrategy):
    def __init__(self, output_route: str):
        self.output_route = output_route

    def __str__(self) -> str:
        return "AccountPairCountStategy()"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        counts = {}
        for tx in batch:
            key = (tx.from_bank, tx.from_account, tx.to_bank, tx.to_account)
            counts[key] = counts.get(key, 0) + 1

        results = []
        for (from_bank, from_account, to_bank, to_account), size in counts.items():
            results.append(
                {
                    "from_bank": from_bank,
                    "from_account": from_account,
                    "to_bank": to_bank,
                    "to_account": to_account,
                    "size": size,
                }
            )   

        return results
    
    def get_eof_routes(self) -> List[str]:
        return [self.output_route]
    
class AccountStrategy(GroupStrategy):
    def __init__(self, output_route: str):
        self.output_route = output_route

    # aca hay que revisar caso en query 4
    # como vamos a routear las cuentas? es necesario mantener un estado?
    def __str__(self) -> str:
        return f"AccountStrategy()"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        accounts = set()
        for tx in batch:
            accounts.add((tx.from_bank, tx.from_account))
            accounts.add((tx.to_bank, tx.to_account))

        return [{"bank": bank, "account": account} for bank, account in accounts]
    
    def get_eof_routes(self) -> List[str]:
        return [self.output_route]