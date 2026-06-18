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

    def __init__(self):
        pass

    def __str__(self) -> str:
        return "NoStrategy"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        pass

    def get_eof_routes(self) -> List[str]:
        pass


class BankMaxAmountStrategy(GroupStrategy):
    def __init__(self):
        pass

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

        routed_data = []
        
        for bank, info in max_per_bank.items():
            route = f"bank_{bank}"
            routed_data.append((route, [info]))

        return routed_data


class PaymentFormatAverageStrategy(GroupStrategy):
    """Shards by payment_format so each aggregator owns all transactions for its
    formats. This guarantees each aggregator computes the correct full average."""

    def __init__(self):
        pass

    def __str__(self) -> str:
        return "PaymentFormatAverageStrategy"

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
            route = str(fmt)
            if route not in routed_batches:
                routed_batches[route] = []
            routed_batches[route].append(
                {
                    "payment_format": fmt,
                    "total_amount": stats["total_amount"],
                    "tx_quantity": stats["tx_quantity"],
                }
            )

        return [(route, b) for route, b in routed_batches.items()]

class AccountPairCountStategy(GroupStrategy):
    def __init__(self):
        pass

    def __str__(self) -> str:
        return "AccountPairCountStategy"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        counts = {}

        for tx in batch:
            key = (tx["from_bank"], tx["from_account"], tx["to_bank"], tx["to_account"])
            counts[key] = counts.get(key, 0) + 1

        routed_batches = {}
        for (from_bank, from_account, to_bank, to_account), size in counts.items():
            string_key = f"{from_bank}_{from_account}_{to_bank}_{to_account}"

            if string_key not in routed_batches:
                routed_batches[string_key] = []

            routed_batches[string_key].append(
                {
                    "from_bank": from_bank,
                    "from_account": from_account,
                    "to_bank": to_bank,
                    "to_account": to_account,
                    "count": size,
                }
            )

        return [(route, b) for route, b in routed_batches.items()]
    

class AccountStrategy(GroupStrategy):
    def __init__(self):
        pass

    def __str__(self) -> str:
        return "AccountStrategy"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        accounts = set()
        for tx in batch:
            accounts.add((tx["from_bank"], tx["from_account"]))
            accounts.add((tx["to_bank"], tx["to_account"]))

        routed_batches = {}
        for bank, account in accounts:
            string_key = f"{bank}_{account}"

            if string_key not in routed_batches:
                routed_batches[string_key] = []

            routed_batches[string_key].append({"bank": bank, "account": account})
        return [(route, b) for route, b in routed_batches.items()]

class MergeRoutingStrategy(GroupStrategy):
    """Routes transactions by BOTH origin and destination for Distributed Joins."""

    def __init__(self):
        pass

    def __str__(self) -> str:
        return "MergeRoutingStrategy"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        routed_batches = {}

        for tx in batch:
            string_key_from = f"{tx.from_bank}_{tx.from_account}"

            if string_key_from not in routed_batches:
                routed_batches[string_key_from] = []
            routed_batches[string_key_from].append(tx)

            string_key_to = f"{tx.to_bank}_{tx.to_account}"

            if string_key_from != string_key_to:
                if string_key_to not in routed_batches:
                    routed_batches[string_key_to] = []
                routed_batches[string_key_to].append(tx)

        return [(route, b) for route, b in routed_batches.items()]


class ScatterGroupStrategy(GroupStrategy):
    def __init__(self):
        pass

    def __str__(self) -> str:
        return "ScatterGroupStrategy"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        routed_batches = {}

        for tx in batch:
            string_key = f"{tx.from_bank}_{tx.from_account}"

            if string_key not in routed_batches:
                routed_batches[string_key] = []

            routed_batches[string_key].append(
                {
                    "from_bank": tx.from_bank,
                    "from_account": tx.from_account,
                    "to_bank": tx.to_bank,
                    "to_account": tx.to_account,
                }
            )

        return [(route, b) for route, b in routed_batches.items()]
