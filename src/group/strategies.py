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
    """A sharded strategy that distributes data across multiple aggregators."""
    def __init__(self, base_route: str, shard_amount: int):
        self.base_route = base_route
        self.shard_amount = shard_amount

    def __str__(self) -> str:
        return "PaymentFormatAverageStrategy"

    def _get_shard_route(self, string_key: str) -> str:
        hash_val = zlib.crc32(string_key.encode('utf-8'))
        shard_id = hash_val % self.shard_amount
        return f"{self.base_route}_{shard_id}"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        grouped_stats = {}

        for tx in batch:
            bank = tx.from_bank
            account = tx.from_account
            fmt = tx.payment_format
            amount = tx.amount_paid

            key = (bank, account, fmt)

            if key not in grouped_stats:
                grouped_stats[key] = {"total_amount": 0.0, "tx_quantity": 0}

            grouped_stats[key]["total_amount"] += amount
            grouped_stats[key]["tx_quantity"] += 1

        routed_batches = {}
        
        for (bank, account, fmt), stats in grouped_stats.items():
            string_key = f"{bank}_{account}_{fmt}"
            route = self._get_shard_route(string_key)
            
            if route not in routed_batches:
                routed_batches[route] = []
                
            routed_batches[route].append({
                "from_bank": bank,
                "from_account": account,
                "payment_format": fmt,
                "total_amount": stats["total_amount"],
                "tx_quantity": stats["tx_quantity"],
            })

        return [(route, b) for route, b in routed_batches.items()]
    
    def get_eof_routes(self) -> List[str]:
        return [f"{self.base_route}_{i}" for i in range(self.shard_amount)]


class AccountPairCountStategy(GroupStrategy):
    def __init__(self, base_route: str, shard_amount: int):
        self.base_route = base_route
        self.shard_amount = shard_amount

    def __str__(self) -> str:
        return "AccountPairCountStategy"

    def _get_shard_route(self, string_key: str) -> str:
        hash_val = zlib.crc32(string_key.encode('utf-8'))
        shard_id = hash_val % self.shard_amount
        return f"{self.base_route}_{shard_id}"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        counts = {}
        for tx in batch:
            key = (tx.from_bank, tx.from_account, tx.to_bank, tx.to_account)
            counts[key] = counts.get(key, 0) + 1

        routed_batches = {}
        for (from_bank, from_account, to_bank, to_account), size in counts.items():
            string_key = f"{from_bank}_{from_account}_{to_bank}_{to_account}"
            route = self._get_shard_route(string_key)

            if route not in routed_batches:
                routed_batches[route] = []

            routed_batches[route].append({
                "from_bank": from_bank,
                "from_account": from_account,
                "to_bank": to_bank,
                "to_account": to_account,
                "count": size
            })

        return [(route, b) for route, b in routed_batches.items()]
    
    def get_eof_routes(self) -> List[str]:
        return [f"{self.base_route}_{i}" for i in range(self.shard_amount)]
    

class AccountStrategy(GroupStrategy):
    def __init__(self, output_route: str):
        self.output_route = output_route

    def __str__(self) -> str:
        return "AccountStrategy"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        accounts = set()
        for tx in batch:
            accounts.add((tx.from_bank, tx.from_account))
            accounts.add((tx.to_bank, tx.to_account))

        batch_result = [{"bank": bank, "account": account} for bank, account in accounts]

        return [(self.output_route, batch_result)]
    
    def get_eof_routes(self) -> List[str]:
        return [self.output_route]


class MergeRoutingStrategy(GroupStrategy):
    """Routes transactions by BOTH origin and destination for Distributed Joins."""
    def __init__(self, base_route: str, shard_amount: int):
        self.base_route = base_route
        self.shard_amount = shard_amount

    def __str__(self) -> str:
        return "MergeRoutingStrategy"

    def _get_shard_route(self, bank: str, account: str) -> str:
        string_key = f"{bank}_{account}"
        numeric_hash = zlib.crc32(string_key.encode('utf-8'))
        shard_id = numeric_hash % self.shard_amount
        return f"{self.base_route}_{shard_id}"

    def group_and_route(self, batch: List[Any]) -> List[Tuple[str, List[Any]]]:
        routed_batches = {}

        for tx in batch:
            route_from = self._get_shard_route(tx.from_bank, tx.from_account)
            if route_from not in routed_batches:
                routed_batches[route_from] = []
            routed_batches[route_from].append(tx)

            route_to = self._get_shard_route(tx.to_bank, tx.to_account)
            
            if route_from != route_to:
                if route_to not in routed_batches:
                    routed_batches[route_to] = []
                routed_batches[route_to].append(tx)

        return [(route, b) for route, b in routed_batches.items()]

    def get_eof_routes(self) -> List[str]:
        return [f"{self.base_route}_{i}" for i in range(self.shard_amount)]