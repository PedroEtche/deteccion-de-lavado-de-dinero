from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict, List, Optional
import logging

class AggregatorStrategy(ABC):
    """Abstract strategy for aggregating batches of messages."""

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def aggregate_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        raise NotImplementedError()

    @abstractmethod
    def get_result_for_client(self, client: str) -> List[Any]:
        raise NotImplementedError()


class NoStrategy(AggregatorStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def aggregate_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        return batch

    def get_result_for_client(self, client: str) -> List[Any]:
        return []

class BankMaxAmountStrategy(AggregatorStrategy):
    def __init__(self):
        self.max_per_bank_by_client: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def __str__(self) -> str:
        return f"BankMaxAmountStrategy"

    def aggregate_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        if client is None:
            raise ValueError("client is required for BankMaxAmountStrategy")

        current_max_per_bank = self.max_per_bank_by_client.setdefault(client, {})
        for tx in batch:
            bank = tx["from_bank"]
            amount = tx["amount_paid"] or 0.0
            current = current_max_per_bank.get(bank)

            if current is None or amount > current["amount_paid"]:
                current_max_per_bank[bank] = {
                    "from_bank": tx["from_bank"],
                    "from_account": tx["from_account"],
                    "amount_paid": amount,
                }

        return list(current_max_per_bank.values())

    def get_result_for_client(self, client: str) -> List[Any]:
        return list(self.max_per_bank_by_client.pop(client, {}).values())

class AccountPairCountStategy(AggregatorStrategy):
    def __init__(self):
        self.counts_by_client: Dict[str, Dict[tuple, int]] = {}

    def __str__(self) -> str:
        return f"AccountPairCountStategy()"

    def aggregate_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        if client is None:
            raise ValueError("client is required for AccountPairCountStategy")

        counts = self.counts_by_client.setdefault(client, {})
        for tx in batch:
            key = (tx["from_bank"], tx["from_account"], tx["to_bank"], tx["to_account"])
            counts[key] = counts.get(key, 0) + 1

        return self._build_results(counts)

    def get_result_for_client(self, client: str) -> List[Any]:
        counts = self.counts_by_client.pop(client, {})
        return self._build_results(counts)

    def _build_results(self, counts: Dict[tuple, int]) -> List[Any]:
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
        
class PaymentFormatAverageStrategy(AggregatorStrategy):
    def __init__(self):
        self.stats_by_client: Dict[str, Dict[tuple, Dict[str, Any]]] = {}

    def __str__(self) -> str:
        return f"PaymentFormatAverageStrategy()"

    def aggregate_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        if client is None:
            raise ValueError("client is required for PaymentFormatAverageStrategy")

        stats = self.stats_by_client.setdefault(client, {})
        for tx in batch:
            bank = tx["from_bank"]
            account = tx["from_account"]
            fmt = tx["payment_format"]

            partial_amount = tx["total_amount"]
            partial_count = tx["tx_quantity"]

            key = (bank, account, fmt)

            if key not in stats:
                stats[key] = {"count": 0, "total": 0.0}


            stats[key]["count"] += partial_count
            stats[fmt]["total"] += partial_amount

        return []
    
    def get_result_for_client(self, client: str) -> List[Any]:
        # Pop the state to calculate the final results and clear memory for this client
        stats = self.stats_by_client.pop(client, {})

        results = []
        for (bank, account, fmt), stat in stats.items():
            count = stat["count"]
            average = stat["total"] / count if count > 0 else 0.0

            results.append({
                "from_bank": bank,
                "from_account": account,
                "payment_format": fmt,
                "average_amount": average,
            })

        return results