from abc import ABC, abstractmethod
from datetime import date
from typing import Any, List, Optional, Dict

class JoinStrategy(ABC):
    """Abstract strategy for grouping batches of messages."""
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        raise NotImplementedError()

    @abstractmethod
    def get_joined_for_client(self, client: str) -> List[Any]:
        raise NotImplementedError()


class NoStrategy(JoinStrategy):
    """A strategy that returns the input batch unchanged."""
    def __init__(self):
        pass

    def __str__(self) -> str:
        return "NoStrategy"

    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        return batch

    def get_joined_for_client(self, client: str) -> List[Any]:
        return []

class CountStrategy(JoinStrategy):
    def __init__(self):
        self.count_by_client = {}

    def __str__(self) -> str:
        return f"CountStrategy"

    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        if client is None:
            raise ValueError("client is required for CountStrategy")
        self.count_by_client[client] = self.count_by_client.get(client, 0) + len(batch)
        return [self.count_by_client[client]]
    
    def get_joined_for_client(self, client: str) -> int:
        return [self.count_by_client.get(client, 0)]
    
class BankMaxAmountStrategy(JoinStrategy):
    def __init__(self):
        self.max_per_bank_by_client: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def __str__(self) -> str:
        return "BankMaxAmountStrategy"

    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        if client is None:
            raise ValueError("client is required for BankMaxAmountStrategy")
        max_per_bank = self.max_per_bank_by_client.setdefault(client, {})
        for tx in batch:
            bank = tx["from_bank"]
            amount = tx["amount_paid"] or 0.0
            current = max_per_bank.get(bank)

            if current is None or amount > current["amount_paid"]:
                max_per_bank[bank] = {
                    "from_bank": tx["from_bank"],
                    "from_account": tx["from_account"],
                    "amount_paid": amount,
                }

        return list(max_per_bank.values())

    def get_joined_for_client(self, client: str) -> List[Any]:
        return list(self.max_per_bank_by_client.get(client, {}).values())
            