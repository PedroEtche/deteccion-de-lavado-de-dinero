from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

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

    # Default no-op. Strategies que necesiten enriquecer con datos de accounts
    # (ej. BankMaxAmountStrategy para Q2) hacen override.
    def add_accounts(self, batch: List[Any], client: Optional[str] = None) -> None:
        pass


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


class UnionStrategy(JoinStrategy):
    def __init__(self):
        self.batch_by_client: Dict[str, List[Any]] = {}

    def __str__(self) -> str:
        return "UnionStrategy"

    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        if client is None:
            raise ValueError("client is required for UnionStrategy")

        current_batch = self.batch_by_client.setdefault(client, [])
        current_batch.extend(batch)
        return current_batch

    def get_joined_for_client(self, client: str) -> List[Any]:
        return list(self.batch_by_client.pop(client, []))

class CountStrategy(JoinStrategy):
    def __init__(self):
        self.count_by_client = {}

    def __str__(self) -> str:
        return "CountStrategy"

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
        # bank_id -> bank_name, por cliente. Poblado desde el accounts_queue.
        self.bank_names_by_client: Dict[str, Dict[Any, str]] = {}

    def __str__(self) -> str:
        return "BankMaxAmountStrategy"

    def add_accounts(self, batch: List[Any], client: Optional[str] = None) -> None:
        if client is None:
            raise ValueError("client is required for BankMaxAmountStrategy.add_accounts")
        bank_names = self.bank_names_by_client.setdefault(client, {})
        for account in batch:
            bank_id = getattr(account, "bank_id", None)
            bank_name = getattr(account, "bank_name", None)
            if bank_id is not None and bank_name is not None:
                bank_names[bank_id] = bank_name

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

        return self._enriched(client, pop=False)

    def get_joined_for_client(self, client: str) -> List[Any]:
        return self._enriched(client, pop=True)

    def _enriched(self, client: str, pop: bool) -> List[Any]:
        if pop:
            entries = self.max_per_bank_by_client.pop(client, {})
            bank_names = self.bank_names_by_client.pop(client, {})
        else:
            entries = self.max_per_bank_by_client.get(client, {})
            bank_names = self.bank_names_by_client.get(client, {})

        results = []
        for entry in entries.values():
            results.append({
                "bank_name": bank_names.get(entry["from_bank"], entry["from_bank"]),
                "from_account": entry["from_account"],
                "amount_paid": entry["amount_paid"],
            })
        return results

class AccountStrategy(JoinStrategy):
    def __init__(self):
        self.accounts_by_client: Dict[str, List[Dict[str, str]]] = {}

    def __str__(self) -> str:
        return "AccountStrategy"

    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        if client is None:
            raise ValueError("client is required for AccountStrategy.join_batch")
        
        client_accounts = self.accounts_by_client.setdefault(client, [])
        for record in batch:
            client_accounts.append(record)

        return []

    def get_joined_for_client(self, client: str) -> List[Any]:
        return self.accounts_by_client.pop(client, [])