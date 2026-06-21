from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set
import logging

logging.basicConfig(level=logging.INFO)


class AggregatorStrategy(ABC):
    """Abstract strategy for aggregating batches of messages."""

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def aggregate_batch(
        self, batch: List[Any], client: Optional[str] = None
    ) -> List[Any]:
        raise NotImplementedError()

    @abstractmethod
    def get_result_for_client(self, client: str) -> List[Any]:
        raise NotImplementedError()

    @abstractmethod
    def clear_client_state(self, client: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_client_state(self, client: str) -> Any:
        """Returns the raw internal memory for a specific client."""
        raise NotImplementedError()

    @abstractmethod
    def set_client_state(self, client: str, state: Any) -> None:
        """Restores the raw internal memory for a specific client."""
        raise NotImplementedError()


class NoStrategy(AggregatorStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def aggregate_batch(
        self, batch: List[Any], client: Optional[str] = None
    ) -> List[Any]:
        return batch

    def get_result_for_client(self, client: str) -> List[Any]:
        return []

    def clear_client_state(self, client: str) -> None:
        pass

    def get_client_state(self, client: str) -> Any:
        pass

    def set_client_state(self, client: str, state: Any) -> None:
        pass


class BankMaxAmountStrategy(AggregatorStrategy):
    def __init__(self):
        self.max_per_bank_by_client: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def __str__(self) -> str:
        return "BankMaxAmountStrategy"

    def aggregate_batch(
        self, batch: List[Any], client: Optional[str] = None
    ) -> List[Any]:
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

        return []

    def get_result_for_client(self, client: str) -> List[Any]:
        return [(None, list(self.max_per_bank_by_client.get(client, {}).values()))]

    def clear_client_state(self, client: str) -> None:
        self.max_per_bank_by_client.pop(client, None)

    def get_client_state(self, client: str) -> Any:
        return self.max_per_bank_by_client.get(client, {})

    def set_client_state(self, client: str, state: Any) -> None:
        if state:
            self.max_per_bank_by_client[client] = state


class AccountPairCountStategy(AggregatorStrategy):
    def __init__(self):
        self.counts_by_client: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def __str__(self) -> str:
        return "AccountPairCountStategy"

    def aggregate_batch(
        self, batch: List[Any], client: Optional[str] = None
    ) -> List[Any]:
        if client is None:
            raise ValueError("client is required for AccountPairCountStategy")

        counts = self.counts_by_client.setdefault(client, {})
        for tx in batch:
            key = f"{tx['from_bank']}_{tx['from_account']}_{tx['to_bank']}_{tx['to_account']}"
            count_from_batch = tx.get("count", 1)

            if key not in counts:
                counts[key] = {
                    "from_bank": tx["from_bank"],
                    "from_account": tx["from_account"],
                    "to_bank": tx["to_bank"],
                    "to_account": tx["to_account"],
                    "count": 0,
                }

            counts[key]["count"] += count_from_batch

        return []

    def get_result_for_client(self, client: str) -> List[Any]:
        counts = self.counts_by_client.get(client, {})
        return [(None, self._build_results(counts))]

    def clear_client_state(self, client: str) -> None:
        self.counts_by_client.pop(client, None)

    def _build_results(self, counts: Dict[str, Dict[str, Any]]) -> List[Any]:
        results = []

        for key, data in counts.items():
            if data["count"] > 5:
                results.append(data)

        return results

    def get_client_state(self, client: str) -> Any:
        return self.counts_by_client.get(client, {})

    def set_client_state(self, client: str, state: Any) -> None:
        if state:
            self.counts_by_client[client] = state


class CountStrategy(AggregatorStrategy):
    """Counts rows per client; emits [{"count": N}] on flush."""

    def __init__(self) -> None:
        self._counts: Dict[str, int] = {}

    def __str__(self) -> str:
        return "CountStrategy"

    def aggregate_batch(
        self, batch: List[Any], client: Optional[str] = None
    ) -> List[Any]:
        if client is None:
            raise ValueError("client is required for CountStrategy")
        self._counts[client] = self._counts.get(client, 0) + len(batch)
        return []

    def get_result_for_client(self, client: str) -> List[Any]:
        count = self._counts.pop(client, 0)
        return [(None, {"count": count})]

    def clear_client_state(self, client: str) -> None:
        self._counts.pop(client, None)

    def get_client_state(self, client: str) -> Any:
        return self._counts.get(client, 0)

    def set_client_state(self, client: str, state: Any) -> None:
        if state:
            self._counts[client] = state


class PaymentFormatAverageStrategy(AggregatorStrategy):
    def __init__(self):
        self.stats_by_client: Dict[str, Dict[str, Dict[str, Any]]] = {}

    def __str__(self) -> str:
        return "PaymentFormatAverageStrategy"

    def aggregate_batch(
        self, batch: List[Any], client: Optional[str] = None
    ) -> List[Any]:
        if client is None:
            raise ValueError("client is required for PaymentFormatAverageStrategy")

        stats = self.stats_by_client.setdefault(client, {})
        for tx in batch:
            fmt = tx["payment_format"]
            partial_amount = tx["total_amount"] or 0.0
            partial_count = tx["tx_quantity"] or 0
            key = str(fmt)

            if key not in stats:
                stats[key] = {"count": 0, "total": 0.0}

            stats[key]["count"] += partial_count
            stats[key]["total"] += partial_amount

        return []

    def get_result_for_client(self, client: str) -> List[Any]:
        stats = self.stats_by_client.get(client, {})

        results = []
        for fmt, stat in stats.items():
            count = stat["count"]
            average = stat["total"] / count if count > 0 else 0.0

            results.append(
                {
                    "payment_format": fmt,
                    "average_amount": average,
                }
            )

        return [(None, results)]

    def clear_client_state(self, client: str) -> None:
        self.stats_by_client.pop(client, None)

    def get_client_state(self, client: str) -> Any:
        return self.stats_by_client.get(client, {})

    def set_client_state(self, client: str, state: Any) -> None:
        if state:
            self.stats_by_client[client] = state


class AccountStrategy(AggregatorStrategy):
    def __init__(self):
        self.accounts_by_client: Dict[str, Set[tuple]] = {}

    def __str__(self) -> str:
        return "AccountStrategy"

    def aggregate_batch(self, batch: List[Any], client: str) -> List[Any]:
        if client not in self.accounts_by_client:
            self.accounts_by_client[client] = set()

        client_accounts = self.accounts_by_client[client]

        for record in batch:
            account_tuple = (record["bank"], record["account"])
            client_accounts.add(account_tuple)

        return client_accounts

    def get_result_for_client(self, client: str) -> List[Dict[str, str]]:
        if client not in self.accounts_by_client:
            return []

        final_accounts = []
        for bank, account in self.accounts_by_client.get(client, set()):
            final_accounts.append({"bank": bank, "account": account})

        return [(None, final_accounts)]

    def clear_client_state(self, client: str) -> None:
        self.accounts_by_client.pop(client, None)

    def get_client_state(self, client: str) -> Any:
        client_set = self.accounts_by_client.get(client, set())
        return list(client_set)

    def set_client_state(self, client: str, state: Any) -> None:
        if not state:
            return

        self.accounts_by_client[client] = set(tuple(item) for item in state)


class ScatterAggregatorStrategy(AggregatorStrategy):
    def __init__(self):
        self.state_by_client: Dict[str, Dict[tuple, Dict[str, Any]]] = {}

    def __str__(self) -> str:
        return "ScatterAggregatorStrategy"

    def aggregate_batch(
        self, batch: List[Any], client: Optional[str] = None
    ) -> List[Any]:
        if client is None:
            raise ValueError("client is required for ScatterAggregatorStrategy")

        client_state = self.state_by_client.setdefault(client, {})

        for tx in batch:
            origin = f"{tx['from_bank']}_{tx['from_account']}"
            dest = f"{tx['to_bank']}_{tx['to_account']}"
            data = client_state.setdefault(origin, {"dests": set(), "txs": []})
            data["dests"].add(dest)
            data["txs"].append(tx)

        return []

    def get_result_for_client(self, client: str) -> List[Any]:
        client_state = self.state_by_client.get(client, {})

        routed_results = []
        for origin, data in client_state.items():
            if len(data["dests"]) > 5:
                routed_results.append((origin, data["txs"]))

                for tx in data["txs"]:
                    dest = (tx["to_bank"], tx["to_account"])
                    routed_results.append((dest, [tx]))

        return routed_results

    def clear_client_state(self, client: str) -> None:
        self.state_by_client.pop(client, None)

    def get_client_state(self, client: str) -> Any:
        client_state = self.state_by_client.get(client, {})

        json_state = {}
        for origin, data in client_state.items():
            if data["txs"]:
                json_state[origin] = {
                    "dests": list(data["dests"]),
                    "txs": data["txs"],
                }

        return json_state

    def set_client_state(self, client: str, state: Any) -> None:
        if not state:
            return

        client_state = {}
        for origin, data in state.items():
            client_state[origin] = {"dests": set(data["dests"]), "txs": data["txs"]}

        self.state_by_client[client] = client_state
