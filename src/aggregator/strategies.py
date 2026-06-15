from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set

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


class AccountPairCountStategy(AggregatorStrategy):
    def __init__(self):
        self.counts_by_client: Dict[str, Dict[tuple, int]] = {}

    def __str__(self) -> str:
        return "AccountPairCountStategy"

    def aggregate_batch(
        self, batch: List[Any], client: Optional[str] = None
    ) -> List[Any]:
        if client is None:
            raise ValueError("client is required for AccountPairCountStategy")

        counts = self.counts_by_client.setdefault(client, {})
        for tx in batch:
            key = (tx["from_bank"], tx["from_account"], tx["to_bank"], tx["to_account"])
            count_from_batch = tx["count"] if "count" in tx else 1
            counts[key] = counts.get(key, 0) + count_from_batch

        return []

    def get_result_for_client(self, client: str) -> List[Any]:
        counts = self.counts_by_client.get(client, {})
        return [(None, self._build_results(counts))]

    def clear_client_state(self, client: str) -> None:
        self.counts_by_client.pop(client, None)

    def _build_results(self, counts: Dict[tuple, int]) -> List[Any]:
        results = []
        for (from_bank, from_account, to_bank, to_account), count in counts.items():
            if count > 5:
                results.append(
                    {
                        "from_bank": from_bank,
                        "from_account": from_account,
                        "to_bank": to_bank,
                        "to_account": to_account,
                        "count": count,
                    }
                )
        return results


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
            origin = (tx["from_bank"], tx["from_account"])
            dest = (tx["to_bank"], tx["to_account"])
            data = client_state.setdefault(origin, {"dests": set(), "txs": []})
            data["dests"].add(dest)
            data["txs"].append(tx)

        return []

    def get_result_for_client(self, client: str) -> List[Any]:
        client_state = self.state_by_client.get(client, {})

        routed_results = []
        for dest, data in client_state.items():
            if len(data["dests"]) > 5:
                routed_results.append((dest, data["txs"]))

        return routed_results

    def clear_client_state(self, client: str) -> None:
        self.state_by_client.pop(client, None)
