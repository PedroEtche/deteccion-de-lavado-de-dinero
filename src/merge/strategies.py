from abc import ABC, abstractmethod
from typing import Any, Dict, List

from src.common.communication.internal import TransactionRow

class MergeStrategy(ABC):
    """Abstract strategy for accumulating and merging data batches."""

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def merge_batch(self, batch: List[Any], client_id: str, msg_type: str) -> None:
        """Accumulates data into memory. Does NOT return the result."""
        raise NotImplementedError()

    @abstractmethod
    def get_result_for_client(self, client_id: str) -> List[Any]:
        """Called during flush. Executes the final join and returns the result."""
        raise NotImplementedError()

    @abstractmethod
    def get_client_state(self, client_id: str) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def set_client_state(self, client_id: str, state: Any) -> None:
        raise NotImplementedError()
        
    @abstractmethod
    def clear_client_state(self, client_id: str) -> None:
        raise NotImplementedError()


class NoStrategy(MergeStrategy):
    """A strategy that just buffers everything and returns it."""
    
    def __init__(self):
        self.accumulated = {}

    def __str__(self) -> str:
        return "NoStrategy"

    def merge_batch(self, batch: List[Any], client_id: str, msg_type: str) -> None:
        if client_id not in self.accumulated:
            self.accumulated[client_id] = []
        self.accumulated[client_id].extend(batch)

    def get_result_for_client(self, client_id: str) -> List[Any]:
        return self.accumulated.get(client_id, [])

    def clear_client_state(self, client_id: str) -> None:
        self.accumulated.pop(client_id, None)

    def get_client_state(self, client_id: str) -> Any:
        return self.accumulated.get(client_id, [])

    def set_client_state(self, client_id: str, state: Any) -> None:
        if state:
            self.accumulated[client_id] = state


class AccountsStrategy(MergeStrategy):
    def __init__(self) -> None:
        self.accounts: Dict[str, Dict[str, str]] = {}
        self.pending_transactions: Dict[str, List[dict]] = {}

    def __str__(self) -> str:
        return "AccountsStrategy(left=From Bank right=Bank ID)"

    def merge_batch(self, batch: List[Any], client_id: str, msg_type: str) -> None:
        """Just accumulate the data. Order of arrival no longer matters!"""
        if msg_type == "raw_accounts":
            if client_id not in self.accounts:
                self.accounts[client_id] = {}
            for row in batch:
                self.accounts[client_id][row.bank_id] = row.bank_name
        else:
            if client_id not in self.pending_transactions:
                self.pending_transactions[client_id] = []
            self.pending_transactions[client_id].extend(batch)

    def get_result_for_client(self, client_id: str) -> List[Any]:
        """Perform the actual Join once all EOFs have arrived."""
        enriched_batch = []
        client_accounts = self.accounts.get(client_id, {})
        transactions = self.pending_transactions.get(client_id, [])

        for row in transactions:
<<<<<<< HEAD
            bank_id = str(row["from_bank"]).lstrip('0')
=======
            bank_id = str(row["from_bank"]).lstrip('0') or '0'
>>>>>>> e04d3ac (less commented code)
            bank_name = client_accounts.get(bank_id, "Unknown")

            enriched_batch.append({
                "from_bank": row.get("from_bank"),
                "from_account": row.get("from_account"),
                "bank_name": bank_name,
                "amount_paid": row.get("amount_paid"),
            })

        return enriched_batch

    def clear_client_state(self, client_id: str) -> None:
        self.accounts.pop(client_id, None)
        self.pending_transactions.pop(client_id, None)

    def get_client_state(self, client_id: str) -> Any:
        return (
            self.accounts.get(client_id, {}),
            self.pending_transactions.get(client_id, []),
        )

    def set_client_state(self, client_id: str, state: Any) -> None:
        if state:
            self.accounts[client_id] = state[0]
            self.pending_transactions[client_id] = state[1]


class SelfMergeStrategy(MergeStrategy):
    """
    Detects A→B→C chains. 
    This uses a Symmetric Hash Join, so it naturally handles out-of-order data, 
    but we buffer the *results* to emit on flush.
    """

    def __init__(self) -> None:
        self.inbound_txs: Dict[str, Dict[str, List[Dict]]] = {}
        self.outbound_txs: Dict[str, Dict[str, List[Dict]]] = {}
        self.merged_results: Dict[str, List[TransactionRow]] = {}

    def __str__(self) -> str:
        return "SelfMergeStrategy"

    def merge_batch(self, batch: List[Any], client_id: str, msg_type: str) -> None:
        if client_id not in self.inbound_txs:
            self.inbound_txs[client_id] = {}
            self.outbound_txs[client_id] = {}
            self.merged_results[client_id] = []

        client_inbound = self.inbound_txs[client_id]
        client_outbound = self.outbound_txs[client_id]
        client_results = self.merged_results[client_id]

        for tx in batch:
            origin_key = f"{tx['from_bank']}_{tx['from_account']}"
            dest_key = f"{tx['to_bank']}_{tx['to_account']}"

            if origin_key in client_inbound:
                for inbound_tx in client_inbound[origin_key]:
                    merged_record = self._create_merged_record(inbound_tx, tx)
                    if merged_record is not None:
                        client_results.append(merged_record)

            if origin_key not in client_outbound:
                client_outbound[origin_key] = []
            client_outbound[origin_key].append(tx)

            if dest_key in client_outbound:
                for outbound_tx in client_outbound[dest_key]:
                    merged_record = self._create_merged_record(tx, outbound_tx)
                    if merged_record is not None:
                        client_results.append(merged_record)
<<<<<<< HEAD

=======
                        
>>>>>>> e04d3ac (less commented code)
            if dest_key not in client_inbound:
                client_inbound[dest_key] = []
            client_inbound[dest_key].append(tx)

    def get_result_for_client(self, client_id: str) -> List[Any]:
        """Return the completed chains."""
        return self.merged_results.get(client_id, [])

    def _create_merged_record(self, tx_1: dict, tx_2: dict):
        if (
            tx_1["from_bank"] == tx_2["to_bank"]
            and tx_1["from_account"] == tx_2["to_account"]
        ):
            return None

        return ({
            "from_bank": tx_1.get("from_bank"),
            "from_account": tx_1.get("from_account"),
            "to_bank": tx_2.get("to_bank"),
            "to_account": tx_2.get("to_account"),
        })

    def clear_client_state(self, client_id: str) -> None:
        self.inbound_txs.pop(client_id, None)
        self.outbound_txs.pop(client_id, None)
        self.merged_results.pop(client_id, None)

    def get_client_state(self, client_id: str) -> Any:
        return (
            self.inbound_txs.get(client_id, {}),
            self.outbound_txs.get(client_id, {}),
            self.merged_results.get(client_id, [])
        )

    def set_client_state(self, client_id: str, state: Any) -> None:
        if state:
            self.inbound_txs[client_id] = state[0]
            self.outbound_txs[client_id] = state[1]
            self.merged_results[client_id] = state[2]