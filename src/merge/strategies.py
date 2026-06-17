from abc import ABC, abstractmethod
from src.common.communication.internal import TransactionRow
from typing import Any, Dict, List

class MergeStrategy(ABC):
    """Abstract strategy for filtering batches of messages.

    Implementations should provide `filter_batch(batch)` which receives
    a list of messages and returns a filtered list.
    """

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def merge_batch(self, batch: List[Any], client_id: str,  msg_type: str) -> List[dict]:
        raise NotImplementedError()
    
    @abstractmethod
    def get_client_state(self, client_id: str) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def set_client_state(self, client_id: str, state: Any) -> None:
        raise NotImplementedError()


class NoStrategy(MergeStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def merge_batch(self, batch: List[Any], client_id: str, msg_type: str) -> List[dict]:
        return batch
    
    def clear_client_state(self, client_id: str) -> None:
        pass

    def get_client_state(self, client_id: str) -> Any:
        return None

    def set_client_state(self, client_id: str, state: Any) -> None:
        pass


class AccountsStrategy:
    def __init__(self) -> None:
        self.accounts: Dict[str, Dict[str, str]] = {}
        self.joined_transactions: Dict[str, List[dict]] = {}

    def __str__(self) -> str:
        return "AccountsStrategy(left=From Bank right=Bank ID)"

    def merge_batch(self, batch: dict, client_id: str, msg_type: str) -> List[dict]:
        if msg_type == "raw_accounts":
            if client_id not in self.accounts:
                self.accounts[client_id] = {}
                
            for row in batch:
                bank_id = row.bank_id
                self.accounts[client_id][bank_id] = row.bank_name

            return []

        else:
            enriched_batch = []
                
            client_accounts = self.accounts.get(client_id, {})
            
            for row in batch:
                bank_id = row["from_bank"]
                bank_name = client_accounts.get(bank_id, "Unknown")
                
                enriched_row = row.copy()
                enriched_row["bank_name"] = bank_name
                enriched_batch.append(enriched_row)

            return enriched_batch

    def clear_client_state(self, client_id: str) -> None:
        """Clean up memory after the client finishes."""
        self.accounts.pop(client_id, None)
        self.joined_transactions.pop(client_id, None)

    def get_client_state(self, client_id: str) -> Any:
        return (
            self.accounts.get(client_id, {}),
            self.joined_transactions.get(client_id, [])
        )

    def set_client_state(self, client_id: str, state: Any) -> None:
        if state:
            self.accounts[client_id] = state[0]
            self.joined_transactions[client_id] = state[1]


class SelfMergeStrategy(MergeStrategy):
    """
    Detects A→B→C chains.
    """

    def __init__(self) -> None:
        self.inbound_txs: Dict[str, Dict[str, List[Dict]]] = {}
        self.outbound_txs: Dict[str, Dict[str, List[Dict]]] = {}

    def __str__(self) -> str:
        return "SelfMergeStrategy"

    def merge_batch(self, batch: List[Any], client_id: str, msg_type: str) -> List[dict]:
        joined_txs = []
        if client_id not in self.inbound_txs:
            self.inbound_txs[client_id] = {}
            self.outbound_txs[client_id] = {}

        client_inbound = self.inbound_txs[client_id]
        client_outbound = self.outbound_txs[client_id]

        for tx in batch:
            origin_key = f"{tx['from_bank']}_{tx['from_account']}"
            dest_key = f"{tx['to_bank']}_{tx['to_account']}"

            if origin_key in client_inbound:
                for inbound_tx in client_inbound[origin_key]:
                    merged_record = self._create_merged_record(inbound_tx, tx)
                    if merged_record is not None:
                        joined_txs.append(merged_record)

            if origin_key not in client_outbound:
                client_outbound[origin_key] = []
            client_outbound[origin_key].append(tx)

            if dest_key in client_outbound:
                for outbound_tx in client_outbound[dest_key]:
                    merged_record = self._create_merged_record(tx, outbound_tx)
                    if merged_record is not None:
                        joined_txs.append(merged_record)

            if dest_key not in client_inbound:
                client_inbound[dest_key] = []
            client_inbound[dest_key].append(tx)

        return joined_txs

    def _create_merged_record(self, tx_1: dict, tx_2: dict):
        if (
            tx_1["from_bank"] == tx_2["to_bank"]
            and tx_1["from_account"] == tx_2["to_account"]
        ):
            return None
        
        return TransactionRow(
            from_bank=tx_1.get("from_bank"),
            from_account=tx_1.get("from_account"),
            to_bank=tx_2.get("to_bank"),
            to_account=tx_2.get("to_account"),
        )

    def clear_client_state(self, client_id: str) -> None:
        self.inbound_txs.pop(client_id, None)
        self.outbound_txs.pop(client_id, None)

    def get_client_state(self, client_id: str) -> Any:
        return (
            self.inbound_txs.get(client_id, {}),
            self.outbound_txs.get(client_id, {})
        )

    def set_client_state(self, client_id: str, state: Any) -> None:
        if state:
            self.inbound_txs[client_id] = state[0]
            self.outbound_txs[client_id] = state[1]