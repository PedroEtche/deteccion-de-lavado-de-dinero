from typing import Any, List, Dict
from abc import ABC, abstractmethod
from src.communication.protocols.queue_protocol.internal import (
    TransactionRow
) 

class JoinerStrategy(ABC):
    """Abstract strategy for filtering batches of messages.

    Implementations should provide `filter_batch(batch)` which receives
    a list of messages and returns a filtered list.
    """

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def joiner_batch(self, batch: List[Any], client_id: str):
        raise NotImplementedError()


class NoStrategy(JoinerStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def joiner_batch(self, batch: List[Any], client_id: str):
        return batch


class AccountsStrategy(JoinerStrategy):
    def __init__(self) -> None:
        self.data = {}

    def __str__(self) -> str:
        return "AccountsStrategy(left=From Bank right=Bank ID)"

    def joiner_batch(self, batch: List[Any], client_id: str):
        msg_type = batch.get("type")
        payload = batch.get("payload", {})
        rows = payload.get("batch", [])

        if msg_type == "raw_transactions":
            for row in rows:
                bank_id = row["From Bank"]
                if bank_id not in self.data:
                    self.data[bank_id] = {}
                self.data[bank_id]["From Bank"] = bank_id
                self.data[bank_id]["Account"] = row["Account"]
                self.data[bank_id]["Amount Paid"] = row["Amount Paid"]

        else: # msg_type == "raw_accounts"
            for row in rows:
                bank_id = row["Bank ID"]
                if bank_id not in self.data:
                    self.data[bank_id] = {}
                self.data[bank_id]["Bank ID"] = bank_id
                self.data[bank_id]["Bank Name"] = row["Bank Name"]

class SelfMergeStrategy(JoinerStrategy):
    def __init__(self) -> None:
        self.inbound_txs: Dict[str, Dict[tuple, List[Dict]]] = {}
        self.outbound_txs: Dict[str, Dict[tuple, List[Dict]]] = {}

    def __str__(self) -> str:
        return "SelfMergeStrategy"

    def joiner_batch(self, batch: List[Any], client_id: str):
        
        joined_txs = []

        if client_id not in self.inbound_txs:
            self.inbound_txs[client_id] = {}
            self.outbound_txs[client_id] = {}

        client_inbound = self.inbound_txs[client_id]
        client_outbound = self.outbound_txs[client_id]

        for tx in batch:
            origin_key = (tx["from_bank"], tx["from_account"])
            dest_key = (tx["to_bank"], tx["to_account"])

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

        if joined_txs:
             return joined_txs
             
        return []
    
    def _create_merged_record(self, tx_1: dict, tx_2: dict) -> TransactionRow:
        if tx_1["from_bank"] == tx_2["to_bank"] and tx_1["from_account"] == tx_2["to_account"]:
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