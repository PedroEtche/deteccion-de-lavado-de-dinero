import zlib
from abc import ABC, abstractmethod
from common.communication.internal import (
    TransactionRow
) 
from typing import Any, Dict, List

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
    """Detects A→B→C chains shard-locally.

    Sharding contract: the upstream `MergeRoutingStrategy` ships every tx X→Y
    to BOTH `shard(X)` and `shard(Y)`. With that, each chain Z→B→Y is visible
    in at least 2 shards (shard(B) plus shard(Z)=shard(Y) when those collide).
    To avoid double-counting, emit a chain only from the shard that owns its
    intermediary B — that shard is unique per chain so we get exactly-once
    emission.

    When `shard_amount == 1` (or `shard_id is None`) the guard is a no-op and
    behavior matches the non-sharded case.
    """

    def __init__(self, shard_amount: int = 1, shard_id: int = 0) -> None:
        self.inbound_txs: Dict[str, Dict[tuple, List[Dict]]] = {}
        self.outbound_txs: Dict[str, Dict[tuple, List[Dict]]] = {}
        self.shard_amount = max(int(shard_amount), 1)
        self.shard_id = int(shard_id)

    def __str__(self) -> str:
        return f"SelfMergeStrategy(shard_id={self.shard_id}/{self.shard_amount})"

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

        return joined_txs

    def _owns_intermediary(self, intermediary_bank: Any, intermediary_account: Any) -> bool:
        """True iff B = (intermediary_bank, intermediary_account) hashes to this shard.

        Must use the same hash as `src.group.strategies._get_shard_route` so a
        chain Z→B→Y is emitted by exactly one SelfMerge instance.
        """
        if self.shard_amount <= 1:
            return True
        key = f"{intermediary_bank}_{intermediary_account}"
        return zlib.crc32(key.encode("utf-8")) % self.shard_amount == self.shard_id

    def _create_merged_record(self, tx_1: dict, tx_2: dict):
        # tx_1 = Z→B, tx_2 = B→Y. B is the intermediary.
        # Drop self-cycles (tx_1.from == tx_2.to).
        if tx_1["from_bank"] == tx_2["to_bank"] and tx_1["from_account"] == tx_2["to_account"]:
            return None

        if not self._owns_intermediary(tx_1["to_bank"], tx_1["to_account"]):
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