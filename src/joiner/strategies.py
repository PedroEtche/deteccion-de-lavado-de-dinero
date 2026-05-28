from typing import Any
from abc import ABC, abstractmethod

class JoinerStrategy(ABC):
    """Abstract strategy for filtering batches of messages.

    Implementations should provide `filter_batch(batch)` which receives
    a list of messages and returns a filtered list.
    """

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def joiner_batch(self, batch: Any):
        raise NotImplementedError()


class NoStrategy(JoinerStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def joiner_batch(self, batch: Any):
        return batch

class AccountsStrategy(JoinerStrategy):
    def __init__(self) -> None:
        self.data = {}

    def __str__(self) -> str:
        return f"AccountsStrategy(left=From Bank right=Bank ID)"

    def joiner_batch(self, batch: Any):
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
