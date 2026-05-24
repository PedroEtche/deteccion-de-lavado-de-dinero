from abc import ABC, abstractmethod
from datetime import date
from typing import Any, List

class GroupStrategy(ABC):
    """Abstract strategy for grouping batches of messages."""

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def group_batch(self, batch: List[Any]) -> List[Any]:
        raise NotImplementedError()


class NoStrategy(GroupStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def group_batch(self, batch: List[Any]) -> List[Any]:
        return batch

# @dataclass
# class TransactionRow(Payload):
#     timestamp: str | None = None
#     from_bank: str | None = None
#     from_account: str | None = None
#     to_bank: str | None = None
#     to_account: str | None = None
#     amount_received: float | None = None
#     receiving_currency: str | None = None
#     amount_paid: float | None = None
#     payment_currency: str | None = None
#     payment_format: str | None = None

class BankMaxAmountStrategy(GroupStrategy):
    def __init__(self, max_amount: float) -> None:
        self.max_amount = max_amount

    def __str__(self) -> str:
        return f"BankMaxAmountStrategy(max_amount={self.max_amount})"

    def group_batch(self, batch):
        max_per_bank = {}
        for tx in batch:
            bank = tx.from_bank
            amount = tx.amount_paid or 0.0
            current = max_per_bank.get(bank)

            if current is None or amount > current["Amount Paid"]:
                max_per_bank[bank] = {
                    "From Bank": tx.from_bank,
                    "Account": tx.from_account,
                    "Amount Paid": amount,
                }

        return list(max_per_bank.values())
            

class CurrencyStrategy(GroupStrategy):
    def __init__(self, target_currency: str) -> None:
        self.target_currency = target_currency

    def __str__(self) -> str:
        return f"CurrencyStrategy(target_currency={self.target_currency})"

    def group_batch(self, batch: List[Any]) -> List[Any]:
        raise NotImplementedError("implement later")


class DateStrategy(GroupStrategy):
    def __init__(self, from_date: date, to_date: date) -> None:
        self.from_date = from_date
        self.to_date = to_date

    def __str__(self) -> str:
        return f"DateStrategy(from_date={self.from_date}, to_date={self.to_date})"

    def group_batch(self, batch: List[Any]) -> List[Any]:
        raise NotImplementedError("implement later")
