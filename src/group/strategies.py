from abc import ABC, abstractmethod
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

class BankMaxAmountStrategy(GroupStrategy):
    def __str__(self) -> str:
        return "BankMaxAmountStrategy"

    def group_batch(self, batch: List[Any]) -> List[Any]:
        max_per_bank = {}
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
            
class PaymentFormatAverageStrategy(GroupStrategy):
    def __str__(self) -> str:
        return "PaymentFormatAverageStrategy"

    def group_batch(self, batch: List[Any]) -> List[Any]:
        totals = {}
        counts = {}

        for tx in batch:
            payment_format = tx["payment_format"]
            amount = tx["amount_paid"]

            totals[payment_format] = totals.get(payment_format, 0.0) + amount
            counts[payment_format] = counts.get(payment_format, 0) + 1

        count_total_by_format = {}
        for payment_format, count in counts.items():
            count_total_by_format[payment_format] = {
                "tx_quantity": count,
                "total_amount": totals[payment_format],
            }

        return count_total_by_format

class AccountPairCountStategy(GroupStrategy):
    def __str__(self) -> str:
        return "AccountPairCountStategy()"

    def group_batch(self, batch: List[Any]) -> List[Any]:
        counts = {}
        for tx in batch:
            key = (tx["from_bank"], tx["from_account"], tx["to_bank"], tx["to_account"])
            counts[key] = counts.get(key, 0) + 1

        results = []
        for (from_bank, from_account, to_bank, to_account), size in counts.items():
            results.append(
                {
                    "from_bank": from_bank,
                    "from_account": from_account,
                    "to_bank": to_bank,
                    "to_account": to_account,
                    "size": size,
                }
            )   

        return results
        
class AccountStrategy(GroupStrategy):
    # aca hay que revisar caso en query 4
    # como vamos a routear las cuentas? es necesario mantener un estado?
    def __str__(self) -> str:
        return f"AccountStrategy()"

    def group_batch(self, batch: List[Any]) -> List[Any]:
        accounts = set()
        for tx in batch:
            accounts.add((tx["from_bank"], tx["from_account"]))
            accounts.add((tx["to_bank"], tx["to_account"]))

        return [{"bank": bank, "account": account} for bank, account in accounts]