from typing import Any, Callable, List, Dict, Optional
from abc import ABC, abstractmethod
from datetime import datetime
import json
import logging
import operator as op
import urllib.request

from src.common.communication.internal import (
    TransactionRow,
)

_AMOUNT_OPS: Dict[str, Callable[[float, float], bool]] = {
    "<": op.lt,
    "=": op.eq,
    ">": op.gt,
}

_TIMESTAMP_FMT = "%Y/%m/%d %H:%M"

# Translate currency for the Frankfurter API
_CURRENCY_TRANSLATE_TABLE: Dict[str, str] = {
    "Euro": "EUR",
    "UK Pound": "GBP",
    "Australian Dollar": "AUD",
    "Canadian Dollar": "CAD",
    "Yen": "JPY",
    "Yuan": "CNY",
    "Swiss Franc": "CHF",
    "Ruble": "RUB",
    "Rupee": "INR",
    "Mexican Peso": "MXN",
    "Saudi Riyal": "SAR",
    "Shekel": "ILS",
    "Brazil Real": "BRL",
}


# TODO: Verificar que este forma de buscar los datos para el converter es correcta
def _fetch_rates_from_api(date_str: str) -> Dict[str, float]:
    """Fetches exchange rates from Frankfurter API for a given date (base=USD)."""
    url = f"https://api.frankfurter.app/{date_str}?base=USD"
    # Cloudflare blocks Python-urllib/* user-agent; use a neutral one.
    req = urllib.request.Request(url, headers={"User-Agent": "curl/7.88.1"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    return data["rates"]


class FilterStrategy(ABC):
    """Abstract strategy for filtering batches of messages.

    Implementations should provide `filter_batch(batch)` which receives
    a list of messages and returns a filtered list.
    """

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def filter_batch(self, batch) -> List[Any]:
        raise NotImplementedError()


class NoStrategy(FilterStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def filter_batch(self, batch: List[Any]) -> List[Any]:
        return batch


class CurrencyStrategy(FilterStrategy):
    def __init__(self, target_currency: str) -> None:
        self.target_currency = target_currency

    def __str__(self) -> str:
        return f"CurrencyStrategy(target_currency={self.target_currency})"

    def filter_batch(self, batch: List[Any]) -> List[Any]:
        filtered = [
            row for row in batch if row.payment_currency == self.target_currency
        ]

        if not filtered:
            return []

        return filtered


class AmountComparisonStrategy(FilterStrategy):
    def __init__(self, operator: str, threshold: float) -> None:
        if operator not in _AMOUNT_OPS:
            raise ValueError(
                f"Unknown operator.  List of valid ones: {list(_AMOUNT_OPS)}"
            )
        self._op = _AMOUNT_OPS[operator]
        self.threshold = float(threshold)

    def __str__(self) -> str:
        return f"AmountComparisonStrategy(amount_paid {self._op} {self.threshold})"

    def filter_batch(self, batch: List[Any]) -> List[Any]:
        # From Bank,Account,To Bank,Account.1,Amount Paid
        return [
            TransactionRow(
                from_bank=row.from_bank,
                from_account=row.from_account,
                to_bank=row.to_bank,
                to_account=row.to_account,
                amount_paid=row.amount_paid,
            )
            for row in batch
            if self._op(float(row.amount_received), self.threshold)
        ]


class CurrencyConversionStrategy(FilterStrategy):
    """Keeps transactions whose payment format is in `conditions` and whose
    amount, converted to USD using the exchange rate for its date (Frankfurter
    API, base=USD), is below 1 USD. The payment-format check runs first; only
    rows that pass it go through the (more expensive) currency conversion.
    Rates are cached by date to avoid repeated API calls. Stateless across
    batches — the cache is pure memoization.
    Bitcoin/unknown/incomplete rows are dropped.
    """

    def __init__(
        self,
        conditions: List[str],
        rate_fetcher: Optional[Callable[[str], Dict[str, float]]] = None,
    ) -> None:
        self.conditions = conditions
        self._rate_cache: Dict[str, Dict[str, float]] = {}
        self._rate_fetcher = rate_fetcher or _fetch_rates_from_api

    def __str__(self) -> str:
        return f"CurrencyConversionStrategy(formats={self.conditions})"

    def filter_batch(self, batch: List[Any]) -> List[Any]:
        # NOTE: Si la API no puede hacer el cambio (i.e Bitcoin) se descarta la fila por el momento
        result = []
        for row in batch:
            if row.payment_format not in self.conditions:
                continue
            converted_amount = self._to_usd(row)
            if converted_amount is not None and converted_amount < 1:
                result.append(row)
        return result

    def _to_usd(self, row: TransactionRow) -> Optional[float]:
        """Returns the transaction amount converted to USD, or None if it
        cannot be converted (incomplete/unsupported/unknown currency)."""
        currency = row.payment_currency
        if currency is None or row.amount_paid is None or row.timestamp is None:
            logging.warning("Dropping transaction with missing fields: %s", row)
            return None

        if currency == "Bitcoin":
            return None  # not supported by Frankfurter

        if currency == "US Dollar":
            return row.amount_paid

        currency_api = _CURRENCY_TRANSLATE_TABLE.get(currency)
        if currency_api is None:
            logging.warning("Unknown currency '%s'; dropping transaction", currency)
            return None

        date_str = self._parse_date(row.timestamp)
        rates = self._get_rates(date_str)
        rate = rates.get(currency_api)
        if rate is None:
            logging.warning(
                "No rate for %s on %s; dropping transaction", currency_api, date_str
            )
            return None

        return row.amount_paid / rate

    def _parse_date(self, timestamp: str) -> str:
        return datetime.strptime(timestamp, _TIMESTAMP_FMT).strftime("%Y-%m-%d")

    def _get_rates(self, date_str: str) -> Dict[str, float]:
        if date_str not in self._rate_cache:
            logging.info("Fetching exchange rates for %s", date_str)
            self._rate_cache[date_str] = self._rate_fetcher(date_str)
        return self._rate_cache[date_str]


class CountFieldComparisonStrategy(FilterStrategy):
    def __init__(self, operator: str, threshold: int) -> None:
        logging.info(
            "Initializing CountFieldComparisonStrategy with operator '%s' and threshold %d",
            operator,
            threshold,
        )
        if operator not in _AMOUNT_OPS:
            raise ValueError(
                f"Unknown operator. List of valid ones: {list(_AMOUNT_OPS)}"
            )
        self._op = _AMOUNT_OPS[operator]
        self.threshold = int(threshold)

    def __str__(self) -> str:
        return (
            f"CountFieldComparisonStrategy(count {self._op.__name__} {self.threshold})"
        )

    def filter_batch(self, batch: List[Any]) -> List[Any]:
        filtered_batch = []

        for row in batch:
            row_count = getattr(row, "count", None)
            if row_count is None and isinstance(row, dict):
                row_count = row.get("count", 0)

            if self._op(int(row_count), self.threshold):
                filtered_batch.append(row)

        return filtered_batch
