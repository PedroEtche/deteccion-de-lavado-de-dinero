import json
import logging
import os
import signal
import urllib.request
import uuid
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Callable, Dict, List, Optional

import yaml

from src.common.middleware import MessageMiddlewareQueueRabbitMQ
from src.communication.protocols.queue_protocol.internal import (
    TransactionRow,
    build_eof_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)

CONFIG_PATH = "./config.yaml"

_TIMESTAMP_FMT = "%Y/%m/%d %H:%M"

# Maps the display currency names used in transaction data to ISO 4217 codes
# understood by the Frankfurter API (base=USD).
_CURRENCY_ISO: Dict[str, str] = {
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


def _fetch_rates_from_api(date_str: str) -> Dict[str, float]:
    """Fetches exchange rates from Frankfurter API for a given date (base=USD)."""
    url = f"https://api.frankfurter.app/{date_str}?base=USD"
    # Cloudflare blocks Python-urllib/* user-agent; use a neutral one.
    req = urllib.request.Request(url, headers={"User-Agent": "curl/7.88.1"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    return data["rates"]


@dataclass
class CurrencyConverterConfig:
    mom_host: str
    input_queue: str
    output_queue: str
    log_level: str


def _load_file_config() -> Dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def init_config() -> CurrencyConverterConfig:
    file_config = _load_file_config()
    return CurrencyConverterConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
    )


class CurrencyConverter:
    """Pure conversion logic — no middleware, easy to unit-test.

    Converts amount_paid to USD using Frankfurter API exchange rates.
    Rates are cached by date to avoid repeated API calls.
    Bitcoin transactions are silently dropped (Frankfurter does not support them).
    """

    def __init__(self, rate_fetcher: Optional[Callable[[str], Dict[str, float]]] = None) -> None:
        self._rate_cache: Dict[str, Dict[str, float]] = {}
        self._rate_fetcher = rate_fetcher or _fetch_rates_from_api

    def convert_batch(self, batch: List[TransactionRow]) -> List[TransactionRow]:
        result = []
        for row in batch:
            converted = self._convert_row(row)
            if converted is not None:
                result.append(converted)
        return result

    def _convert_row(self, row: TransactionRow) -> Optional[TransactionRow]:
        currency = row.payment_currency
        if currency is None or row.amount_paid is None or row.timestamp is None:
            logging.warning("Dropping transaction with missing fields: %s", row)
            return None

        if currency == "Bitcoin":
            return None  # not supported by Frankfurter

        if currency == "US Dollar":
            return row  # already in USD

        iso = _CURRENCY_ISO.get(currency)
        if iso is None:
            logging.warning("Unknown currency '%s'; dropping transaction", currency)
            return None

        date_str = self._parse_date(row.timestamp)
        rates = self._get_rates(date_str)
        rate = rates.get(iso)
        if rate is None:
            logging.warning("No rate for %s on %s; dropping transaction", iso, date_str)
            return None

        # rates[iso] = units of currency per 1 USD  →  USD = amount / rate
        converted_amount = row.amount_paid / rate
        return replace(row, amount_paid=converted_amount, payment_currency="US Dollar")

    def _parse_date(self, timestamp: str) -> str:
        return datetime.strptime(timestamp, _TIMESTAMP_FMT).strftime("%Y-%m-%d")

    def _get_rates(self, date_str: str) -> Dict[str, float]:
        if date_str not in self._rate_cache:
            logging.info("Fetching exchange rates for %s", date_str)
            self._rate_cache[date_str] = self._rate_fetcher(date_str)
        return self._rate_cache[date_str]


class CurrencyConverterService:
    def __init__(self, config: CurrencyConverterConfig, converter: Optional[CurrencyConverter] = None) -> None:
        self._input = MessageMiddlewareQueueRabbitMQ(config.mom_host, config.input_queue)
        self._output = MessageMiddlewareQueueRabbitMQ(config.mom_host, config.output_queue)
        self._converter = converter or CurrencyConverter()
        self._running = False

    def start(self) -> None:
        logging.info("Starting currency converter service")
        self._running = True
        try:
            self._input.start_consuming(self._on_message)
        finally:
            self._input.close()
            self._output.close()

    def stop(self) -> None:
        logging.info("Stopping currency converter service")
        self._running = False
        try:
            self._input.stop_consuming()
        except Exception:
            logging.exception("error stopping consumer")

    def _on_message(self, message: bytes, ack, _nack) -> None:
        decoded = deserialize(message)
        client = decoded["client"]

        if decoded["type"] == "eof":
            logging.info("Forwarding EOF for client %s", client)
            self._output.send(serialize(build_eof_message(client=client, msg_id=str(uuid.uuid4()))))
            ack()
            return

        batch = decoded["payload"]["batch"]
        converted = self._converter.convert_batch(batch)
        if converted:
            self._output.send(
                serialize(
                    build_raw_transactions_message(
                        client=client,
                        msg_id=str(uuid.uuid4()),
                        batch=converted,
                    )
                )
            )
        ack()


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    logging.info(
        "Currency converter startup: mom_host=%s input=%s output=%s",
        config.mom_host, config.input_queue, config.output_queue,
    )
    service = CurrencyConverterService(config)

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    service.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
