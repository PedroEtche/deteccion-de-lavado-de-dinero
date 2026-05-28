import unittest
import uuid
from unittest.mock import MagicMock, patch

from src.communication.protocols.queue_protocol.internal import (
    TransactionRow,
    deserialize,
    serialize,
)
from src.currency_converter.main import CurrencyConverter, CurrencyConverterConfig, CurrencyConverterService


def _fake_fetcher(rates_by_date):
    """Returns a rate fetcher that serves from a fixed dict keyed by date string."""
    def fetcher(date_str):
        return rates_by_date[date_str]
    return fetcher


def _tx(**kwargs):
    defaults = dict(
        timestamp="2022/09/03 10:00",
        from_bank="B1", from_account="A1",
        to_bank="B2", to_account="A2",
        amount_paid=100.0,
        payment_currency="US Dollar",
        payment_format="Wire",
    )
    defaults.update(kwargs)
    return TransactionRow(**defaults)


class TestCurrencyConverter(unittest.TestCase):

    def setUp(self):
        self.rates = {"2022-09-03": {"EUR": 0.9, "GBP": 0.8, "JPY": 140.0}}
        self.converter = CurrencyConverter(rate_fetcher=_fake_fetcher(self.rates))

    # --- USD passthrough ---

    def test_usd_transaction_passes_through_unchanged(self):
        row = _tx(amount_paid=50.0, payment_currency="US Dollar")
        result = self.converter.convert_batch([row])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].amount_paid, 50.0)
        self.assertEqual(result[0].payment_currency, "US Dollar")

    # --- Bitcoin drop ---

    def test_bitcoin_transaction_is_dropped(self):
        row = _tx(payment_currency="Bitcoin", amount_paid=0.001)
        result = self.converter.convert_batch([row])
        self.assertEqual(result, [])

    # --- Currency conversion ---

    def test_euro_converted_to_usd(self):
        # EUR rate 0.9 means 1 USD = 0.9 EUR  →  100 EUR / 0.9 ≈ 111.11 USD
        row = _tx(payment_currency="Euro", amount_paid=100.0)
        result = self.converter.convert_batch([row])
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result[0].amount_paid, 100.0 / 0.9, places=6)
        self.assertEqual(result[0].payment_currency, "US Dollar")

    def test_gbp_converted_to_usd(self):
        row = _tx(payment_currency="UK Pound", amount_paid=80.0)
        result = self.converter.convert_batch([row])
        self.assertAlmostEqual(result[0].amount_paid, 80.0 / 0.8, places=6)

    def test_other_fields_are_preserved_after_conversion(self):
        row = _tx(payment_currency="Euro", amount_paid=50.0, from_bank="MyBank", from_account="ACC99")
        result = self.converter.convert_batch([row])
        self.assertEqual(result[0].from_bank, "MyBank")
        self.assertEqual(result[0].from_account, "ACC99")

    # --- Unknown / missing fields ---

    def test_unknown_currency_is_dropped(self):
        row = _tx(payment_currency="Doubloon")
        result = self.converter.convert_batch([row])
        self.assertEqual(result, [])

    def test_none_currency_is_dropped(self):
        row = _tx(payment_currency=None)
        result = self.converter.convert_batch([row])
        self.assertEqual(result, [])

    def test_none_amount_is_dropped(self):
        row = _tx(payment_currency="Euro", amount_paid=None)
        result = self.converter.convert_batch([row])
        self.assertEqual(result, [])

    def test_none_timestamp_is_dropped(self):
        row = _tx(payment_currency="Euro", amount_paid=50.0, timestamp=None)
        result = self.converter.convert_batch([row])
        self.assertEqual(result, [])

    # --- Mixed batch ---

    def test_mixed_batch_keeps_valid_drops_invalid(self):
        batch = [
            _tx(payment_currency="US Dollar", amount_paid=10.0),
            _tx(payment_currency="Bitcoin", amount_paid=0.001),
            _tx(payment_currency="Euro", amount_paid=90.0),
            _tx(payment_currency="Doubloon", amount_paid=5.0),
        ]
        result = self.converter.convert_batch(batch)
        self.assertEqual(len(result), 2)
        currencies = [r.payment_currency for r in result]
        self.assertEqual(currencies, ["US Dollar", "US Dollar"])

    # --- Rate cache ---

    def test_rates_are_cached_per_date(self):
        call_count = [0]
        def counting_fetcher(date_str):
            call_count[0] += 1
            return {"EUR": 0.9}

        converter = CurrencyConverter(rate_fetcher=counting_fetcher)
        batch = [
            _tx(payment_currency="Euro", amount_paid=10.0, timestamp="2022/09/03 08:00"),
            _tx(payment_currency="Euro", amount_paid=20.0, timestamp="2022/09/03 14:00"),
        ]
        converter.convert_batch(batch)
        self.assertEqual(call_count[0], 1, "API should be called once for the same date")

    def test_different_dates_fetch_separately(self):
        rates = {
            "2022-09-03": {"EUR": 0.9},
            "2022-09-04": {"EUR": 0.95},
        }
        converter = CurrencyConverter(rate_fetcher=_fake_fetcher(rates))
        batch = [
            _tx(payment_currency="Euro", amount_paid=100.0, timestamp="2022/09/03 08:00"),
            _tx(payment_currency="Euro", amount_paid=100.0, timestamp="2022/09/04 08:00"),
        ]
        result = converter.convert_batch(batch)
        self.assertAlmostEqual(result[0].amount_paid, 100.0 / 0.9, places=6)
        self.assertAlmostEqual(result[1].amount_paid, 100.0 / 0.95, places=6)


class TestCurrencyConverterService(unittest.TestCase):

    def _make_service(self):
        config = CurrencyConverterConfig(
            mom_host="ignored",
            input_queue="in_q",
            output_queue="out_q",
            log_level="INFO",
        )
        rates = {"2022-09-03": {"EUR": 0.9}}
        converter = CurrencyConverter(rate_fetcher=_fake_fetcher(rates))

        with patch("src.currency_converter.main.MessageMiddlewareQueueRabbitMQ") as mw_cls:
            mw_cls.side_effect = lambda host, name: MagicMock(name=f"queue:{name}")
            service = CurrencyConverterService(config, converter=converter)

        return service

    def _raw_tx_msg(self, client, batch):
        return serialize({
            "type": "raw_transactions",
            "client": client,
            "msg_id": str(uuid.uuid4()),
            "payload": {"batch_size": len(batch), "batch": batch},
        })

    def _eof_msg(self, client):
        return serialize({"type": "eof", "client": client, "msg_id": str(uuid.uuid4())})

    def test_eof_is_forwarded(self):
        service = self._make_service()
        service._on_message(self._eof_msg("c1"), MagicMock(), MagicMock())
        service._output.send.assert_called_once()
        sent = deserialize(service._output.send.call_args[0][0])
        self.assertEqual(sent["type"], "eof")
        self.assertEqual(sent["client"], "c1")

    def test_converted_batch_is_sent(self):
        service = self._make_service()
        batch = [_tx(payment_currency="Euro", amount_paid=100.0)]
        service._on_message(self._raw_tx_msg("c1", batch), MagicMock(), MagicMock())
        service._output.send.assert_called_once()
        sent = deserialize(service._output.send.call_args[0][0])
        self.assertEqual(sent["type"], "raw_transactions")
        self.assertEqual(sent["payload"]["batch_size"], 1)
        self.assertAlmostEqual(sent["payload"]["batch"][0].amount_paid, 100.0 / 0.9, places=6)

    def test_all_dropped_batch_sends_nothing(self):
        service = self._make_service()
        batch = [_tx(payment_currency="Bitcoin", amount_paid=0.001)]
        service._on_message(self._raw_tx_msg("c1", batch), MagicMock(), MagicMock())
        service._output.send.assert_not_called()

    def test_eof_gets_new_msg_id(self):
        service = self._make_service()
        original = self._eof_msg("c1")
        original_id = deserialize(original)["msg_id"]
        service._on_message(original, MagicMock(), MagicMock())
        forwarded = deserialize(service._output.send.call_args[0][0])
        self.assertNotEqual(forwarded["msg_id"], original_id)


if __name__ == "__main__":
    unittest.main()
