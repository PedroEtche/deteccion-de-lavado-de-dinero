import unittest

from src.communication.protocols.queue_protocol.internal import TransactionRow
from src.filter.strategies import (
    AmountLessThanStrategy,
    CurrencyStrategy,
    DateRangeRoute,
    DateStrategy,
    NoStrategy,
    PaymentFormatStrategy,
)
from datetime import datetime


class TestCurrencyStrategy(unittest.TestCase):

    def test_keeps_only_rows_matching_target_currency(self):
        batch = [
            TransactionRow(payment_currency="USD"),
            TransactionRow(payment_currency="EUR"),
            TransactionRow(payment_currency="USD"),
        ]
        result = CurrencyStrategy("out_q", "USD").filter_batch(batch)
        self.assertEqual(set(result.keys()), {"out_q"})
        self.assertEqual(len(result["out_q"]), 2)
        for row in result["out_q"]:
            self.assertEqual(row.payment_currency, "USD")

    def test_empty_batch_returns_empty_dict(self):
        self.assertEqual(CurrencyStrategy("out_q", "USD").filter_batch([]), {})

    def test_no_matches_returns_empty_dict(self):
        batch = [
            TransactionRow(payment_currency="EUR"),
            TransactionRow(payment_currency="GBP"),
        ]
        self.assertEqual(CurrencyStrategy("out_q", "USD").filter_batch(batch), {})

    def test_rows_with_none_currency_are_dropped(self):
        batch = [TransactionRow(payment_currency=None), TransactionRow(payment_currency="USD")]
        result = CurrencyStrategy("out_q", "USD").filter_batch(batch)
        self.assertEqual(len(result["out_q"]), 1)
        self.assertEqual(result["out_q"][0].payment_currency, "USD")

    def test_target_currency_is_case_sensitive(self):
        batch = [TransactionRow(payment_currency="usd"), TransactionRow(payment_currency="USD")]
        result = CurrencyStrategy("out_q", "USD").filter_batch(batch)
        self.assertEqual(len(result["out_q"]), 1)
        self.assertEqual(result["out_q"][0].payment_currency, "USD")


class TestAmountLessThanStrategy(unittest.TestCase):

    def test_keeps_only_rows_strictly_below_threshold(self):
        batch = [
            TransactionRow(amount_paid=49.99),
            TransactionRow(amount_paid=50.0),
            TransactionRow(amount_paid=50.01),
            TransactionRow(amount_paid=0.0),
        ]
        result = AmountLessThanStrategy("out_q", 50.0).filter_batch(batch)
        self.assertEqual(len(result["out_q"]), 2)
        for row in result["out_q"]:
            self.assertLess(row.amount_paid, 50.0)

    def test_empty_batch_returns_empty_dict(self):
        self.assertEqual(AmountLessThanStrategy("out_q", 50.0).filter_batch([]), {})

    def test_rows_with_none_amount_are_dropped(self):
        batch = [TransactionRow(amount_paid=None), TransactionRow(amount_paid=10.0)]
        result = AmountLessThanStrategy("out_q", 50.0).filter_batch(batch)
        self.assertEqual(len(result["out_q"]), 1)
        self.assertEqual(result["out_q"][0].amount_paid, 10.0)

    def test_negative_amounts_kept_when_below_threshold(self):
        batch = [TransactionRow(amount_paid=-5.0), TransactionRow(amount_paid=100.0)]
        result = AmountLessThanStrategy("out_q", 50.0).filter_batch(batch)
        self.assertEqual(len(result["out_q"]), 1)
        self.assertEqual(result["out_q"][0].amount_paid, -5.0)


class TestNoStrategy(unittest.TestCase):

    def test_returns_batch_unchanged_under_output_queue_key(self):
        batch = [
            TransactionRow(payment_currency="USD", amount_paid=10.0),
            TransactionRow(payment_currency="EUR", amount_paid=200.0),
        ]
        result = NoStrategy("out_q").filter_batch(batch)
        self.assertEqual(set(result.keys()), {"out_q"})
        self.assertEqual(result["out_q"], batch)


class TestPaymentFormatStrategy(unittest.TestCase):

    def test_keeps_only_allowed_formats(self):
        batch = [
            TransactionRow(payment_format="Wire"),
            TransactionRow(payment_format="ACH"),
            TransactionRow(payment_format="Cash"),
            TransactionRow(payment_format="Cheque"),
        ]
        result = PaymentFormatStrategy("out_q", ["Wire", "ACH"]).filter_batch(batch)
        self.assertEqual(set(result.keys()), {"out_q"})
        self.assertEqual(len(result["out_q"]), 2)
        for row in result["out_q"]:
            self.assertIn(row.payment_format, {"Wire", "ACH"})

    def test_empty_batch_returns_empty_dict(self):
        self.assertEqual(PaymentFormatStrategy("out_q", ["Wire"]).filter_batch([]), {})

    def test_no_matches_returns_empty_dict(self):
        batch = [TransactionRow(payment_format="Cash"), TransactionRow(payment_format="Cheque")]
        self.assertEqual(PaymentFormatStrategy("out_q", ["Wire", "ACH"]).filter_batch(batch), {})

    def test_none_payment_format_is_dropped(self):
        batch = [TransactionRow(payment_format=None), TransactionRow(payment_format="Wire")]
        result = PaymentFormatStrategy("out_q", ["Wire"]).filter_batch(batch)
        self.assertEqual(len(result["out_q"]), 1)

    def test_matching_is_case_sensitive(self):
        batch = [TransactionRow(payment_format="wire"), TransactionRow(payment_format="Wire")]
        result = PaymentFormatStrategy("out_q", ["Wire"]).filter_batch(batch)
        self.assertEqual(len(result["out_q"]), 1)
        self.assertEqual(result["out_q"][0].payment_format, "Wire")

    def test_single_format_filter(self):
        batch = [TransactionRow(payment_format="Wire"), TransactionRow(payment_format="ACH")]
        result = PaymentFormatStrategy("out_q", ["Wire"]).filter_batch(batch)
        self.assertEqual(len(result["out_q"]), 1)
        self.assertEqual(result["out_q"][0].payment_format, "Wire")


class TestDateStrategy(unittest.TestCase):

    def _make_strategy(self):
        route = DateRangeRoute(
            from_date=datetime(2022, 9, 1, 0, 0),
            to_date=datetime(2022, 9, 5, 23, 59),
            queue="q5_queue",
        )
        return DateStrategy(routes=[route])

    def test_keeps_rows_inside_date_range(self):
        strategy = self._make_strategy()
        batch = [
            TransactionRow(timestamp="2022/09/01 00:00"),
            TransactionRow(timestamp="2022/09/03 12:00"),
            TransactionRow(timestamp="2022/09/05 23:59"),
        ]
        result = strategy.filter_batch(batch)
        self.assertEqual(len(result["q5_queue"]), 3)

    def test_drops_rows_outside_date_range(self):
        strategy = self._make_strategy()
        batch = [
            TransactionRow(timestamp="2022/08/31 23:59"),
            TransactionRow(timestamp="2022/09/06 00:00"),
        ]
        result = strategy.filter_batch(batch)
        self.assertEqual(result, {})

    def test_mixed_batch_routes_correctly(self):
        strategy = self._make_strategy()
        batch = [
            TransactionRow(timestamp="2022/09/02 10:00"),   # inside
            TransactionRow(timestamp="2022/09/10 10:00"),   # outside
            TransactionRow(timestamp="2022/09/04 08:00"),   # inside
        ]
        result = strategy.filter_batch(batch)
        self.assertEqual(len(result["q5_queue"]), 2)

    def test_none_timestamp_is_skipped(self):
        strategy = self._make_strategy()
        batch = [TransactionRow(timestamp=None), TransactionRow(timestamp="2022/09/03 00:00")]
        result = strategy.filter_batch(batch)
        self.assertEqual(len(result["q5_queue"]), 1)

    def test_invalid_timestamp_format_is_skipped(self):
        strategy = self._make_strategy()
        batch = [TransactionRow(timestamp="not-a-date"), TransactionRow(timestamp="2022/09/03 00:00")]
        result = strategy.filter_batch(batch)
        self.assertEqual(len(result["q5_queue"]), 1)

    def test_multiple_routes(self):
        routes = [
            DateRangeRoute(datetime(2022, 9, 1), datetime(2022, 9, 5, 23, 59), "q_early"),
            DateRangeRoute(datetime(2022, 9, 6), datetime(2022, 9, 15, 23, 59), "q_late"),
        ]
        strategy = DateStrategy(routes=routes)
        batch = [
            TransactionRow(timestamp="2022/09/03 00:00"),
            TransactionRow(timestamp="2022/09/10 00:00"),
            TransactionRow(timestamp="2022/09/20 00:00"),
        ]
        result = strategy.filter_batch(batch)
        self.assertEqual(len(result["q_early"]), 1)
        self.assertEqual(len(result["q_late"]), 1)


if __name__ == "__main__":
    unittest.main()
