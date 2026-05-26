import unittest

from src.communication.protocols.queue_protocol.internal import TransactionRow
from src.filter.strategies import (
    AmountLessThanStrategy,
    CurrencyStrategy,
    NoStrategy,
)


class TestCurrencyStrategy(unittest.TestCase):

    def test_keeps_only_rows_matching_target_currency(self):
        batch = [
            TransactionRow(payment_currency="USD"),
            TransactionRow(payment_currency="EUR"),
            TransactionRow(payment_currency="USD"),
        ]
        result = CurrencyStrategy("USD").filter_batch(batch)
        self.assertEqual(len(result), 2)
        for row in result:
            self.assertEqual(row.payment_currency, "USD")

    def test_empty_batch_returns_empty(self):
        self.assertEqual(CurrencyStrategy("USD").filter_batch([]), [])

    def test_no_matches_returns_empty(self):
        batch = [
            TransactionRow(payment_currency="EUR"),
            TransactionRow(payment_currency="GBP"),
        ]
        self.assertEqual(CurrencyStrategy("USD").filter_batch(batch), [])

    def test_rows_with_none_currency_are_dropped(self):
        batch = [TransactionRow(payment_currency=None), TransactionRow(payment_currency="USD")]
        result = CurrencyStrategy("USD").filter_batch(batch)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].payment_currency, "USD")

    def test_target_currency_is_case_sensitive(self):
        batch = [TransactionRow(payment_currency="usd"), TransactionRow(payment_currency="USD")]
        result = CurrencyStrategy("USD").filter_batch(batch)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].payment_currency, "USD")


class TestAmountLessThanStrategy(unittest.TestCase):

    def test_keeps_only_rows_strictly_below_threshold(self):
        batch = [
            TransactionRow(amount_paid=49.99),
            TransactionRow(amount_paid=50.0),
            TransactionRow(amount_paid=50.01),
            TransactionRow(amount_paid=0.0),
        ]
        result = AmountLessThanStrategy(50.0).filter_batch(batch)
        self.assertEqual(len(result), 2)
        for row in result:
            self.assertLess(row.amount_paid, 50.0)

    def test_empty_batch_returns_empty(self):
        self.assertEqual(AmountLessThanStrategy(50.0).filter_batch([]), [])

    def test_rows_with_none_amount_are_dropped(self):
        batch = [TransactionRow(amount_paid=None), TransactionRow(amount_paid=10.0)]
        result = AmountLessThanStrategy(50.0).filter_batch(batch)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].amount_paid, 10.0)

    def test_negative_amounts_kept_when_below_threshold(self):
        batch = [TransactionRow(amount_paid=-5.0), TransactionRow(amount_paid=100.0)]
        result = AmountLessThanStrategy(50.0).filter_batch(batch)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].amount_paid, -5.0)


class TestNoStrategy(unittest.TestCase):

    def test_returns_batch_unchanged(self):
        batch = [
            TransactionRow(payment_currency="USD", amount_paid=10.0),
            TransactionRow(payment_currency="EUR", amount_paid=200.0),
        ]
        self.assertEqual(NoStrategy().filter_batch(batch), batch)


if __name__ == "__main__":
    unittest.main()
