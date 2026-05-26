import unittest
import uuid

from src.communication.protocols.queue_protocol.internal import (
    TransactionRow,
    build_eof_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)
from src.filter.main import _project_row, process_message
from src.filter.strategies import (
    AmountLessThanStrategy,
    CurrencyStrategy,
    NoStrategy,
)


def _make_transactions_msg(rows, client_id=None):
    msg = build_raw_transactions_message(
        client=client_id or str(uuid.uuid4()),
        msg_id=str(uuid.uuid4()),
        batch=rows,
    )
    return serialize(msg)


class TestProjectRow(unittest.TestCase):

    def test_keeps_only_specified_fields(self):
        tx = TransactionRow(
            timestamp="2022/09/02 06:00",
            from_bank=20,
            from_account="A",
            to_bank=30,
            to_account="B",
            amount_paid=100.0,
            payment_currency="USD",
        )
        projected = _project_row(tx, ["from_account", "to_account", "amount_paid"])
        self.assertEqual(projected.from_account, "A")
        self.assertEqual(projected.to_account, "B")
        self.assertEqual(projected.amount_paid, 100.0)

    def test_unspecified_fields_become_none(self):
        tx = TransactionRow(
            timestamp="2022/09/02 06:00",
            from_account="A",
            payment_currency="USD",
        )
        projected = _project_row(tx, ["from_account"])
        self.assertIsNone(projected.timestamp)
        self.assertIsNone(projected.payment_currency)
        self.assertIsNone(projected.amount_paid)

    def test_returns_transaction_row_instance(self):
        tx = TransactionRow(amount_paid=10.0)
        result = _project_row(tx, ["amount_paid"])
        self.assertIsInstance(result, TransactionRow)

    def test_empty_fields_list_returns_row_with_all_none(self):
        tx = TransactionRow(amount_paid=10.0, from_account="A")
        result = _project_row(tx, [])
        self.assertIsNone(result.amount_paid)
        self.assertIsNone(result.from_account)


class TestProcessMessage(unittest.TestCase):

    def test_filters_rows_by_strategy(self):
        rows = [
            TransactionRow(payment_currency="USD", amount_paid=10.0),
            TransactionRow(payment_currency="EUR", amount_paid=20.0),
            TransactionRow(payment_currency="USD", amount_paid=30.0),
        ]
        result = process_message(_make_transactions_msg(rows), CurrencyStrategy("USD"), None)
        self.assertIsNotNone(result)

        decoded = deserialize(result)
        batch = decoded["payload"]["batch"]
        self.assertEqual(len(batch), 2)
        for row in batch:
            self.assertEqual(row.payment_currency, "USD")

    def test_applies_projection_after_filter(self):
        rows = [
            TransactionRow(
                payment_currency="USD",
                amount_paid=10.0,
                from_account="A",
                to_account="B",
            )
        ]
        result = process_message(
            _make_transactions_msg(rows),
            NoStrategy(),
            ["from_account", "to_account", "amount_paid"],
        )
        decoded = deserialize(result)
        row = decoded["payload"]["batch"][0]
        self.assertEqual(row.from_account, "A")
        self.assertEqual(row.to_account, "B")
        self.assertEqual(row.amount_paid, 10.0)
        self.assertIsNone(row.payment_currency)

    def test_q1_pipeline_filter_then_project(self):
        rows = [
            TransactionRow(payment_currency="USD", amount_paid=10.0, from_account="A", to_account="B"),
            TransactionRow(payment_currency="USD", amount_paid=80.0, from_account="C", to_account="D"),
            TransactionRow(payment_currency="USD", amount_paid=5.0, from_account="E", to_account="F"),
        ]
        result = process_message(
            _make_transactions_msg(rows),
            AmountLessThanStrategy(50.0),
            ["from_account", "to_account", "amount_paid"],
        )
        decoded = deserialize(result)
        batch = decoded["payload"]["batch"]
        self.assertEqual(len(batch), 2)
        for row in batch:
            self.assertLess(row.amount_paid, 50.0)
            self.assertIsNotNone(row.from_account)
            self.assertIsNone(row.payment_currency)

    def test_empty_filtered_batch_returns_none(self):
        rows = [TransactionRow(payment_currency="EUR")]
        result = process_message(_make_transactions_msg(rows), CurrencyStrategy("USD"), None)
        self.assertIsNone(result)

    def test_non_transaction_message_returns_none(self):
        eof = build_eof_message(client=str(uuid.uuid4()), msg_id=str(uuid.uuid4()))
        result = process_message(serialize(eof), CurrencyStrategy("USD"), None)
        self.assertIsNone(result)

    def test_output_preserves_client_id(self):
        rows = [TransactionRow(payment_currency="USD", amount_paid=10.0)]
        result = process_message(
            _make_transactions_msg(rows, client_id="client-abc"),
            CurrencyStrategy("USD"),
            None,
        )
        decoded = deserialize(result)
        self.assertEqual(decoded["client"], "client-abc")

    def test_output_message_is_raw_transactions_type(self):
        rows = [TransactionRow(payment_currency="USD", amount_paid=10.0)]
        result = process_message(_make_transactions_msg(rows), CurrencyStrategy("USD"), None)
        decoded = deserialize(result)
        self.assertEqual(decoded["type"], "raw_transactions")


if __name__ == "__main__":
    unittest.main()
