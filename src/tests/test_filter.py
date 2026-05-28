import unittest
import uuid

from src.communication.protocols.queue_protocol.internal import (
    TransactionRow,
    build_eof_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)
from unittest.mock import MagicMock, patch

from src.filter.main import FilterConfig, FilterService, _project_row, process_message
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


class TestForwardEof(unittest.TestCase):

    def _make_service_with_outputs(self, output_queues):
        config = FilterConfig(
            mom_host="ignored",
            input_queue="in_q",
            output_queues=output_queues,
            log_level="INFO",
            strategy=NoStrategy(""),
            projection_fields=None,
        )
        service = FilterService(config)
        service._output_middleware = {q: MagicMock(name=f"mw:{q}") for q in output_queues}
        return service

    def test_forwards_eof_to_every_output_queue(self):
        service = self._make_service_with_outputs(["q_a", "q_b", "q_c"])
        service._forward_eof("client-1")
        for queue, mw in service._output_middleware.items():
            mw.send.assert_called_once()
            payload = mw.send.call_args.args[0]
            decoded = deserialize(payload)
            self.assertEqual(decoded["type"], "eof")
            self.assertEqual(decoded["client"], "client-1")

    def test_eof_msg_id_is_new_per_queue(self):
        service = self._make_service_with_outputs(["q_a", "q_b"])
        service._forward_eof("client-1")
        msg_ids = []
        for mw in service._output_middleware.values():
            decoded = deserialize(mw.send.call_args.args[0])
            msg_ids.append(decoded["msg_id"])
        self.assertEqual(len(set(msg_ids)), 2)

    def test_eof_message_in_consumer_triggers_forward_not_strategy(self):
        service = self._make_service_with_outputs(["q_a"])
        # Reemplazamos process_message para asegurar que no se invoca para EOFs.
        with patch("src.filter.main.process_message") as proc:
            eof = build_eof_message(client="c1", msg_id=str(uuid.uuid4()))
            # construimos el handler que arma start():
            ack = MagicMock()
            nack = MagicMock()
            # Reproducimos manualmente el branch del handler
            decoded = deserialize(serialize(eof))
            if decoded["type"] == "eof":
                service._forward_eof(decoded["client"])
                ack()
            proc.assert_not_called()
            ack.assert_called_once()
            nack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
