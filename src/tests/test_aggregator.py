import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.aggregator.main import AggregatorConfig, AggregatorService
from src.aggregator.strategies import BankMaxAmountStrategy
from src.communication.protocols.queue_protocol.internal import deserialize, serialize


def _make_data_msg(client, batch):
    return serialize({
        "type": "batch",
        "client": client,
        "msg_id": str(uuid.uuid4()),
        "payload": {"batch_size": len(batch), "batch": batch},
    })


def _make_eof_msg(client):
    return serialize({"type": "eof", "client": client, "msg_id": str(uuid.uuid4())})


class _FakeCoord:
    def __init__(self):
        self.broadcasts: list[str] = []
        self.flushes: list[str] = []
        self.started = False
        self.stopped = False
        self.closed = False

    @contextmanager
    def lock(self):
        yield

    def broadcast(self, client_id):
        self.broadcasts.append(client_id)

    def start(self):
        self.started = True

    def stop(self, timeout=None):
        self.stopped = True

    def close(self):
        self.closed = True


def _make_service(strategy):
    config = AggregatorConfig(
        mom_host="ignored",
        input_queue="in_q",
        output_queue="out_q",
        log_level="INFO",
        eof_fanout="agg_eof",
        expected_eofs=1,
        strategy=strategy,
    )

    with patch("src.aggregator.main.middleware.MessageMiddlewareQueueRabbitMQ") as mw, \
         patch("src.aggregator.main.EofCoordinator") as coord_cls:
        mw.side_effect = lambda host, name: MagicMock(name=f"queue:{name}")
        fake_coord = _FakeCoord()
        coord_cls.return_value = fake_coord
        service = AggregatorService(config)

    return service, fake_coord


class AggregatorServiceTest(unittest.TestCase):

    def test_data_message_aggregates_under_lock(self):
        service, coord = _make_service(BankMaxAmountStrategy())
        ack = MagicMock()

        service._on_input(
            _make_data_msg("c1", [
                {"from_bank": "BankA", "from_account": "acc1", "amount_paid": 100.0},
                {"from_bank": "BankA", "from_account": "acc2", "amount_paid": 200.0},
            ]),
            ack,
            MagicMock(),
        )

        ack.assert_called_once()
        self.assertEqual(coord.broadcasts, [])
        # estado se acumuló
        self.assertIn("c1", service.strategy.max_per_bank_by_client)

    def test_eof_message_only_broadcasts(self):
        service, coord = _make_service(BankMaxAmountStrategy())
        ack = MagicMock()

        service._on_input(_make_eof_msg("c1"), ack, MagicMock())

        ack.assert_called_once()
        self.assertEqual(coord.broadcasts, ["c1"])
        # nada se envió downstream todavía
        service.output_queue.send.assert_not_called()

    def test_flush_sends_batch_then_eof_with_new_msg_ids(self):
        service, _ = _make_service(BankMaxAmountStrategy())
        # cargar estado
        service.strategy.aggregate_batch(
            [{"from_bank": "BankA", "from_account": "acc1", "amount_paid": 100.0}],
            client="c1",
        )

        service._flush_client("c1")

        sent = [call.args[0] for call in service.output_queue.send.call_args_list]
        self.assertEqual(len(sent), 2)

        batch_msg = deserialize(sent[0])
        eof_msg = deserialize(sent[1])

        self.assertEqual(batch_msg["type"], "batch")
        self.assertEqual(batch_msg["client"], "c1")
        self.assertEqual(eof_msg["type"], "eof")
        self.assertEqual(eof_msg["client"], "c1")
        # msg_ids son nuevos y distintos entre sí
        self.assertNotEqual(batch_msg["msg_id"], eof_msg["msg_id"])

    def test_flush_clears_strategy_state(self):
        service, _ = _make_service(BankMaxAmountStrategy())
        service.strategy.aggregate_batch(
            [{"from_bank": "BankA", "from_account": "acc1", "amount_paid": 100.0}],
            client="c1",
        )

        service._flush_client("c1")
        self.assertNotIn("c1", service.strategy.max_per_bank_by_client)

    def test_stop_stops_input_and_coord(self):
        service, coord = _make_service(BankMaxAmountStrategy())
        service.stop()
        service.input_queue.stop_consuming.assert_called_once()
        self.assertTrue(coord.stopped)


if __name__ == "__main__":
    unittest.main()
