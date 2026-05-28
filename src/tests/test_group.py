import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.communication.protocols.queue_protocol.internal import deserialize, serialize
from src.group.main import GroupConfig, GroupService
from src.group.strategies import BankMaxAmountStrategy


def _data_msg(client, batch):
    return serialize({
        "type": "batch",
        "client": client,
        "msg_id": str(uuid.uuid4()),
        "payload": {"batch_size": len(batch), "batch": batch},
    })


def _eof_msg(client):
    return serialize({"type": "eof", "client": client, "msg_id": str(uuid.uuid4())})


class _FakeCoord:
    def __init__(self):
        self.broadcasts: list[str] = []
        self.started = self.stopped = self.closed = False

    @contextmanager
    def lock(self):
        yield

    def broadcast(self, client_id): self.broadcasts.append(client_id)
    def start(self): self.started = True
    def stop(self, timeout=None): self.stopped = True
    def close(self): self.closed = True


def _make_service(strategy):
    config = GroupConfig(
        mom_host="ignored",
        input_queue="in_q",
        output_exchange="out_ex",
        log_level="INFO",
        eof_fanout="group_eof",
        expected_eofs=1,
        strategy=strategy,
    )

    with patch("src.group.main.middleware.MessageMiddlewareQueueRabbitMQ") as q_mw, \
         patch("src.group.main.middleware.MessageMiddlewareExchangeRabbitMQ") as ex_mw, \
         patch("src.group.main.EofCoordinator") as coord_cls:
        q_mw.return_value = MagicMock(name="input_queue")
        ex_mw.return_value = MagicMock(name="output_exchange")
        coord = _FakeCoord()
        coord_cls.return_value = coord
        service = GroupService(config)
    return service, coord


class GroupServiceTest(unittest.TestCase):

    def test_data_routes_grouped_batches(self):
        service, _ = _make_service(BankMaxAmountStrategy("agg_q2"))

        service._on_input(
            _data_msg("c1", [
                {"from_bank": "B1", "from_account": "a1", "amount_paid": 50.0},
                {"from_bank": "B1", "from_account": "a2", "amount_paid": 200.0},
                {"from_bank": "B2", "from_account": "a3", "amount_paid": 30.0},
            ]),
            MagicMock(),
            MagicMock(),
        )

        sends = service.output_exchange.send.call_args_list
        self.assertEqual(len(sends), 1)
        body, kwargs = sends[0].args[0], sends[0].kwargs
        self.assertEqual(kwargs["routing_key"], "agg_q2")
        decoded = deserialize(body)
        self.assertEqual(decoded["type"], "batch")
        # 2 bancos distintos en el batch
        self.assertEqual(decoded["payload"]["batch_size"], 2)

    def test_eof_does_not_emit_immediately(self):
        service, coord = _make_service(BankMaxAmountStrategy("agg_q2"))
        service._on_input(_eof_msg("c1"), MagicMock(), MagicMock())
        self.assertEqual(coord.broadcasts, ["c1"])
        service.output_exchange.send.assert_not_called()

    def test_flush_sends_eof_to_each_route_with_new_msg_id(self):
        service, _ = _make_service(BankMaxAmountStrategy("agg_q2"))
        service._flush_client("c1")
        sends = service.output_exchange.send.call_args_list
        self.assertEqual(len(sends), 1)
        body = sends[0].args[0]
        kwargs = sends[0].kwargs
        self.assertEqual(kwargs["routing_key"], "agg_q2")
        eof = deserialize(body)
        self.assertEqual(eof["type"], "eof")
        self.assertEqual(eof["client"], "c1")
        self.assertTrue(eof["msg_id"])  # uuid no vacío

    def test_empty_grouped_routes_are_skipped(self):
        service, _ = _make_service(BankMaxAmountStrategy("agg_q2"))
        service._on_input(
            _data_msg("c1", []),
            MagicMock(),
            MagicMock(),
        )
        service.output_exchange.send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
