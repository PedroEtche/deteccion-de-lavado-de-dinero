import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.communication.protocols.queue_protocol.internal import deserialize, serialize
from src.join.main import JoinConfig, JoinService
from src.join.strategies import CountStrategy


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

    def broadcast(self, c): self.broadcasts.append(c)
    def start(self): self.started = True
    def stop(self, timeout=None): self.stopped = True
    def close(self): self.closed = True


def _make_service(strategy):
    config = JoinConfig(
        mom_host="ignored",
        input_queue="in_q",
        output_queue="out_q",
        log_level="INFO",
        eof_fanout="join_eof",
        expected_eofs=1,
        strategy=strategy,
    )
    with patch("src.join.main.middleware.MessageMiddlewareQueueRabbitMQ") as mw, \
         patch("src.join.main.EofCoordinator") as coord_cls:
        mw.side_effect = lambda host, name: MagicMock(name=f"q:{name}")
        coord = _FakeCoord()
        coord_cls.return_value = coord
        service = JoinService(config)
    return service, coord


class JoinServiceTest(unittest.TestCase):

    def test_data_message_joined_under_lock(self):
        service, coord = _make_service(CountStrategy())
        service._on_input(
            _data_msg("c1", [{"k": "v"}, {"k": "v"}, {"k": "v"}]),
            MagicMock(),
            MagicMock(),
        )
        self.assertEqual(coord.broadcasts, [])
        self.assertEqual(service.strategy.count_by_client["c1"], 3)

    def test_eof_only_broadcasts(self):
        service, coord = _make_service(CountStrategy())
        service._on_input(_eof_msg("c1"), MagicMock(), MagicMock())
        self.assertEqual(coord.broadcasts, ["c1"])
        service.output_queue.send.assert_not_called()

    def test_flush_emits_joined_batch_then_eof_new_msg_ids(self):
        service, _ = _make_service(CountStrategy())
        service.strategy.count_by_client["c1"] = 7

        service._flush_client("c1")

        sent = [c.args[0] for c in service.output_queue.send.call_args_list]
        self.assertEqual(len(sent), 2)
        batch = deserialize(sent[0])
        eof = deserialize(sent[1])
        self.assertEqual(batch["type"], "joined_data")
        self.assertEqual(batch["client"], "c1")
        self.assertEqual(batch["payload"]["batch"], [7])
        self.assertEqual(eof["type"], "eof")
        self.assertNotEqual(batch["msg_id"], eof["msg_id"])

    def test_stop_chains_to_coord(self):
        service, coord = _make_service(CountStrategy())
        service.stop()
        service.input_queue.stop_consuming.assert_called_once()
        self.assertTrue(coord.stopped)


if __name__ == "__main__":
    unittest.main()
