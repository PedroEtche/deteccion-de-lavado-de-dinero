import unittest
import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.communication.protocols.queue_protocol.internal import (
    AccountRow,
    deserialize,
    serialize,
)
from src.join.main import JoinConfig, JoinService
from src.join.strategies import (
    BankMaxAmountStrategy,
    CountStrategy,
    NoStrategy,
)


def _data_msg(client, batch):
    return serialize({
        "type": "batch",
        "client": client,
        "msg_id": str(uuid.uuid4()),
        "payload": {"batch_size": len(batch), "batch": batch},
    })


def _eof_msg(client):
    return serialize({"type": "eof", "client": client, "msg_id": str(uuid.uuid4())})


def _accounts_msg(client, batch):
    return serialize({
        "type": "raw_accounts",
        "client": client,
        "msg_id": str(uuid.uuid4()),
        "payload": {"batch_size": len(batch), "batch": batch},
    })


def _make_account(bank_id, bank_name):
    return AccountRow(bank_id=bank_id, bank_name=bank_name)


def _max_tx(from_bank, from_account, amount_paid):
    return {
        "from_bank": from_bank,
        "from_account": from_account,
        "amount_paid": amount_paid,
    }


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


def _make_service(strategy, accounts_input_queue=None):
    config = JoinConfig(
        mom_host="ignored",
        input_queue="in_q",
        output_queue="out_q",
        log_level="INFO",
        eof_fanout="join_eof",
        expected_eofs=1,
        strategy=strategy,
        accounts_input_queue=accounts_input_queue,
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

    def test_accounts_message_feeds_strategy(self):
        service, _ = _make_service(BankMaxAmountStrategy(), accounts_input_queue="accounts_q")
        service._on_accounts(
            _accounts_msg("c1", [_make_account("1", "Bank One")]),
            MagicMock(),
            MagicMock(),
        )
        self.assertEqual(service.strategy.bank_names_by_client["c1"]["1"], "Bank One")

    def test_accounts_eof_does_not_flush(self):
        service, coord = _make_service(BankMaxAmountStrategy(), accounts_input_queue="accounts_q")
        service._on_accounts(_eof_msg("c1"), MagicMock(), MagicMock())
        self.assertEqual(coord.broadcasts, [])
        service.output_queue.send.assert_not_called()


class TestBankMaxAmountStrategyForQ2(unittest.TestCase):

    def test_join_keeps_max_per_bank(self):
        strategy = BankMaxAmountStrategy()
        strategy.join_batch([
            _max_tx("1", "A", 10.0),
            _max_tx("1", "B", 30.0),
            _max_tx("2", "C", 5.0),
        ], client="c1")

        results = strategy.get_joined_for_client("c1")
        by_bank = {r["bank_name"]: r for r in results}
        self.assertEqual(by_bank["1"]["amount_paid"], 30.0)
        self.assertEqual(by_bank["1"]["from_account"], "B")
        self.assertEqual(by_bank["2"]["amount_paid"], 5.0)
        self.assertEqual(by_bank["2"]["from_account"], "C")

    def test_join_enriches_with_bank_name_from_accounts(self):
        strategy = BankMaxAmountStrategy()
        strategy.add_accounts([
            _make_account("1", "Bank One"),
            _make_account("2", "Bank Two"),
        ], client="c1")
        strategy.join_batch([
            _max_tx("1", "A", 30.0),
            _max_tx("2", "C", 5.0),
        ], client="c1")

        results = strategy.get_joined_for_client("c1")
        by_bank = {r["bank_name"]: r for r in results}
        self.assertIn("Bank One", by_bank)
        self.assertIn("Bank Two", by_bank)
        self.assertEqual(by_bank["Bank One"]["amount_paid"], 30.0)
        self.assertEqual(by_bank["Bank One"]["from_account"], "A")
        self.assertEqual(by_bank["Bank Two"]["amount_paid"], 5.0)

    def test_accounts_can_be_added_before_or_after_join(self):
        s1 = BankMaxAmountStrategy()
        s1.add_accounts([_make_account("1", "Bank One")], client="c1")
        s1.join_batch([_max_tx("1", "A", 10.0)], client="c1")

        s2 = BankMaxAmountStrategy()
        s2.join_batch([_max_tx("1", "A", 10.0)], client="c1")
        s2.add_accounts([_make_account("1", "Bank One")], client="c1")

        self.assertEqual(s1.get_joined_for_client("c1"), s2.get_joined_for_client("c1"))

    def test_missing_bank_in_accounts_falls_back_to_bank_id(self):
        strategy = BankMaxAmountStrategy()
        strategy.add_accounts([_make_account("1", "Bank One")], client="c1")
        strategy.join_batch([
            _max_tx("1", "A", 10.0),
            _max_tx("unknown", "X", 99.0),
        ], client="c1")

        results = strategy.get_joined_for_client("c1")
        bank_names = {r["bank_name"] for r in results}
        self.assertIn("Bank One", bank_names)
        self.assertIn("unknown", bank_names)

    def test_state_is_isolated_per_client(self):
        strategy = BankMaxAmountStrategy()
        strategy.add_accounts([_make_account("1", "Bank One")], client="c1")
        strategy.add_accounts([_make_account("1", "Other Bank")], client="c2")
        strategy.join_batch([_max_tx("1", "A", 10.0)], client="c1")
        strategy.join_batch([_max_tx("1", "B", 20.0)], client="c2")

        results_c1 = strategy.get_joined_for_client("c1")
        results_c2 = strategy.get_joined_for_client("c2")

        self.assertEqual(len(results_c1), 1)
        self.assertEqual(results_c1[0]["bank_name"], "Bank One")
        self.assertEqual(results_c1[0]["from_account"], "A")

        self.assertEqual(len(results_c2), 1)
        self.assertEqual(results_c2[0]["bank_name"], "Other Bank")
        self.assertEqual(results_c2[0]["from_account"], "B")

    def test_get_joined_clears_client_state(self):
        strategy = BankMaxAmountStrategy()
        strategy.add_accounts([_make_account("1", "Bank One")], client="c1")
        strategy.join_batch([_max_tx("1", "A", 10.0)], client="c1")

        first = strategy.get_joined_for_client("c1")
        second = strategy.get_joined_for_client("c1")
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])

    def test_account_row_with_none_fields_is_ignored(self):
        strategy = BankMaxAmountStrategy()
        strategy.add_accounts([
            AccountRow(bank_id=None, bank_name="Nameless"),
            AccountRow(bank_id="2", bank_name=None),
            AccountRow(bank_id="3", bank_name="Bank Three"),
        ], client="c1")
        strategy.join_batch([
            _max_tx("2", "X", 5.0),
            _max_tx("3", "Y", 7.0),
        ], client="c1")

        results = strategy.get_joined_for_client("c1")
        bank_names = {r["bank_name"] for r in results}
        self.assertEqual(bank_names, {"2", "Bank Three"})


class TestNoStrategyAndCountStrategyAddAccountsAreNoOp(unittest.TestCase):

    def test_no_strategy_add_accounts_is_noop(self):
        NoStrategy().add_accounts([_make_account("1", "Bank One")], client="c1")

    def test_count_strategy_add_accounts_is_noop(self):
        CountStrategy().add_accounts([_make_account("1", "Bank One")], client="c1")


if __name__ == "__main__":
    unittest.main()
