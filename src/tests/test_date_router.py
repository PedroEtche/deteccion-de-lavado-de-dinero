import os
import sys
import unittest
from datetime import date
from unittest.mock import MagicMock

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.common.communication.internal import (
    TransactionRow,
    build_raw_transactions_message,
    deserialize,
)
from src.date_router.main import (
    DateRoute,
    DateRouterConfig,
    DateRouterWorker,
    _default_routes,
    init_config,
)


def _row(timestamp, **kwargs):
    return TransactionRow(timestamp=timestamp, **kwargs)


def _make_config(routes):
    return DateRouterConfig(
        mom_host="ignored",
        input_exchange="in_q",
        log_level="INFO",
        expected_eofs=1,
        worker_id=1,
        routes=routes,
    )


def _make_worker(routes, num_downstream_per_route=1):
    for r in routes:
        r.num_downstream_workers = num_downstream_per_route
    worker = DateRouterWorker(_make_config(routes))
    for route in worker.routes:
        route._exchange = MagicMock(name=f"mw:{route.name}")
    return worker


def _route(name, output, from_date_str, to_date_str):
    y1, m1, d1 = (int(x) for x in from_date_str.split("/"))
    y2, m2, d2 = (int(x) for x in to_date_str.split("/"))
    return DateRoute(
        name=name,
        output_exchange=output,
        from_date=date(y1, m1, d1),
        to_date=date(y2, m2, d2),
    )


class TestDefaultRoutes(unittest.TestCase):
    def test_returns_two_routes_matching_the_spec(self):
        routes = _default_routes(num_downstream_workers=1)
        self.assertEqual(len(routes), 2)

        early, late = routes
        self.assertEqual(early.from_date, date(2022, 9, 1))
        self.assertEqual(early.to_date, date(2022, 9, 5))
        self.assertEqual(late.from_date, date(2022, 9, 6))
        self.assertEqual(late.to_date, date(2022, 9, 15))

    def test_applies_num_downstream_workers_to_all_routes(self):
        routes = _default_routes(num_downstream_workers=4)
        for r in routes:
            self.assertEqual(r.num_downstream_workers, 4)


class TestInitConfig(unittest.TestCase):
    def test_uses_env_defaults(self):
        prev = {k: os.environ.get(k) for k in (
            "MOM_HOST", "INPUT_EXCHANGE", "WORKER_ID", "EOF_EXPECTED",
            "NUM_DOWNSTREAM_WORKERS", "LOG_LEVEL",
        )}
        try:
            os.environ["MOM_HOST"] = "rabbit-test"
            os.environ["INPUT_EXCHANGE"] = "in_x"
            os.environ["WORKER_ID"] = "2"
            os.environ["EOF_EXPECTED"] = "3"
            os.environ["NUM_DOWNSTREAM_WORKERS"] = "5"
            os.environ["LOG_LEVEL"] = "DEBUG"

            cfg = init_config()
            self.assertEqual(cfg.mom_host, "rabbit-test")
            self.assertEqual(cfg.input_exchange, "in_x")
            self.assertEqual(cfg.worker_id, 2)
            self.assertEqual(cfg.expected_eofs, 3)
            self.assertEqual(len(cfg.routes), 2)
            for r in cfg.routes:
                self.assertEqual(r.num_downstream_workers, 5)
        finally:
            for k, v in prev.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


class TestDateRouteMatches(unittest.TestCase):
    def test_inclusive_lower_bound(self):
        route = _route("r", "ex", "2022/09/01", "2022/09/05")
        self.assertTrue(route.matches(date(2022, 9, 1)))

    def test_inclusive_upper_bound(self):
        route = _route("r", "ex", "2022/09/01", "2022/09/05")
        self.assertTrue(route.matches(date(2022, 9, 5)))

    def test_outside_range(self):
        route = _route("r", "ex", "2022/09/01", "2022/09/05")
        self.assertFalse(route.matches(date(2022, 8, 31)))
        self.assertFalse(route.matches(date(2022, 9, 6)))


class TestProcessData(unittest.TestCase):
    def setUp(self):
        self.routes = [
            _route("early", "ex_early", "2022/09/01", "2022/09/05"),
            _route("late", "ex_late", "2022/09/06", "2022/09/15"),
        ]
        self.worker = _make_worker(self.routes)

    def _exchange_for(self, name):
        return next(r._exchange for r in self.worker.routes if r.name == name)

    def _last_sent(self, name):
        ex = self._exchange_for(name)
        if not ex.send.call_args_list:
            return None
        return deserialize(ex.send.call_args.args[0])

    def test_row_in_first_range_only_goes_to_first(self):
        msg = build_raw_transactions_message(
            client="c1", msg_id="m1", batch=[_row("2022/09/02 06:00", from_account="A")]
        )
        self.worker.process_data("c1", msg["payload"])
        self._exchange_for("early").send.assert_called_once()
        self._exchange_for("late").send.assert_not_called()

    def test_row_in_second_range_only_goes_to_second(self):
        msg = build_raw_transactions_message(
            client="c1", msg_id="m1", batch=[_row("2022/09/10 06:00", from_account="A")]
        )
        self.worker.process_data("c1", msg["payload"])
        self._exchange_for("early").send.assert_not_called()
        self._exchange_for("late").send.assert_called_once()

    def test_row_in_overlap_goes_to_both_routes(self):
        overlap_routes = [
            _route("a", "ex_a", "2022/09/01", "2022/09/15"),
            _route("b", "ex_b", "2022/09/05", "2022/09/10"),
        ]
        worker = _make_worker(overlap_routes)
        msg = build_raw_transactions_message(
            client="c1", msg_id="m1", batch=[_row("2022/09/07 06:00", from_account="A")]
        )
        worker.process_data("c1", msg["payload"])
        for r in worker.routes:
            r._exchange.send.assert_called_once()
            decoded = deserialize(r._exchange.send.call_args.args[0])
            self.assertEqual(len(decoded["payload"]["batch"]), 1)

    def test_row_outside_all_ranges_is_dropped(self):
        msg = build_raw_transactions_message(
            client="c1", msg_id="m1", batch=[_row("2021/01/01 00:00", from_account="A")]
        )
        self.worker.process_data("c1", msg["payload"])
        self._exchange_for("early").send.assert_not_called()
        self._exchange_for("late").send.assert_not_called()

    def test_row_with_unparseable_timestamp_is_dropped(self):
        msg = build_raw_transactions_message(
            client="c1", msg_id="m1", batch=[_row("not a date")]
        )
        self.worker.process_data("c1", msg["payload"])
        self._exchange_for("early").send.assert_not_called()
        self._exchange_for("late").send.assert_not_called()

    def test_row_with_null_timestamp_is_dropped(self):
        msg = build_raw_transactions_message(
            client="c1", msg_id="m1", batch=[_row(None)]
        )
        self.worker.process_data("c1", msg["payload"])
        self._exchange_for("early").send.assert_not_called()
        self._exchange_for("late").send.assert_not_called()

    def test_empty_batch_sends_nothing(self):
        self.worker.process_data("c1", {"batch": []})
        self._exchange_for("early").send.assert_not_called()
        self._exchange_for("late").send.assert_not_called()

    def test_mixed_batch_splits_per_route(self):
        batch = [
            _row("2022/09/02 06:00", from_account="A"),  # early only
            _row("2022/09/10 06:00", from_account="B"),  # late only
            _row("2022/09/12 06:00", from_account="C"),  # late only
            _row("2099/01/01 00:00", from_account="X"),  # dropped
            _row("nope", from_account="Y"),  # dropped
        ]
        msg = build_raw_transactions_message(client="c1", msg_id="m1", batch=batch)
        self.worker.process_data("c1", msg["payload"])

        early = self._last_sent("early")
        late = self._last_sent("late")
        self.assertEqual(len(early["payload"]["batch"]), 1)
        self.assertEqual(len(late["payload"]["batch"]), 2)

    def test_output_preserves_client_id(self):
        msg = build_raw_transactions_message(
            client="client-abc",
            msg_id="m1",
            batch=[_row("2022/09/02 06:00", from_account="A")],
        )
        self.worker.process_data("client-abc", msg["payload"])
        decoded = self._last_sent("early")
        self.assertEqual(decoded["client"], "client-abc")
        self.assertEqual(decoded["type"], "raw_transactions")

    def test_output_msg_id_is_new(self):
        msg = build_raw_transactions_message(
            client="c1", msg_id="orig", batch=[_row("2022/09/02 06:00", from_account="A")]
        )
        self.worker.process_data("c1", msg["payload"])
        decoded = self._last_sent("early")
        self.assertNotEqual(decoded["msg_id"], "orig")


class TestRoundRobin(unittest.TestCase):
    def test_alternates_workers(self):
        routes = [_route("r", "ex", "2022/09/01", "2022/09/15")]
        worker = _make_worker(routes, num_downstream_per_route=3)
        for i in range(7):
            msg = build_raw_transactions_message(
                client="c1",
                msg_id=f"m{i}",
                batch=[_row("2022/09/02 06:00", from_account=f"A{i}")],
            )
            worker.process_data("c1", msg["payload"])

        ex = worker.routes[0]._exchange
        keys = [call.kwargs["routing_key"] for call in ex.send.call_args_list]
        self.assertEqual(
            keys,
            ["worker_1", "worker_2", "worker_3", "worker_1", "worker_2", "worker_3", "worker_1"],
        )

    def test_per_route_counters_are_independent(self):
        routes = [
            _route("a", "ex_a", "2022/09/01", "2022/09/15"),
            _route("b", "ex_b", "2022/09/01", "2022/09/15"),
        ]
        worker = _make_worker(routes, num_downstream_per_route=2)
        msg = build_raw_transactions_message(
            client="c1", msg_id="m1", batch=[_row("2022/09/05 06:00", from_account="A")]
        )
        worker.process_data("c1", msg["payload"])

        for r in worker.routes:
            self.assertEqual(r._exchange.send.call_args.kwargs["routing_key"], "worker_1")


class TestEofBroadcast(unittest.TestCase):
    def test_on_flush_broadcasts_eof_to_all_routes(self):
        routes = [
            _route("a", "ex_a", "2022/09/01", "2022/09/05"),
            _route("b", "ex_b", "2022/09/06", "2022/09/10"),
            _route("c", "ex_c", "2022/09/11", "2022/09/15"),
        ]
        worker = _make_worker(routes)
        worker._on_flush("client-1")
        for r in worker.routes:
            r._exchange.send.assert_called_once()
            decoded = deserialize(r._exchange.send.call_args.args[0])
            self.assertEqual(decoded["type"], "eof")
            self.assertEqual(decoded["client"], "client-1")
            self.assertEqual(r._exchange.send.call_args.kwargs["routing_key"], "eof_broadcast")

    def test_eof_msg_id_is_new_per_route(self):
        routes = [
            _route("a", "ex_a", "2022/09/01", "2022/09/05"),
            _route("b", "ex_b", "2022/09/06", "2022/09/10"),
        ]
        worker = _make_worker(routes)
        worker._on_flush("client-1")
        ids = [
            deserialize(r._exchange.send.call_args.args[0])["msg_id"]
            for r in worker.routes
        ]
        self.assertEqual(len(set(ids)), 2)

    def test_eof_broadcast_runs_even_with_zero_data_for_client(self):
        # No process_data was ever called for this client, but EOF must still propagate.
        routes = [_route("a", "ex_a", "2022/09/01", "2022/09/05")]
        worker = _make_worker(routes)
        worker._on_flush("never-saw-data")
        worker.routes[0]._exchange.send.assert_called_once()


if __name__ == "__main__":
    unittest.main()
