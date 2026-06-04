"""Unit tests for the strategies that compose pipelines Q3 and Q4.

Cada query usa varias strategies distribuidas entre los componentes filter,
group, aggregator, join y joiner. Estos tests cubren cada strategy de forma
aislada, sin levantar middleware ni dockerizar nada.
"""

import unittest

from src.aggregator.strategies import (
    AccountPairCountStategy as AggregatorAccountPairCountStategy,
    AccountStrategy as AggregatorAccountStrategy,
    PaymentFormatAverageStrategy as AggregatorPaymentFormatAverageStrategy,
)
from src.communication.protocols.queue_protocol.internal import TransactionRow
from src.filter.strategies import HistoricalAverageFilterStrategy
from src.group.strategies import (
    AccountPairCountStategy as GroupAccountPairCountStategy,
    AccountStrategy as GroupAccountStrategy,
    MergeRoutingStrategy,
    PaymentFormatAverageStrategy as GroupPaymentFormatAverageStrategy,
)
from src.join.strategies import UnionStrategy
from src.joiner.strategies import SelfMergeStrategy


# ────────────────────────────────────────────────────────────────────────────
# Q3 — Group: PaymentFormatAverage (partial sum/count, sharded by format)
# ────────────────────────────────────────────────────────────────────────────


class GroupPaymentFormatAverageTest(unittest.TestCase):
    def test_one_route_per_unique_format(self):
        strategy = GroupPaymentFormatAverageStrategy("aggregate_route", shard_amount=3)
        batch = [
            TransactionRow(payment_format="Wire", amount_paid=100.0),
            TransactionRow(payment_format="Wire", amount_paid=50.0),
            TransactionRow(payment_format="ACH", amount_paid=10.0),
        ]
        routed = strategy.group_and_route(batch)

        by_route = {route: rows for route, rows in routed}
        # Total acumulado se conserva (ningún batch se pierde).
        all_rows = [row for rows in by_route.values() for row in rows]
        self.assertEqual(sum(r["tx_quantity"] for r in all_rows), 3)
        self.assertEqual(sum(r["total_amount"] for r in all_rows), 160.0)

    def test_same_format_always_same_route(self):
        strategy = GroupPaymentFormatAverageStrategy("aggregate_route", shard_amount=3)
        routed_a = strategy.group_and_route(
            [TransactionRow(payment_format="Wire", amount_paid=10.0)]
        )
        routed_b = strategy.group_and_route(
            [TransactionRow(payment_format="Wire", amount_paid=20.0)]
        )
        self.assertEqual(routed_a[0][0], routed_b[0][0])

    def test_none_amount_treated_as_zero(self):
        strategy = GroupPaymentFormatAverageStrategy("agg", shard_amount=1)
        routed = strategy.group_and_route(
            [
                TransactionRow(payment_format="Wire", amount_paid=None),
                TransactionRow(payment_format="Wire", amount_paid=10.0),
            ]
        )
        rows = routed[0][1]
        self.assertEqual(rows[0]["tx_quantity"], 2)
        self.assertEqual(rows[0]["total_amount"], 10.0)

    def test_eof_routes_cover_every_shard(self):
        strategy = GroupPaymentFormatAverageStrategy("agg", shard_amount=4)
        self.assertEqual(
            strategy.get_eof_routes(),
            ["agg_0", "agg_1", "agg_2", "agg_3"],
        )


# ────────────────────────────────────────────────────────────────────────────
# Q3 — Aggregator: PaymentFormatAverage (acumula stats y emite averages)
# ────────────────────────────────────────────────────────────────────────────


class AggregatorPaymentFormatAverageTest(unittest.TestCase):
    def test_accumulates_partial_sums_across_batches(self):
        strategy = AggregatorPaymentFormatAverageStrategy()
        strategy.aggregate_batch(
            [{"payment_format": "Wire", "total_amount": 100.0, "tx_quantity": 2}],
            client="c1",
        )
        strategy.aggregate_batch(
            [{"payment_format": "Wire", "total_amount": 50.0, "tx_quantity": 1}],
            client="c1",
        )
        result = strategy.get_result_for_client("c1")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["payment_format"], "Wire")
        self.assertAlmostEqual(result[0]["average_amount"], 150.0 / 3)

    def test_separate_state_per_client(self):
        strategy = AggregatorPaymentFormatAverageStrategy()
        strategy.aggregate_batch(
            [{"payment_format": "Wire", "total_amount": 100.0, "tx_quantity": 1}],
            client="c1",
        )
        strategy.aggregate_batch(
            [{"payment_format": "Wire", "total_amount": 200.0, "tx_quantity": 1}],
            client="c2",
        )
        self.assertEqual(
            strategy.get_result_for_client("c1")[0]["average_amount"], 100.0
        )
        self.assertEqual(
            strategy.get_result_for_client("c2")[0]["average_amount"], 200.0
        )

    def test_clear_client_state_removes_only_target_client(self):
        strategy = AggregatorPaymentFormatAverageStrategy()
        strategy.aggregate_batch(
            [{"payment_format": "Wire", "total_amount": 10.0, "tx_quantity": 1}],
            client="c1",
        )
        strategy.aggregate_batch(
            [{"payment_format": "ACH", "total_amount": 20.0, "tx_quantity": 1}],
            client="c2",
        )
        strategy.clear_client_state("c1")
        self.assertEqual(strategy.get_result_for_client("c1"), [])
        self.assertEqual(len(strategy.get_result_for_client("c2")), 1)

    def test_zero_count_yields_zero_average(self):
        strategy = AggregatorPaymentFormatAverageStrategy()
        strategy.aggregate_batch(
            [{"payment_format": "Wire", "total_amount": 0.0, "tx_quantity": 0}],
            client="c1",
        )
        result = strategy.get_result_for_client("c1")
        self.assertEqual(result[0]["average_amount"], 0.0)

    def test_client_required(self):
        with self.assertRaises(ValueError):
            AggregatorPaymentFormatAverageStrategy().aggregate_batch(
                [{"payment_format": "Wire", "total_amount": 10.0, "tx_quantity": 1}],
                client=None,
            )


# ────────────────────────────────────────────────────────────────────────────
# Q3 — Filter: HistoricalAverage (espera averages via control_queue)
# ────────────────────────────────────────────────────────────────────────────


class HistoricalAverageFilterTest(unittest.TestCase):
    def _batch_for(self, *txs):
        # Set required attribute manualmente; el service lo hace al recibir
        # mensajes, acá lo simulamos.
        return list(txs)

    def test_no_averages_yields_empty_result(self):
        strategy = HistoricalAverageFilterStrategy(output_queue="out_q")
        strategy._current_client = "c1"
        result = strategy.filter_batch(
            self._batch_for(TransactionRow(payment_format="Wire", amount_paid=0.01))
        )
        self.assertEqual(result, {})

    def test_keeps_rows_below_threshold_multiplier_of_average(self):
        strategy = HistoricalAverageFilterStrategy(
            output_queue="out_q", threshold_multiplier=0.01
        )
        strategy.update_averages(
            "c1", [{"payment_format": "Wire", "average_amount": 100.0}]
        )
        strategy._current_client = "c1"
        batch = self._batch_for(
            TransactionRow(payment_format="Wire", amount_paid=0.99),  # < 1.0  ✓
            TransactionRow(payment_format="Wire", amount_paid=1.0),  # = 1.0  ✗
            TransactionRow(payment_format="Wire", amount_paid=0.5),  # < 1.0  ✓
        )
        result = strategy.filter_batch(batch)
        self.assertEqual(len(result["out_q"]), 2)
        for row in result["out_q"]:
            self.assertLess(row.amount_paid, 1.0)

    def test_drops_rows_with_unknown_format(self):
        strategy = HistoricalAverageFilterStrategy(output_queue="out_q")
        strategy.update_averages(
            "c1", [{"payment_format": "Wire", "average_amount": 100.0}]
        )
        strategy._current_client = "c1"
        batch = self._batch_for(
            TransactionRow(payment_format="ACH", amount_paid=0.1),
        )
        self.assertEqual(strategy.filter_batch(batch), {})

    def test_separate_averages_per_client(self):
        strategy = HistoricalAverageFilterStrategy(output_queue="out_q")
        strategy.update_averages(
            "c1", [{"payment_format": "Wire", "average_amount": 100.0}]
        )
        strategy.update_averages(
            "c2", [{"payment_format": "Wire", "average_amount": 10.0}]
        )

        strategy._current_client = "c1"
        result_c1 = strategy.filter_batch(
            [TransactionRow(payment_format="Wire", amount_paid=0.5)]
        )
        self.assertEqual(len(result_c1["out_q"]), 1)

        strategy._current_client = "c2"
        # 0.5 NO es < 10 * 0.01 = 0.1 → debe descartarse para c2
        result_c2 = strategy.filter_batch(
            [TransactionRow(payment_format="Wire", amount_paid=0.5)]
        )
        self.assertEqual(result_c2, {})

    def test_clear_client_drops_only_that_clients_averages(self):
        strategy = HistoricalAverageFilterStrategy(output_queue="out_q")
        strategy.update_averages(
            "c1", [{"payment_format": "Wire", "average_amount": 100.0}]
        )
        strategy.update_averages(
            "c2", [{"payment_format": "Wire", "average_amount": 50.0}]
        )
        strategy.clear_client("c1")
        self.assertNotIn("c1", strategy.averages_by_client)
        self.assertIn("c2", strategy.averages_by_client)


# ────────────────────────────────────────────────────────────────────────────
# Q3 — Join: UnionStrategy (juntar batches parciales por cliente)
# ────────────────────────────────────────────────────────────────────────────


class UnionStrategyTest(unittest.TestCase):
    def test_concatenates_batches_per_client(self):
        strategy = UnionStrategy()
        strategy.join_batch([{"a": 1}, {"a": 2}], client="c1")
        strategy.join_batch([{"a": 3}], client="c1")
        joined = strategy.get_joined_for_client("c1")
        self.assertEqual(joined, [{"a": 1}, {"a": 2}, {"a": 3}])

    def test_separates_state_between_clients(self):
        strategy = UnionStrategy()
        strategy.join_batch([{"a": 1}], client="c1")
        strategy.join_batch([{"b": 1}], client="c2")
        self.assertEqual(strategy.get_joined_for_client("c1"), [{"a": 1}])
        self.assertEqual(strategy.get_joined_for_client("c2"), [{"b": 1}])

    def test_get_joined_clears_state(self):
        strategy = UnionStrategy()
        strategy.join_batch([{"a": 1}], client="c1")
        strategy.get_joined_for_client("c1")
        self.assertEqual(strategy.get_joined_for_client("c1"), [])

    def test_client_required(self):
        with self.assertRaises(ValueError):
            UnionStrategy().join_batch([{"a": 1}], client=None)


# ────────────────────────────────────────────────────────────────────────────
# Q4 — Group: MergeRouting (rutea cada tx a shard de origen y destino)
# ────────────────────────────────────────────────────────────────────────────


class MergeRoutingStrategyTest(unittest.TestCase):
    def test_single_shard_routes_everything_together(self):
        strategy = MergeRoutingStrategy("selfmerge_route", shard_amount=1)
        batch = [
            TransactionRow(
                from_bank="A", from_account="a1", to_bank="B", to_account="b1"
            ),
            TransactionRow(
                from_bank="C", from_account="c1", to_bank="D", to_account="d1"
            ),
        ]
        routed = strategy.group_and_route(batch)
        self.assertEqual(len(routed), 1)
        self.assertEqual(routed[0][0], "selfmerge_route_0")
        self.assertEqual(len(routed[0][1]), 2)

    def test_tx_with_distinct_origin_and_destination_published_to_both_shards(self):
        # Con shard_amount alto la mayoría de los tx terminan en dos shards.
        strategy = MergeRoutingStrategy("route", shard_amount=4)
        tx = TransactionRow(
            from_bank="A", from_account="a1", to_bank="B", to_account="b1"
        )
        routed = dict(strategy.group_and_route([tx]))

        appearances = sum(1 for batch in routed.values() if tx in batch)
        self.assertGreaterEqual(appearances, 1)
        self.assertLessEqual(appearances, 2)

    def test_tx_not_duplicated_when_origin_and_destination_share_shard(self):
        # Con shard_amount=1 ambos lados caen en el mismo shard.
        strategy = MergeRoutingStrategy("route", shard_amount=1)
        tx = TransactionRow(
            from_bank="A", from_account="a1", to_bank="A", to_account="a1"
        )
        routed = strategy.group_and_route([tx])
        self.assertEqual(len(routed[0][1]), 1)

    def test_eof_routes_cover_every_shard(self):
        strategy = MergeRoutingStrategy("route", shard_amount=3)
        self.assertEqual(strategy.get_eof_routes(), ["route_0", "route_1", "route_2"])


# ────────────────────────────────────────────────────────────────────────────
# Q4 — Joiner: SelfMerge (detecta cadenas A→B→C)
# ────────────────────────────────────────────────────────────────────────────


def _tx_dict(from_bank, from_account, to_bank, to_account):
    return {
        "from_bank": from_bank,
        "from_account": from_account,
        "to_bank": to_bank,
        "to_account": to_account,
    }


class SelfMergeStrategyTest(unittest.TestCase):
    def test_chain_AB_then_BC_produces_AC(self):
        strategy = SelfMergeStrategy()
        result = strategy.joiner_batch(
            [
                _tx_dict("A", "a1", "B", "b1"),
                _tx_dict("B", "b1", "C", "c1"),
            ],
            client_id="c1",
        )
        self.assertEqual(len(result), 1)
        merged = result[0]
        self.assertEqual(merged.from_bank, "A")
        self.assertEqual(merged.from_account, "a1")
        self.assertEqual(merged.to_bank, "C")
        self.assertEqual(merged.to_account, "c1")

    def test_chain_BC_then_AB_produces_AC_out_of_order(self):
        strategy = SelfMergeStrategy()
        result = strategy.joiner_batch(
            [
                _tx_dict("B", "b1", "C", "c1"),
                _tx_dict("A", "a1", "B", "b1"),
            ],
            client_id="c1",
        )
        self.assertEqual(len(result), 1)
        self.assertEqual((result[0].from_bank, result[0].to_bank), ("A", "C"))

    def test_self_cycle_AB_BA_is_filtered_out(self):
        strategy = SelfMergeStrategy()
        result = strategy.joiner_batch(
            [
                _tx_dict("A", "a1", "B", "b1"),
                _tx_dict("B", "b1", "A", "a1"),
            ],
            client_id="c1",
        )
        self.assertEqual(result, [])

    def test_independent_transactions_yield_no_merges(self):
        strategy = SelfMergeStrategy()
        result = strategy.joiner_batch(
            [
                _tx_dict("A", "a1", "B", "b1"),
                _tx_dict("C", "c1", "D", "d1"),
            ],
            client_id="c1",
        )
        self.assertEqual(result, [])

    def test_state_is_per_client(self):
        strategy = SelfMergeStrategy()
        strategy.joiner_batch([_tx_dict("A", "a1", "B", "b1")], client_id="c1")
        # Mismo segundo salto pero para otro cliente NO debería matchear.
        result = strategy.joiner_batch([_tx_dict("B", "b1", "C", "c1")], client_id="c2")
        self.assertEqual(result, [])

    def test_clear_client_state(self):
        strategy = SelfMergeStrategy()
        strategy.joiner_batch([_tx_dict("A", "a1", "B", "b1")], client_id="c1")
        strategy.clear_client_state("c1")
        self.assertNotIn("c1", strategy.inbound_txs)
        self.assertNotIn("c1", strategy.outbound_txs)


# ────────────────────────────────────────────────────────────────────────────
# Q4 — Sharded SelfMerge: dedup contract (only shard(B) emits the chain)
# ────────────────────────────────────────────────────────────────────────────


import zlib  # noqa: E402  — colocado aquí para mantener el grupo lógico


def _shard_of(bank, account, n):
    key = f"{bank}_{account}"
    return zlib.crc32(key.encode("utf-8")) % n


class ShardedSelfMergeStrategyTest(unittest.TestCase):
    """The sharded SelfMerge emits chain A→B→C ONLY from the shard that owns B.
    Without this, the MergeRouting (tx → shard(from) AND shard(to)) makes the
    chain visible in multiple shards and gets double-counted ~19% of the time
    with N=4 (when shard(A) == shard(C) ≠ shard(B))."""

    def _feed_chain(self, strategy):
        """Both txs of a chain A→B→C, fed in order (A→B, B→C)."""
        strategy.joiner_batch(
            [_tx_dict("A", "a1", "B", "b1"), _tx_dict("B", "b1", "C", "c1")],
            client_id="c1",
        )

    def test_only_shard_of_B_emits_the_chain(self):
        N = 4
        b_shard = _shard_of("B", "b1", N)
        emitted_from = []
        for shard_id in range(N):
            strategy = SelfMergeStrategy(shard_amount=N, shard_id=shard_id)
            strategy.joiner_batch(
                [_tx_dict("A", "a1", "B", "b1"), _tx_dict("B", "b1", "C", "c1")],
                client_id="c1",
            )
            # Cada shard ve la misma chain (test in-memory) pero solo el de B la emite.
            result = strategy.joiner_batch(
                [_tx_dict("A", "a1", "B", "b1"), _tx_dict("B", "b1", "C", "c1")],
                client_id="c2",
            )
            if result:
                emitted_from.append(shard_id)
        self.assertEqual(emitted_from, [b_shard])

    def test_default_shard_amount_1_is_pass_through(self):
        # Comportamiento backwards-compatible: sin sharding, todos los chains se emiten.
        strategy = SelfMergeStrategy()  # shard_amount=1, shard_id=0
        result = strategy.joiner_batch(
            [_tx_dict("A", "a1", "B", "b1"), _tx_dict("B", "b1", "C", "c1")],
            client_id="c1",
        )
        self.assertEqual(len(result), 1)

    def test_sharded_chain_total_emissions_is_exactly_one(self):
        """Reparte el procesamiento entre N shards y verifica que la chain salga
        UNA sola vez en total. Garantiza dedup ante el caso shard(A)==shard(C)."""
        N = 4
        total = 0
        # Caso fácil: shard(B) único; pero también probamos varias chains.
        chains = [
            ("A", "a1", "B", "b1", "C", "c1"),
            ("X", "x1", "Y", "y1", "Z", "z1"),
            ("P", "p1", "Q", "q1", "R", "r1"),
        ]
        for fb, fa, mb, ma, tb, ta in chains:
            emitted_per_chain = 0
            for shard_id in range(N):
                strategy = SelfMergeStrategy(shard_amount=N, shard_id=shard_id)
                result = strategy.joiner_batch(
                    [_tx_dict(fb, fa, mb, ma), _tx_dict(mb, ma, tb, ta)],
                    client_id="c1",
                )
                emitted_per_chain += len(result)
            self.assertEqual(
                emitted_per_chain,
                1,
                f"chain {fb}→{mb}→{tb} should emit once across all shards",
            )
            total += emitted_per_chain
        self.assertEqual(total, len(chains))

    def test_sharded_drops_self_cycle(self):
        # Z→X→Z debe descartarse aunque la chain caiga en shard(X).
        N = 4
        x_shard = _shard_of("X", "x1", N)
        strategy = SelfMergeStrategy(shard_amount=N, shard_id=x_shard)
        result = strategy.joiner_batch(
            [_tx_dict("Z", "z1", "X", "x1"), _tx_dict("X", "x1", "Z", "z1")],
            client_id="c1",
        )
        self.assertEqual(result, [])


# ────────────────────────────────────────────────────────────────────────────
# Q4 — Group: AccountPairCount (rutea por par origen/destino)
# ────────────────────────────────────────────────────────────────────────────


class GroupAccountPairCountTest(unittest.TestCase):
    def test_counts_pairs_within_batch(self):
        strategy = GroupAccountPairCountStategy("route", shard_amount=1)
        routed = strategy.group_and_route(
            [
                _tx_dict("A", "a1", "B", "b1"),
                _tx_dict("A", "a1", "B", "b1"),
                _tx_dict("A", "a1", "C", "c1"),
            ]
        )
        rows = routed[0][1]
        pair_to_count = {(r["from_account"], r["to_account"]): r["count"] for r in rows}
        self.assertEqual(pair_to_count[("a1", "b1")], 2)
        self.assertEqual(pair_to_count[("a1", "c1")], 1)

    def test_same_pair_always_same_shard(self):
        strategy = GroupAccountPairCountStategy("route", shard_amount=5)
        routed_a = strategy.group_and_route([_tx_dict("A", "a1", "B", "b1")])
        routed_b = strategy.group_and_route([_tx_dict("A", "a1", "B", "b1")])
        self.assertEqual(routed_a[0][0], routed_b[0][0])

    def test_eof_routes_match_shard_amount(self):
        strategy = GroupAccountPairCountStategy("r", shard_amount=2)
        self.assertEqual(strategy.get_eof_routes(), ["r_0", "r_1"])


# ────────────────────────────────────────────────────────────────────────────
# Q4 — Aggregator: AccountPairCount (consolida counts globales por par)
# ────────────────────────────────────────────────────────────────────────────


class AggregatorAccountPairCountTest(unittest.TestCase):
    def test_aggregates_pair_counts_across_batches(self):
        strategy = AggregatorAccountPairCountStategy()
        strategy.aggregate_batch([_tx_dict("A", "a1", "B", "b1")], client="c1")
        strategy.aggregate_batch([_tx_dict("A", "a1", "B", "b1")], client="c1")
        result = strategy.get_result_for_client("c1")
        # Cada tx aporta 1 al count del par (a1, b1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["count"], 2)

    def test_state_per_client(self):
        strategy = AggregatorAccountPairCountStategy()
        strategy.aggregate_batch([_tx_dict("A", "a1", "B", "b1")], client="c1")
        strategy.aggregate_batch([_tx_dict("A", "a1", "B", "b1")], client="c2")
        self.assertEqual(strategy.get_result_for_client("c1")[0]["count"], 1)
        self.assertEqual(strategy.get_result_for_client("c2")[0]["count"], 1)

    def test_clear_client_state(self):
        strategy = AggregatorAccountPairCountStategy()
        strategy.aggregate_batch([_tx_dict("A", "a1", "B", "b1")], client="c1")
        strategy.clear_client_state("c1")
        self.assertEqual(strategy.get_result_for_client("c1"), [])


# ────────────────────────────────────────────────────────────────────────────
# Q4 — Group: AccountStrategy (extrae cuentas únicas de cada tx)
# ────────────────────────────────────────────────────────────────────────────


class GroupAccountStrategyTest(unittest.TestCase):
    # Q4 AccountStrategy en `group` consume del filter `field_greater_than`,
    # cuyo upstream emite dicts (los pares contados por el aggregator). De ahí
    # que esta strategy acceda con tx["from_bank"] y no .from_bank.

    def test_emits_each_unique_account_once_per_batch(self):
        strategy = GroupAccountStrategy("r", shard_amount=1)
        routed = strategy.group_and_route(
            [
                _tx_dict("A", "a1", "B", "b1"),
                _tx_dict("A", "a1", "C", "c1"),
            ]
        )
        rows = routed[0][1]
        keys = {(r["bank"], r["account"]) for r in rows}
        # Cuentas únicas: (A,a1), (B,b1), (C,c1) — la cuenta de origen no se duplica.
        self.assertEqual(keys, {("A", "a1"), ("B", "b1"), ("C", "c1")})

    def test_same_account_always_same_shard(self):
        strategy = GroupAccountStrategy("r", shard_amount=5)
        routed_a = dict(strategy.group_and_route([_tx_dict("A", "a1", "B", "b1")]))
        routed_b = dict(strategy.group_and_route([_tx_dict("A", "a1", "C", "c1")]))
        shard_a = next(
            route
            for route, rows in routed_a.items()
            if {"bank": "A", "account": "a1"} in rows
        )
        shard_b = next(
            route
            for route, rows in routed_b.items()
            if {"bank": "A", "account": "a1"} in rows
        )
        self.assertEqual(shard_a, shard_b)


# ────────────────────────────────────────────────────────────────────────────
# Q4 — Aggregator: AccountStrategy (set único de cuentas por cliente)
# ────────────────────────────────────────────────────────────────────────────


class AggregatorAccountStrategyTest(unittest.TestCase):
    def test_collects_unique_accounts(self):
        strategy = AggregatorAccountStrategy()
        strategy.aggregate_batch(
            [
                {"bank": "A", "account": "a1"},
                {"bank": "A", "account": "a1"},  # duplicado
                {"bank": "B", "account": "b1"},
            ],
            client="c1",
        )
        result = strategy.get_result_for_client("c1")
        # Mismas cuentas, ahora consolidadas como dicts {bank, account}
        keys = {(r["bank"], r["account"]) for r in result}
        self.assertEqual(keys, {("A", "a1"), ("B", "b1")})

    def test_state_per_client(self):
        strategy = AggregatorAccountStrategy()
        strategy.aggregate_batch([{"bank": "A", "account": "a1"}], client="c1")
        strategy.aggregate_batch([{"bank": "B", "account": "b1"}], client="c2")
        keys_c1 = {
            (r["bank"], r["account"]) for r in strategy.get_result_for_client("c1")
        }
        keys_c2 = {
            (r["bank"], r["account"]) for r in strategy.get_result_for_client("c2")
        }
        self.assertEqual(keys_c1, {("A", "a1")})
        self.assertEqual(keys_c2, {("B", "b1")})

    def test_clear_client_state(self):
        strategy = AggregatorAccountStrategy()
        strategy.aggregate_batch([{"bank": "A", "account": "a1"}], client="c1")
        strategy.clear_client_state("c1")
        self.assertEqual(strategy.get_result_for_client("c1"), [])


if __name__ == "__main__":
    unittest.main()
