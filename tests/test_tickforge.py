"""Automated test suite for TickForge quant invariants."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tickforge import BiTemporalStore, FeedReconciler, L2OrderBook


class TestTickForgeInvariants(unittest.TestCase):
    def setUp(self) -> None:
        self.book = L2OrderBook("BTCUSDT")
        self.book.load_snapshot(
            last_update_id=100,
            bids=[(62000.0, 2.0), (61999.0, 5.0), (61998.0, 10.0)],
            asks=[(62001.0, 1.0), (62002.0, 4.0), (62003.0, 10.0)],
        )

    def test_delta_apply_and_level_deletion(self) -> None:
        applied = self.book.apply_depth_diff(
            first_update_id=101,
            final_update_id=102,
            bids=[(62000.0, 0.0), (61999.5, 3.0)],
            asks=[(62001.0, 2.5)],
        )
        self.assertTrue(applied)
        self.assertEqual(self.book.last_update_id, 102)
        self.assertEqual(self.book.top_bids(2)[0], (61999.5, 3.0))
        self.assertNotIn(62000.0, self.book.bids)
        self.assertEqual(self.book.top_asks(1)[0], (62001.0, 2.5))

    def test_sequence_gap_detection(self) -> None:
        applied = self.book.apply_depth_diff(
            first_update_id=105,  # Gap! Expected <= 101
            final_update_id=106,
            bids=[(62000.5, 1.0)],
            asks=[],
            strict_sequence=True,
        )
        self.assertFalse(applied)
        self.assertEqual(self.book.sequence_gaps, 1)
        self.assertEqual(self.book.last_update_id, 100)

    def test_microstructure_and_crossed_book_detection(self) -> None:
        ms = self.book.compute_microstructure(event_time_ms=1_700_000_000_000)
        self.assertAlmostEqual(ms.mid_price, 62000.5)
        # Microprice shifts toward ask because bid_qty (2.0) > ask_qty (1.0)
        self.assertGreater(ms.microprice, ms.mid_price)
        self.assertFalse(ms.crossed_book)
        self.assertGreater(ms.impact_buy_100k_bps, 0.0)

        # Force crossed book invariant violation
        self.book.apply_depth_diff(101, 101, bids=[(62002.0, 1.0)], asks=[])
        ms_crossed = self.book.compute_microstructure(event_time_ms=1_700_000_001_000)
        self.assertTrue(ms_crossed.crossed_book)
        self.assertEqual(self.book.crossed_book_events, 1)

    def test_bitemporal_zero_lookahead_bias(self) -> None:
        # Tick 1: normal live tick (event=1000, known=1005)
        t1 = self.book.compute_microstructure(event_time_ms=1000, knowledge_time_ms=1005)
        # Tick 2: late-arriving historical backfill (event=900 BEFORE t1, but known=2000 AFTER cutoff!)
        t2_late_backfill = self.book.compute_microstructure(event_time_ms=900, knowledge_time_ms=2000)
        # Tick 3: future tick (event=1500, known=1510)
        t3 = self.book.compute_microstructure(event_time_ms=1500, knowledge_time_ms=1510)

        with tempfile.TemporaryDirectory() as tmp:
            store = BiTemporalStore(Path(tmp) / "pit.parquet")
            self.assertEqual(store.write_snapshots([t1, t2_late_backfill, t3]), 3)

            # Query strictly as of knowledge_time = 1200 -> MUST ONLY return t1 (excludes late backfill t2!)
            pit = store.query_as_of(as_of_knowledge_ms=1200, symbol="BTCUSDT")
            self.assertEqual(pit.num_rows, 1)
            self.assertEqual(pit["event_time_ms"].to_pylist(), [1000])

            audit = store.verify_zero_lookahead(as_of_knowledge_ms=1200, symbol="BTCUSDT")
            self.assertTrue(audit["passed"])
            self.assertEqual(audit["future_leaks"], 0)

    def test_cross_feed_reconciliation_drift_alert(self) -> None:
        reconciler = FeedReconciler(warn_drift_bps=1.0, alert_drift_bps=5.0)
        ok_res = reconciler.reconcile_snapshot(
            self.book,
            {"lastUpdateId": 100, "bids": [["62000.0", "2.0"]], "asks": [["62001.0", "1.0"]]},
            timestamp_ms=1_700_000_000_000,
        )
        self.assertEqual(ok_res.status, "OK")
        self.assertAlmostEqual(ok_res.mid_drift_bps, 0.0)

        alert_res = reconciler.reconcile_snapshot(
            self.book,
            {"lastUpdateId": 105, "bids": [["62100.0", "2.0"]], "asks": [["62101.0", "1.0"]]},
            timestamp_ms=1_700_000_001_000,
        )
        self.assertEqual(alert_res.status, "ALERT")
        self.assertGreater(alert_res.mid_drift_bps, 5.0)


if __name__ == "__main__":
    unittest.main()
