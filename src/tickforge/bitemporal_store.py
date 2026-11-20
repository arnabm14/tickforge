"""TickForge — Bi-Temporal Point-in-Time Parquet / Iceberg-Ready Lakehouse.

Stores market microstructure snapshots with dual timestamps:
  - event_time_ms:     Exchange generation timestamp (what happened)
  - knowledge_time_ms: Local ingestion timestamp (when our system knew it)

Guarantees zero look-ahead bias during quantitative backtest replay by enforcing
knowledge_time_ms <= as_of_knowledge_ms across all point-in-time queries.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Iterable, List, Optional
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .orderbook import MicrostructureSnapshot


BITEMPORAL_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("event_time_ms", pa.int64()),
        ("knowledge_time_ms", pa.int64()),
        ("last_update_id", pa.int64()),
        ("best_bid", pa.float64()),
        ("best_bid_qty", pa.float64()),
        ("best_ask", pa.float64()),
        ("best_ask_qty", pa.float64()),
        ("mid_price", pa.float64()),
        ("microprice", pa.float64()),
        ("spread_bps", pa.float64()),
        ("obi_l1", pa.float64()),
        ("obi_l5", pa.float64()),
        ("obi_l10", pa.float64()),
        ("impact_buy_100k_bps", pa.float64()),
        ("impact_sell_100k_bps", pa.float64()),
        ("impact_buy_500k_bps", pa.float64()),
        ("crossed_book", pa.bool_()),
        ("processing_us", pa.float64()),
    ]
)


class BiTemporalStore:
    """Apache Parquet bi-temporal market-data store with PIT replay verification."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write_snapshots(self, snapshots: Iterable[MicrostructureSnapshot]) -> int:
        rows = [asdict(s) for s in snapshots]
        if not rows:
            return 0
        table = pa.Table.from_pylist(rows, schema=BITEMPORAL_SCHEMA)
        pq.write_table(table, self.path, compression="zstd")
        return len(rows)

    def read_all(self) -> pa.Table:
        return pq.read_table(self.path)

    def query_as_of(
        self,
        as_of_knowledge_ms: int,
        symbol: Optional[str] = None,
    ) -> pa.Table:
        """Return strictly point-in-time rows known at or before as_of_knowledge_ms.

        Even if a late-arriving backfill has event_time_ms < as_of_knowledge_ms,
        it is EXCLUDED if knowledge_time_ms > as_of_knowledge_ms (zero look-ahead bias).
        """
        table = self.read_all()
        mask = pc.less_equal(table["knowledge_time_ms"], pa.scalar(int(as_of_knowledge_ms), pa.int64()))
        if symbol is not None:
            sym_mask = pc.equal(table["symbol"], pa.scalar(symbol.upper(), pa.string()))
            mask = pc.and_(mask, sym_mask)
        return table.filter(mask)

    def verify_zero_lookahead(self, as_of_knowledge_ms: int, symbol: Optional[str] = None) -> dict:
        """Audit PIT invariant on a replay slice and report leakage count (must be 0)."""
        pit = self.query_as_of(as_of_knowledge_ms, symbol=symbol)
        if pit.num_rows == 0:
            return {
                "as_of_knowledge_ms": as_of_knowledge_ms,
                "rows_returned": 0,
                "future_leaks": 0,
                "max_knowledge_ms": None,
                "passed": True,
            }
        max_k = pc.max(pit["knowledge_time_ms"]).as_py()
        leaks = pc.sum(
            pc.greater(pit["knowledge_time_ms"], pa.scalar(int(as_of_knowledge_ms), pa.int64()))
        ).as_py()
        return {
            "as_of_knowledge_ms": as_of_knowledge_ms,
            "rows_returned": pit.num_rows,
            "future_leaks": int(leaks or 0),
            "max_knowledge_ms": int(max_k),
            "passed": int(leaks or 0) == 0 and int(max_k) <= int(as_of_knowledge_ms),
        }
