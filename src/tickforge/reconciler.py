"""TickForge — Cross-Feed Reconciliation & Market Data Quality Auditor.

Continuously reconciles real-time streaming L2 order book state against
independent REST depth snapshots, flagging crossed books, price drift (bps),
sequence gaps, feed staleness, and >5-sigma return anomalies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional
import math
import statistics

from .orderbook import L2OrderBook


@dataclass(slots=True)
class ReconciliationResult:
    symbol: str
    timestamp_ms: int
    ws_last_update_id: int
    rest_last_update_id: int
    ws_mid: float
    rest_mid: float
    mid_drift_bps: float
    bid_drift_bps: float
    ask_drift_bps: float
    crossed_book: bool
    sequence_gaps: int
    z_score_outlier: bool
    status: str  # OK | WARN | ALERT


class FeedReconciler:
    """Reconciles incremental streaming book state vs authoritative REST snapshots."""

    def __init__(
        self,
        warn_drift_bps: float = 1.5,
        alert_drift_bps: float = 5.0,
        outlier_sigma: float = 5.0,
        window_size: int = 120,
    ) -> None:
        self.warn_drift_bps = warn_drift_bps
        self.alert_drift_bps = alert_drift_bps
        self.outlier_sigma = outlier_sigma
        self.window_size = window_size
        self._return_windows: Dict[str, List[float]] = {}
        self._last_mid: Dict[str, float] = {}

    def check_return_outlier(self, symbol: str, mid_price: float) -> bool:
        """Flag tick-to-tick log returns exceeding +/- outlier_sigma std devs."""
        sym = symbol.upper()
        prev = self._last_mid.get(sym)
        self._last_mid[sym] = mid_price
        if prev is None or prev <= 0 or mid_price <= 0:
            return False

        ret = math.log(mid_price / prev)
        win = self._return_windows.setdefault(sym, [])
        is_outlier = False
        if len(win) >= 20:
            mu = statistics.fmean(win)
            sd = statistics.pstdev(win)
            if sd > 1e-12 and abs(ret - mu) > self.outlier_sigma * sd:
                is_outlier = True

        win.append(ret)
        if len(win) > self.window_size:
            del win[0]
        return is_outlier

    def reconcile_snapshot(
        self,
        book: L2OrderBook,
        rest_snapshot: dict,
        timestamp_ms: int,
    ) -> ReconciliationResult:
        """Compare live in-memory L2 book against an independent Binance REST snapshot."""
        ws_bids = book.top_bids(1)
        ws_asks = book.top_asks(1)
        rest_bids = rest_snapshot.get("bids", [])
        rest_asks = rest_snapshot.get("asks", [])
        if not ws_bids or not ws_asks or not rest_bids or not rest_asks:
            raise ValueError(f"Incomplete book or REST snapshot for {book.symbol}")

        ws_bid = ws_bids[0][0]
        ws_ask = ws_asks[0][0]
        ws_mid = 0.5 * (ws_bid + ws_ask)

        rest_bid = float(rest_bids[0][0])
        rest_ask = float(rest_asks[0][0])
        rest_mid = 0.5 * (rest_bid + rest_ask)

        mid_drift_bps = abs(ws_mid - rest_mid) / rest_mid * 10_000.0
        bid_drift_bps = abs(ws_bid - rest_bid) / rest_bid * 10_000.0
        ask_drift_bps = abs(ws_ask - rest_ask) / rest_ask * 10_000.0
        crossed = ws_bid >= ws_ask or rest_bid >= rest_ask
        outlier = self.check_return_outlier(book.symbol, ws_mid)

        if crossed or mid_drift_bps >= self.alert_drift_bps:
            status = "ALERT"
        elif outlier or mid_drift_bps >= self.warn_drift_bps or book.sequence_gaps > 0:
            status = "WARN"
        else:
            status = "OK"

        return ReconciliationResult(
            symbol=book.symbol,
            timestamp_ms=int(timestamp_ms),
            ws_last_update_id=book.last_update_id,
            rest_last_update_id=int(rest_snapshot.get("lastUpdateId", 0)),
            ws_mid=ws_mid,
            rest_mid=rest_mid,
            mid_drift_bps=round(mid_drift_bps, 4),
            bid_drift_bps=round(bid_drift_bps, 4),
            ask_drift_bps=round(ask_drift_bps, 4),
            crossed_book=crossed,
            sequence_gaps=book.sequence_gaps,
            z_score_outlier=outlier,
            status=status,
        )
