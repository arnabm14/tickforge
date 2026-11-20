"""TickForge — L2 Limit Order Book Engine & Microstructure Feature Extractor.

Implements deterministic Binance diff-depth synchronization, strict sequence-gap
detection, crossed-book invariants, weighted microprice, multi-level Order Book
Imbalance (OBI), and notional market-impact slippage estimation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple
import math
import time


@dataclass(slots=True)
class MicrostructureSnapshot:
    symbol: str
    event_time_ms: int
    knowledge_time_ms: int
    last_update_id: int
    best_bid: float
    best_bid_qty: float
    best_ask: float
    best_ask_qty: float
    mid_price: float
    microprice: float
    spread_bps: float
    obi_l1: float
    obi_l5: float
    obi_l10: float
    impact_buy_100k_bps: float
    impact_sell_100k_bps: float
    impact_buy_500k_bps: float
    crossed_book: bool
    processing_us: float


class L2OrderBook:
    """In-memory L2 limit order book with strict Binance sequence-gap guards."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol.upper()
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id: int = 0
        self.synced: bool = False
        self.updates_applied: int = 0
        self.sequence_gaps: int = 0
        self.crossed_book_events: int = 0
        self._sorted_bids_cache: Optional[List[Tuple[float, float]]] = None
        self._sorted_asks_cache: Optional[List[Tuple[float, float]]] = None

    def load_snapshot(
        self,
        last_update_id: int,
        bids: Iterable[Tuple[float | str, float | str]],
        asks: Iterable[Tuple[float | str, float | str]],
    ) -> None:
        """Hydrate full book state from a Binance REST /api/v3/depth snapshot."""
        self.bids.clear()
        self.asks.clear()
        for px, qty in bids:
            q = float(qty)
            if q > 0.0:
                self.bids[float(px)] = q
        for px, qty in asks:
            q = float(qty)
            if q > 0.0:
                self.asks[float(px)] = q
        self.last_update_id = int(last_update_id)
        self.synced = True
        self._invalidate_cache()

    def apply_depth_diff(
        self,
        first_update_id: int,
        final_update_id: int,
        bids: Iterable[Tuple[float | str, float | str]],
        asks: Iterable[Tuple[float | str, float | str]],
        *,
        strict_sequence: bool = True,
    ) -> bool:
        """Apply incremental L2 depth delta following Binance's official sync rules.

        Returns True if applied, False if dropped (stale) or gap detected.
        """
        if not self.synced:
            return False

        # Drop stale events older than snapshot
        if final_update_id <= self.last_update_id:
            return False

        # Detect sequence gap: U must be <= lastUpdateId + 1
        if strict_sequence and first_update_id > self.last_update_id + 1:
            self.sequence_gaps += 1
            return False

        for px_raw, qty_raw in bids:
            px = float(px_raw)
            qty = float(qty_raw)
            if qty <= 0.0:
                self.bids.pop(px, None)
            else:
                self.bids[px] = qty

        for px_raw, qty_raw in asks:
            px = float(px_raw)
            qty = float(qty_raw)
            if qty <= 0.0:
                self.asks.pop(px, None)
            else:
                self.asks[px] = qty

        self.last_update_id = int(final_update_id)
        self.updates_applied += 1
        self._invalidate_cache()
        return True

    def _invalidate_cache(self) -> None:
        self._sorted_bids_cache = None
        self._sorted_asks_cache = None

    def top_bids(self, depth: int = 20) -> List[Tuple[float, float]]:
        if self._sorted_bids_cache is None:
            self._sorted_bids_cache = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)
        return self._sorted_bids_cache[:depth]

    def top_asks(self, depth: int = 20) -> List[Tuple[float, float]]:
        if self._sorted_asks_cache is None:
            self._sorted_asks_cache = sorted(self.asks.items(), key=lambda x: x[0])
        return self._sorted_asks_cache[:depth]

    def estimate_market_impact_bps(self, side: str, notional_usd: float) -> float:
        """Compute exact VWAP fill slippage (bps vs mid) for a given USD order size."""
        bids = self.top_bids(50)
        asks = self.top_asks(50)
        if not bids or not asks:
            return math.nan
        mid = 0.5 * (bids[0][0] + asks[0][0])
        if mid <= 0:
            return math.nan

        levels = asks if side.lower() == "buy" else bids
        remaining_usd = notional_usd
        total_qty = 0.0
        total_cost = 0.0

        for px, qty in levels:
            level_usd = px * qty
            take_usd = min(remaining_usd, level_usd)
            take_qty = take_usd / px
            total_qty += take_qty
            total_cost += take_usd
            remaining_usd -= take_usd
            if remaining_usd <= 1e-9:
                break

        if total_qty <= 0:
            return math.nan
        vwap_fill = total_cost / total_qty
        if side.lower() == "buy":
            return ((vwap_fill - mid) / mid) * 10_000.0
        return ((mid - vwap_fill) / mid) * 10_000.0

    def compute_microstructure(
        self,
        event_time_ms: int,
        knowledge_time_ms: Optional[int] = None,
        processing_us: float = 0.0,
    ) -> MicrostructureSnapshot:
        """Extract live microstructure signals and check book invariants."""
        t0 = time.perf_counter_ns()
        if knowledge_time_ms is None:
            knowledge_time_ms = int(time.time() * 1000)

        bids = self.top_bids(20)
        asks = self.top_asks(20)
        if not bids or not asks:
            raise ValueError(f"Empty order book for {self.symbol}")

        best_bid, best_bid_qty = bids[0]
        best_ask, best_ask_qty = asks[0]
        crossed = best_bid >= best_ask
        if crossed:
            self.crossed_book_events += 1

        mid = 0.5 * (best_bid + best_ask)
        denom = best_bid_qty + best_ask_qty
        microprice = (
            (best_bid * best_ask_qty + best_ask * best_bid_qty) / denom
            if denom > 0
            else mid
        )
        spread_bps = ((best_ask - best_bid) / mid) * 10_000.0 if mid > 0 else 0.0

        def obi(n: int) -> float:
            bv = sum(q for _, q in bids[:n])
            av = sum(q for _, q in asks[:n])
            tot = bv + av
            return (bv - av) / tot if tot > 0 else 0.0

        imp_buy_100k = self.estimate_market_impact_bps("buy", 100_000.0)
        imp_sell_100k = self.estimate_market_impact_bps("sell", 100_000.0)
        imp_buy_500k = self.estimate_market_impact_bps("buy", 500_000.0)

        elapsed_us = processing_us if processing_us > 0 else (time.perf_counter_ns() - t0) / 1000.0
        return MicrostructureSnapshot(
            symbol=self.symbol,
            event_time_ms=int(event_time_ms),
            knowledge_time_ms=int(knowledge_time_ms),
            last_update_id=self.last_update_id,
            best_bid=best_bid,
            best_bid_qty=best_bid_qty,
            best_ask=best_ask,
            best_ask_qty=best_ask_qty,
            mid_price=mid,
            microprice=microprice,
            spread_bps=spread_bps,
            obi_l1=obi(1),
            obi_l5=obi(5),
            obi_l10=obi(10),
            impact_buy_100k_bps=imp_buy_100k,
            impact_sell_100k_bps=imp_sell_100k,
            impact_buy_500k_bps=imp_buy_500k,
            crossed_book=crossed,
            processing_us=elapsed_us,
        )
