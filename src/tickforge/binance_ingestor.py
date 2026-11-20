"""TickForge — Live Binance WebSocket + REST Ingestor & Benchmark Runner.

Captures real live L2 order book deltas for BTCUSDT, ETHUSDT, and SOLUSDT from
Binance public market data endpoints, persists bi-temporal Zstd Parquet tables,
reconciles WS state against independent REST depth snapshots, runs a 50,000-event
hot-path replay benchmark, and exports telemetry JSON for the web terminal.
"""

from __future__ import annotations

import asyncio
import json
import random
import statistics
import time
import urllib.request
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import websockets

from .bitemporal_store import BiTemporalStore
from .orderbook import L2OrderBook, MicrostructureSnapshot
from .reconciler import FeedReconciler, ReconciliationResult

REST_BASES = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]
WS_BASES = [
    "wss://data-stream.binance.vision/stream",
    "wss://stream.binance.com:9443/stream",
]
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def fetch_rest_depth(symbol: str, limit: int = 50) -> dict:
    last_err = None
    for base in REST_BASES:
        url = f"{base}/api/v3/depth?symbol={symbol.upper()}&limit={limit}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "TickForge/1.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"Failed to fetch REST depth for {symbol}: {last_err}")


def fetch_rest_24h_ticker(symbol: str) -> dict:
    for base in REST_BASES:
        url = f"{base}/api/v3/ticker/24hr?symbol={symbol.upper()}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "TickForge/1.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            continue
    return {}


async def capture_live_binance(
    duration_sec: float = 12.0,
) -> tuple[Dict[str, L2OrderBook], List[MicrostructureSnapshot], List[ReconciliationResult], Dict[str, dict]]:
    """Official Binance buffer-first L2 sync protocol: open WS -> buffer -> REST snapshot -> replay -> reconcile."""
    books: Dict[str, L2OrderBook] = {sym: L2OrderBook(sym) for sym in SYMBOLS}
    buffers: Dict[str, List[dict]] = {sym: [] for sym in SYMBOLS}
    snapshots: List[MicrostructureSnapshot] = []
    reconciliations: List[ReconciliationResult] = []
    tickers: Dict[str, dict] = {sym: fetch_rest_24h_ticker(sym) for sym in SYMBOLS}
    reconciler = FeedReconciler()

    streams = "/".join(f"{s.lower()}@depth@100ms" for s in SYMBOLS)
    ws_connected = False
    last_err = None

    for base in WS_BASES:
        ws_url = f"{base}?streams={streams}"
        try:
            async with websockets.connect(ws_url, open_timeout=8, ping_interval=20) as ws:
                ws_connected = True

                # Step 1: Buffer 1.0s of WS diffs FIRST before requesting REST snapshots
                buf_end = time.monotonic() + 1.0
                while time.monotonic() < buf_end:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                        msg = json.loads(raw)
                        data = msg.get("data", msg)
                        sym = data.get("s", "").upper()
                        if sym in buffers:
                            buffers[sym].append(data)
                    except asyncio.TimeoutError:
                        continue

                # Step 2: Hydrate REST snapshots while WS stays open & replay buffered diffs strictly
                loop = asyncio.get_running_loop()
                rest_snaps = await asyncio.gather(
                    *(loop.run_in_executor(None, fetch_rest_depth, sym, 100) for sym in SYMBOLS)
                )
                for sym, snap in zip(SYMBOLS, rest_snaps):
                    book = books[sym]
                    book.load_snapshot(snap["lastUpdateId"], snap["bids"], snap["asks"])
                    for ev in buffers[sym]:
                        if int(ev["u"]) <= book.last_update_id:
                            continue
                        book.apply_depth_diff(
                            int(ev["U"]),
                            int(ev["u"]),
                            ev.get("b", []),
                            ev.get("a", []),
                            strict_sequence=True,
                        )

                # Step 3: Stream live Binance L2 diffs with strict sequence checking
                start_mono = time.monotonic()
                while time.monotonic() - start_mono < duration_sec:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    except asyncio.TimeoutError:
                        continue
                    t0 = time.perf_counter_ns()
                    msg = json.loads(raw)
                    data = msg.get("data", msg)
                    sym = data.get("s", "").upper()
                    if sym not in books:
                        continue
                    book = books[sym]
                    applied = book.apply_depth_diff(
                        int(data["U"]),
                        int(data["u"]),
                        data.get("b", []),
                        data.get("a", []),
                        strict_sequence=True,
                    )
                    if applied:
                        proc_us = (time.perf_counter_ns() - t0) / 1000.0
                        now_ms = int(time.time() * 1000)
                        ms = book.compute_microstructure(
                            event_time_ms=int(data.get("E", now_ms)),
                            knowledge_time_ms=now_ms,
                            processing_us=proc_us,
                        )
                        snapshots.append(ms)
                # Step 4: Concurrent cross-feed REST reconciliation right at end of live WS window
                now_ms = int(time.time() * 1000)
                rec_snaps = await asyncio.gather(
                    *(loop.run_in_executor(None, fetch_rest_depth, sym, 20) for sym in SYMBOLS)
                )
                for sym, rest_snap in zip(SYMBOLS, rec_snaps):
                    rec = reconciler.reconcile_snapshot(books[sym], rest_snap, timestamp_ms=now_ms)
                    reconciliations.append(rec)
                break
        except Exception as exc:
            last_err = exc

    if not ws_connected:
        raise RuntimeError(f"Could not connect to Binance WS: {last_err}")

    return books, snapshots, reconciliations, tickers


def run_replay_benchmark(seed_book: L2OrderBook, num_events: int = 50_000) -> dict:
    """Measure deterministic hot-path L2 delta processing latency on 50,000 events."""
    rng = random.Random(42)
    bench_book = L2OrderBook(seed_book.symbol)
    top_b = seed_book.top_bids(30)
    top_a = seed_book.top_asks(30)
    bench_book.load_snapshot(seed_book.last_update_id, top_b, top_a)

    bid_prices = [p for p, _ in top_b]
    ask_prices = [p for p, _ in top_a]
    base_uid = bench_book.last_update_id

    # Pre-generate realistic deltas so we benchmark pure orderbook update time
    deltas = []
    for i in range(1, num_events + 1):
        b_px = bid_prices[i % len(bid_prices)]
        a_px = ask_prices[i % len(ask_prices)]
        b_qty = round(rng.uniform(0.05, 8.5), 4)
        a_qty = round(rng.uniform(0.05, 8.5), 4)
        deltas.append((base_uid + i, base_uid + i, [(b_px, b_qty)], [(a_px, a_qty)]))

    latencies_us = np.empty(num_events, dtype=np.float64)
    t_total_0 = time.perf_counter()
    for idx, (u_first, u_final, b_upd, a_upd) in enumerate(deltas):
        t0 = time.perf_counter_ns()
        bench_book.apply_depth_diff(u_first, u_final, b_upd, a_upd, strict_sequence=True)
        latencies_us[idx] = (time.perf_counter_ns() - t0) / 1000.0
    total_sec = time.perf_counter() - t_total_0

    # Histogram buckets in microseconds
    bins = [0.0, 1.0, 2.0, 3.0, 5.0, 10.0, 25.0, 100.0]
    counts, edges = np.histogram(latencies_us, bins=bins)
    hist = [
        {"bucket": f"{edges[i]:.0f}-{edges[i+1]:.0f}us", "count": int(counts[i])}
        for i in range(len(counts))
    ]
    overflow = int(np.sum(latencies_us >= bins[-1]))
    if overflow:
        hist.append({"bucket": f">={bins[-1]:.0f}us", "count": overflow})

    return {
        "events_replayed": num_events,
        "total_seconds": round(total_sec, 4),
        "throughput_events_per_sec": int(round(num_events / total_sec)),
        "latency_us": {
            "p50": round(float(np.percentile(latencies_us, 50)), 2),
            "p95": round(float(np.percentile(latencies_us, 95)), 2),
            "p99": round(float(np.percentile(latencies_us, 99)), 2),
            "p99_9": round(float(np.percentile(latencies_us, 99.9)), 2),
            "mean": round(float(np.mean(latencies_us)), 2),
        },
        "sequence_gaps": bench_book.sequence_gaps,
        "histogram": hist,
    }


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    data_dir = root / "data"
    web_data_dir = root / "web" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    web_data_dir.mkdir(parents=True, exist_ok=True)

    print("[TickForge] Connecting to live Binance L2 WebSocket + REST feeds (12s window)...")
    books, snapshots, reconciliations, tickers = asyncio.run(capture_live_binance(duration_sec=12.0))
    print(f"[TickForge] Captured {len(snapshots)} live microstructure ticks across {list(books.keys())}")

    # Persist real bi-temporal Zstd Parquet table
    parquet_path = data_dir / "bitemporal_ticks.parquet"
    store = BiTemporalStore(parquet_path)
    written = store.write_snapshots(snapshots)
    parquet_kb = round(parquet_path.stat().st_size / 1024.0, 2)

    # Point-in-time backtest replay audit at midpoint knowledge_time_ms
    k_times = sorted(s.knowledge_time_ms for s in snapshots)
    mid_k = k_times[len(k_times) // 2] if k_times else int(time.time() * 1000)
    pit_audit = store.verify_zero_lookahead(mid_k)

    # Run 50,000-event hot-path replay benchmark seeded from live BTCUSDT order book
    print("[TickForge] Running 50,000-event deterministic L2 replay benchmark...")
    bench = run_replay_benchmark(books["BTCUSDT"], num_events=50_000)

    # Build clean web telemetry bundle (strictly public Binance market data + measured metrics)
    btc_series = [
        {
            "event_time_ms": s.event_time_ms,
            "knowledge_time_ms": s.knowledge_time_ms,
            "mid_price": round(s.mid_price, 2),
            "microprice": round(s.microprice, 4),
            "spread_bps": round(s.spread_bps, 4),
            "obi_l5": round(s.obi_l5, 4),
            "impact_buy_100k_bps": round(s.impact_buy_100k_bps, 4),
            "processing_us": round(s.processing_us, 2),
        }
        for s in snapshots
        if s.symbol == "BTCUSDT"
    ][-120:]

    bundle = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "source": "Binance Public Market Data API (data-api.binance.vision / data-stream.binance.vision)",
        "live_capture": {
            "duration_sec": 12.0,
            "symbols": SYMBOLS,
            "total_ticks_captured": written,
            "parquet_file": "data/bitemporal_ticks.parquet",
            "parquet_size_kb": parquet_kb,
            "compression": "zstd",
        },
        "benchmark_50k": bench,
        "pit_audit": pit_audit,
        "reconciliation_report": [asdict(r) for r in reconciliations],
        "symbol_snapshots": {
            sym: {
                "last_update_id": books[sym].last_update_id,
                "updates_applied": books[sym].updates_applied,
                "sequence_gaps": books[sym].sequence_gaps,
                "crossed_book_events": books[sym].crossed_book_events,
                "top_bids": books[sym].top_bids(12),
                "top_asks": books[sym].top_asks(12),
                "volume_24h_quote": float(tickers[sym].get("quoteVolume", 0.0)),
                "price_change_pct_24h": float(tickers[sym].get("priceChangePercent", 0.0)),
            }
            for sym in SYMBOLS
        },
        "bitemporal_replay_series": btc_series,
    }

    out_json = web_data_dir / "benchmark_telemetry.json"
    out_json.write_text(json.dumps(bundle, indent=2), encoding="utf-8")
    print(f"[TickForge] Exported {out_json} ({round(out_json.stat().st_size / 1024, 1)} KB)")
    print(
        f"[TickForge] 50k Benchmark: {bench['throughput_events_per_sec']:,} events/sec | "
        f"p50={bench['latency_us']['p50']}us | p95={bench['latency_us']['p95']}us | p99={bench['latency_us']['p99']}us"
    )
    print(f"[TickForge] Bi-Temporal PIT Audit: {pit_audit}")
    for rec in reconciliations:
        print(
            f"[TickForge] Reconcile {rec.symbol}: status={rec.status} | "
            f"mid_drift={rec.mid_drift_bps} bps | crossed={rec.crossed_book} | gaps={rec.sequence_gaps}"
        )


if __name__ == "__main__":
    main()
