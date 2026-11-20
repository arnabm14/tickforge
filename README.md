# TickForge

**Real-Time Binance L2 Limit Order Book, Bi-Temporal Parquet Lakehouse & Cross-Feed Reconciliation Engine**

- **Live Terminal (Cloudflare Pages):** https://tickforge-2dn.pages.dev
- **Author Portfolio:** https://arnab-mondal.pages.dev
- **Author:** Arnab Mondal — Data & Systems Engineer (`arnavm014@gmail.com`)

---

## Why TickForge?

Quantitative trading desks and systematic research platforms rely on four foundational data invariants:
1. **Exact L2 Book Synchronization (Zero Missed Updates):** Official Binance buffer-first WebSocket sync (`open WS -> buffer deltas -> fetch REST /api/v3/depth snapshot -> replay strictly with U <= lastUpdateId + 1 <= u`).
2. **Real-Time Microstructure Feature Extraction:** Sub-microsecond computation of Mid-Price, Weighted Microprice, Multi-Level Order Book Imbalance (`OBI L1/L5/L10`), Spread (bps), and Notional Market Impact / VWAP Slippage (`$100K` / `$500K`).
3. **Zero Look-Ahead Bias (Bi-Temporal Storage):** Dual-timestamped Apache Parquet (`event_time_ms` vs `knowledge_time_ms`, Zstd-compressed via PyArrow) guaranteeing that late-arriving historical backfills never leak into earlier point-in-time backtests (`knowledge_time_ms <= as_of_knowledge_ms`).
4. **Automated Cross-Feed Reconciliation:** Continuous verification of live streaming WebSocket state against independent REST depth snapshots, flagging crossed books (`best_bid >= best_ask`), top-of-book drift (bps), sequence gaps, and `> 5-sigma` return outliers.

---

## Verified Benchmark & Live Binance Capture Results

Measured directly against live Binance public feeds (`data-stream.binance.vision` & `data-api.binance.vision`):

| Metric | Measured Result |
|---|---|
| **50,000-Event Deterministic L2 Replay Throughput** | **1,545,576 events/sec** |
| **Per-Event Hot-Path Update Latency (`p50 / p95 / p99`)** | **0.5 µs / 0.5 µs / 0.6 µs** |
| **Live Multi-Pair Capture (12s window, BTC/ETH/SOL)** | **347 ticks** → `data/bitemporal_ticks.parquet` (Zstd) |
| **Bi-Temporal Point-in-Time Audit (`as_of_knowledge_ms`)** | **0 future leaks** (`174/174` rows strictly verified) |
| **Cross-Feed WS vs REST Reconciliation (`BTCUSDT / ETHUSDT / SOLUSDT`)** | **3/3 `OK`** (`0.00–1.03 bps` drift, `crossed=False`, `gaps=0`) |
| **Unit Test Suite (`tests/test_tickforge.py`)** | **5/5 passing** (`0.50s`) |

---

## Repository Layout

```text
tickforge/
├── src/tickforge/
│   ├── orderbook.py          # L2OrderBook, sequence-gap guard, microprice, OBI, VWAP slippage
│   ├── bitemporal_store.py   # PyArrow Zstd Parquet bi-temporal lakehouse + PIT leakage audit
│   ├── reconciler.py         # Cross-feed WS vs REST drift + crossed-book + 5-sigma outlier auditor
│   └── binance_ingestor.py   # Live Binance buffer-first WS/REST ingestor + 50k replay benchmark
├── tests/
│   └── test_tickforge.py     # Automated quant invariant test suite
├── web/                      # Live Cloudflare Pages quant terminal (real Binance WS + PIT explorer)
│   ├── index.html
│   ├── styles.css
│   ├── app.js
│   └── data/benchmark_telemetry.json
├── data/bitemporal_ticks.parquet
├── pyproject.toml
└── wrangler.toml
```

---

## Quickstart

```bash
# Run quant invariant unit tests
PYTHONPATH=src python -m unittest discover -s tests -v

# Capture live Binance L2 streams, write bi-temporal Parquet, reconcile & benchmark 50,000 updates
PYTHONPATH=src python -m tickforge.binance_ingestor
```
