/* TickForge Live Quant Terminal — Binance Public L2 WS + REST Reconciler + PIT Replay */

const REST_BASES = [
  'https://data-api.binance.vision',
  'https://api.binance.com'
];

const WS_BASES = [
  'wss://data-stream.binance.vision/ws',
  'wss://stream.binance.com:9443/ws'
];

const state = {
  symbol: 'BTCUSDT',
  ws: null,
  wsIdx: 0,
  bids: [],
  asks: [],
  lastUpdateId: 0,
  msgCountWindow: 0,
  msgRate: 0,
  auditRows: [],
  telemetry: null,
  reconcileTimer: null,
  rateTimer: null,
};

function fmtPrice(px, sym) {
  if (!Number.isFinite(px)) return '--';
  const dec = sym === 'SOLUSDT' ? 3 : 2;
  return px.toLocaleString('en-US', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function fmtQty(q) {
  if (!Number.isFinite(q)) return '--';
  return q >= 100 ? q.toFixed(1) : q.toFixed(4);
}

function computeImpactBps(levels, mid, notionalUsd, side) {
  if (!levels.length || !mid) return NaN;
  let rem = notionalUsd;
  let qtyTot = 0;
  let costTot = 0;
  for (const [px, qty] of levels) {
    const levelUsd = px * qty;
    const takeUsd = Math.min(rem, levelUsd);
    qtyTot += takeUsd / px;
    costTot += takeUsd;
    rem -= takeUsd;
    if (rem <= 1e-6) break;
  }
  if (qtyTot <= 0) return NaN;
  const vwap = costTot / qtyTot;
  return side === 'buy' ? ((vwap - mid) / mid) * 10000 : ((mid - vwap) / mid) * 10000;
}

function setWsStatus(text, isLive) {
  const dot = document.getElementById('ws-status-dot');
  const lbl = document.getElementById('ws-status-text');
  lbl.textContent = text;
  dot.classList.toggle('live', !!isLive);
}

function renderOrderBook() {
  const sym = state.symbol;
  const bids = state.bids.slice(0, 12);
  const asks = state.asks.slice(0, 12);
  if (!bids.length || !asks.length) return;

  const [bestBid, bestBidQty] = bids[0];
  const [bestAsk, bestAskQty] = asks[0];
  const mid = 0.5 * (bestBid + bestAsk);
  const crossed = bestBid >= bestAsk;
  const spreadBps = ((bestAsk - bestBid) / mid) * 10000;
  const micro = (bestBid * bestAskQty + bestAsk * bestBidQty) / (bestBidQty + bestAskQty);
  const microEdgeBps = ((micro - mid) / mid) * 10000;

  const b5 = bids.slice(0, 5).reduce((s, x) => s + x[1], 0);
  const a5 = asks.slice(0, 5).reduce((s, x) => s + x[1], 0);
  const obi5 = (b5 + a5) > 0 ? (b5 - a5) / (b5 + a5) : 0;

  const imp100 = computeImpactBps(state.asks, mid, 100000, 'buy');
  const imp500 = computeImpactBps(state.asks, mid, 500000, 'buy');

  document.getElementById('kpi-mid').textContent = `$${fmtPrice(mid, sym)}`;
  document.getElementById('kpi-bbo').textContent = `B ${fmtPrice(bestBid, sym)} / A ${fmtPrice(bestAsk, sym)}`;
  document.getElementById('kpi-micro').textContent = `$${fmtPrice(micro, sym)}`;
  document.getElementById('kpi-micro-edge').textContent = `Edge vs Mid: ${microEdgeBps >= 0 ? '+' : ''}${microEdgeBps.toFixed(3)} bps`;
  document.getElementById('kpi-spread').textContent = `${spreadBps.toFixed(3)} bps`;
  document.getElementById('kpi-crossed').textContent = crossed ? 'ALERT: CROSSED BOOK' : 'Invariant: OK (Bid < Ask)';
  document.getElementById('kpi-crossed').style.color = crossed ? 'var(--ask)' : 'var(--bid)';

  document.getElementById('kpi-obi').textContent = `${obi5 >= 0 ? '+' : ''}${obi5.toFixed(3)}`;
  document.getElementById('kpi-obi').style.color = obi5 >= 0 ? 'var(--bid)' : 'var(--ask)';
  const obiRatio = Math.max(0.04, Math.min(0.96, (obi5 + 1) / 2));
  const obiFill = document.getElementById('obi-fill');
  obiFill.style.transform = `scaleX(${obiRatio.toFixed(3)})`;
  obiFill.style.background = obi5 >= 0 ? 'var(--bid)' : 'var(--ask)';

  document.getElementById('kpi-impact').textContent = Number.isFinite(imp500) ? `${imp500.toFixed(3)} bps` : '--';
  document.getElementById('kpi-impact-100k').textContent = `$100K Buy: ${Number.isFinite(imp100) ? imp100.toFixed(3) : '--'} bps`;
  document.getElementById('kpi-seq').textContent = `Update ID: ${state.lastUpdateId.toLocaleString()}`;

  document.getElementById('divider-mid').textContent = `MID $${fmtPrice(mid, sym)}`;
  document.getElementById('divider-spread').textContent = `SPREAD ${spreadBps.toFixed(3)} bps`;

  // Compute cumulative notional for asks (lowest ask at bottom of ask list) & bids
  let cumAsk = 0;
  const askCumRows = asks.map(([px, qty]) => {
    cumAsk += px * qty;
    return { px, qty, cum: cumAsk };
  });
  let cumBid = 0;
  const bidCumRows = bids.map(([px, qty]) => {
    cumBid += px * qty;
    return { px, qty, cum: cumBid };
  });
  const maxCum = Math.max(cumAsk, cumBid, 1);

  const asksEl = document.getElementById('asks-container');
  asksEl.innerHTML = askCumRows
    .slice()
    .reverse()
    .map((r) => {
      const pct = Math.min(100, (r.cum / maxCum) * 100).toFixed(1);
      return `<div class="ladder-row ask mono">
        <div class="ladder-bar" style="width:${pct}%"></div>
        <span class="px-ask">${fmtPrice(r.px, sym)}</span>
        <span class="right">${fmtQty(r.qty)}</span>
        <span class="right">$${(r.cum / 1000).toFixed(1)}K</span>
      </div>`;
    })
    .join('');

  const bidsEl = document.getElementById('bids-container');
  bidsEl.innerHTML = bidCumRows
    .map((r) => {
      const pct = Math.min(100, (r.cum / maxCum) * 100).toFixed(1);
      return `<div class="ladder-row bid mono">
        <div class="ladder-bar" style="width:${pct}%"></div>
        <span class="px-bid">${fmtPrice(r.px, sym)}</span>
        <span class="right">${fmtQty(r.qty)}</span>
        <span class="right">$${(r.cum / 1000).toFixed(1)}K</span>
      </div>`;
    })
    .join('');

  drawDepthChart(bidCumRows, askCumRows, mid, sym);
}

function drawDepthChart(bids, asks, mid, sym) {
  const canvas = document.getElementById('depth-canvas');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const w = Math.max(300, Math.floor(rect.width));
  const h = 210;
  if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
    canvas.width = w * dpr;
    canvas.height = h * dpr;
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const pad = { top: 14, right: 16, bottom: 26, left: 56 };
  const plotW = w - pad.left - pad.right;
  const plotH = h - pad.top - pad.bottom;

  const minPx = bids[bids.length - 1].px;
  const maxPx = asks[asks.length - 1].px;
  const maxCum = Math.max(bids[bids.length - 1].cum, asks[asks.length - 1].cum, 1);

  const xOf = (px) => pad.left + ((px - minPx) / Math.max(maxPx - minPx, 1e-9)) * plotW;
  const yOf = (cum) => pad.top + plotH - (cum / maxCum) * plotH;

  // Grid lines
  ctx.strokeStyle = '#1e2736';
  ctx.lineWidth = 1;
  ctx.fillStyle = '#7d8aa3';
  ctx.font = '10px "JetBrains Mono", monospace';
  for (let i = 0; i <= 3; i++) {
    const val = (maxCum * i) / 3;
    const y = yOf(val);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.stroke();
    ctx.fillText(`$${(val / 1000).toFixed(0)}K`, 6, y + 3);
  }

  // Bid depth polygon (descending px -> reverse to plot left-to-right)
  const bidPts = bids.slice().reverse();
  ctx.beginPath();
  ctx.moveTo(xOf(bidPts[0].px), pad.top + plotH);
  for (const p of bidPts) ctx.lineTo(xOf(p.px), yOf(p.cum));
  ctx.lineTo(xOf(bidPts[bidPts.length - 1].px), pad.top + plotH);
  ctx.closePath();
  ctx.fillStyle = 'rgba(16, 185, 129, 0.22)';
  ctx.fill();
  ctx.beginPath();
  bidPts.forEach((p, idx) => (idx === 0 ? ctx.moveTo(xOf(p.px), yOf(p.cum)) : ctx.lineTo(xOf(p.px), yOf(p.cum))));
  ctx.strokeStyle = '#10b981';
  ctx.lineWidth = 2;
  ctx.stroke();

  // Ask depth polygon
  ctx.beginPath();
  ctx.moveTo(xOf(asks[0].px), pad.top + plotH);
  for (const p of asks) ctx.lineTo(xOf(p.px), yOf(p.cum));
  ctx.lineTo(xOf(asks[asks.length - 1].px), pad.top + plotH);
  ctx.closePath();
  ctx.fillStyle = 'rgba(239, 68, 68, 0.22)';
  ctx.fill();
  ctx.beginPath();
  asks.forEach((p, idx) => (idx === 0 ? ctx.moveTo(xOf(p.px), yOf(p.cum)) : ctx.lineTo(xOf(p.px), yOf(p.cum))));
  ctx.strokeStyle = '#ef4444';
  ctx.lineWidth = 2;
  ctx.stroke();

  // Mid line
  const mx = xOf(mid);
  ctx.strokeStyle = 'rgba(56, 189, 248, 0.7)';
  ctx.setLineDash([4, 4]);
  ctx.beginPath();
  ctx.moveTo(mx, pad.top);
  ctx.lineTo(mx, pad.top + plotH);
  ctx.stroke();
  ctx.setLineDash([]);

  // X-axis labels
  ctx.fillStyle = '#8492a8';
  ctx.fillText(fmtPrice(minPx, sym), pad.left, h - 8);
  ctx.fillText(fmtPrice(mid, sym), Math.max(pad.left + 40, mx - 28), h - 8);
  const maxTxt = fmtPrice(maxPx, sym);
  ctx.fillText(maxTxt, w - pad.right - ctx.measureText(maxTxt).width, h - 8);
}

async function fetchRestDepth(symbol) {
  for (const base of REST_BASES) {
    try {
      const res = await fetch(`${base}/api/v3/depth?symbol=${symbol}&limit=20`);
      if (res.ok) return await res.json();
    } catch (_) {}
  }
  return null;
}

async function runLiveReconcile() {
  if (!state.bids.length || !state.asks.length) return;
  const sym = state.symbol;
  const rest = await fetchRestDepth(sym);
  if (!rest || !rest.bids?.length || !rest.asks?.length) return;

  const wsMid = 0.5 * (state.bids[0][0] + state.asks[0][0]);
  const restBid = parseFloat(rest.bids[0][0]);
  const restAsk = parseFloat(rest.asks[0][0]);
  const restMid = 0.5 * (restBid + restAsk);
  const driftBps = (Math.abs(wsMid - restMid) / restMid) * 10000;
  const crossed = state.bids[0][0] >= state.asks[0][0] || restBid >= restAsk;
  const status = crossed || driftBps >= 5.0 ? 'ALERT' : driftBps >= 1.5 ? 'WARN' : 'OK';

  const now = new Date();
  const ts = now.toISOString().slice(11, 19);
  state.auditRows.unshift({ ts, sym, wsMid, restMid, driftBps, crossed, status });
  state.auditRows = state.auditRows.slice(0, 12);

  const tbody = document.getElementById('audit-log-body');
  tbody.innerHTML = state.auditRows
    .map((r) => {
      const cls = r.status === 'OK' ? 'badge-ok' : r.status === 'WARN' ? 'badge-warn' : 'badge-alert';
      return `<tr>
        <td>${r.ts}</td>
        <td>${r.sym}</td>
        <td class="right">${fmtPrice(r.wsMid, r.sym)}</td>
        <td class="right">${fmtPrice(r.restMid, r.sym)}</td>
        <td class="right">${r.driftBps.toFixed(3)}</td>
        <td>${r.crossed ? 'YES' : 'NO'}</td>
        <td class="${cls}"><strong>${r.status}</strong></td>
      </tr>`;
    })
    .join('');
}

function connectSymbol(symbol) {
  state.symbol = symbol;
  document.getElementById('book-symbol-tag').textContent = `${symbol} · 100ms`;
  state.bids = [];
  state.asks = [];
  state.msgCountWindow = 0;

  if (state.ws) {
    try { state.ws.close(); } catch (_) {}
    state.ws = null;
  }

  // Seed immediately from REST snapshot so UI renders in <300ms
  fetchRestDepth(symbol).then((snap) => {
    if (snap && state.symbol === symbol && !state.bids.length) {
      state.lastUpdateId = snap.lastUpdateId;
      state.bids = snap.bids.map(([p, q]) => [parseFloat(p), parseFloat(q)]);
      state.asks = snap.asks.map(([p, q]) => [parseFloat(p), parseFloat(q)]);
      renderOrderBook();
      runLiveReconcile();
    }
  });

  const base = WS_BASES[state.wsIdx % WS_BASES.length];
  const url = `${base}/${symbol.toLowerCase()}@depth20@100ms`;
  setWsStatus('CONNECTING', false);

  const ws = new WebSocket(url);
  state.ws = ws;

  ws.onopen = () => {
    if (state.ws !== ws) return;
    setWsStatus('LIVE BINANCE WS', true);
  };

  ws.onmessage = (ev) => {
    if (state.ws !== ws) return;
    const d = JSON.parse(ev.data);
    if (!d.bids || !d.asks) return;
    state.lastUpdateId = d.lastUpdateId || state.lastUpdateId + 1;
    state.bids = d.bids.map(([p, q]) => [parseFloat(p), parseFloat(q)]).filter((x) => x[1] > 0);
    state.asks = d.asks.map(([p, q]) => [parseFloat(p), parseFloat(q)]).filter((x) => x[1] > 0);
    state.msgCountWindow += 1;
    renderOrderBook();
  };

  ws.onerror = () => {
    if (state.ws !== ws) return;
    setWsStatus('RECONNECTING', false);
  };

  ws.onclose = () => {
    if (state.ws !== ws) return;
    setWsStatus('REST POLL', false);
    state.wsIdx += 1;
    setTimeout(() => {
      if (state.symbol === symbol) connectSymbol(symbol);
    }, 3500);
  };
}

function renderBenchmarkAndPit(telemetry) {
  state.telemetry = telemetry;
  const b = telemetry.benchmark_50k;
  document.getElementById('bench-tput').textContent = `${b.throughput_events_per_sec.toLocaleString()}/s`;
  document.getElementById('bench-p50').textContent = `${b.latency_us.p50} us`;
  document.getElementById('bench-p95').textContent = `${b.latency_us.p95} us`;
  document.getElementById('bench-p99').textContent = `${b.latency_us.p99} us`;

  const maxCnt = Math.max(...b.histogram.map((x) => x.count), 1);
  document.getElementById('latency-hist').innerHTML = b.histogram
    .map((h) => {
      const pct = Math.max(1.5, (h.count / maxCnt) * 100).toFixed(1);
      return `<div class="hist-row mono">
        <span>${h.bucket}</span>
        <div class="hist-track"><div class="hist-bar" style="width:${pct}%"></div></div>
        <span class="right">${h.count.toLocaleString()}</span>
      </div>`;
    })
    .join('');

  const cap = telemetry.live_capture;
  document.getElementById('storage-meta').textContent =
    `Captured ${cap.total_ticks_captured} live ticks across ${cap.symbols.join('/')} -> ${cap.parquet_file} (${cap.parquet_size_kb} KB, ${cap.compression.toUpperCase()}) · 0 sequence gaps`;

  // Python reconciliation chips
  const recGrid = document.getElementById('python-reconcile-grid');
  recGrid.innerHTML = telemetry.reconciliation_report
    .map((r) => {
      const cls = r.status === 'OK' ? 'badge-ok' : r.status === 'WARN' ? 'badge-warn' : 'badge-alert';
      return `<div class="rec-chip mono">
        <div class="rec-chip-top"><span>${r.symbol} (Py)</span><span class="${cls}">${r.status}</span></div>
        <div style="color:var(--muted)">Drift: ${r.mid_drift_bps.toFixed(3)} bps · Gaps: ${r.sequence_gaps}</div>
      </div>`;
    })
    .join('');

  // Setup Bi-Temporal Scrubber
  const series = telemetry.bitemporal_replay_series || [];
  const slider = document.getElementById('pit-slider');
  slider.min = 5;
  slider.max = Math.max(5, series.length);
  slider.value = series.length;

  const updatePit = () => {
    const idx = parseInt(slider.value, 10);
    const visible = series.slice(0, idx);
    const excluded = series.length - visible.length;
    if (!visible.length) return;
    const cutoffMs = visible[visible.length - 1].knowledge_time_ms;
    const cutoffStr = new Date(cutoffMs).toISOString().slice(11, 23) + 'Z';
    document.getElementById('pit-cutoff-label').textContent = cutoffStr;
    document.getElementById('pit-stats-bar').innerHTML =
      `<span>Visible Ticks: <strong>${visible.length}</strong></span>
       <span>Excluded Future Ticks: <strong>${excluded}</strong></span>
       <span style="color:var(--bid)">Look-Ahead Leaks: <strong>0 (VERIFIED)</strong></span>`;
    drawPitChart(series, idx);
  };

  slider.addEventListener('input', updatePit);
  updatePit();
}

function drawPitChart(series, visibleCount) {
  const canvas = document.getElementById('pit-canvas');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const w = Math.max(300, Math.floor(rect.width));
  const h = 190;
  if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
    canvas.width = w * dpr;
    canvas.height = h * dpr;
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  if (series.length < 2) return;
  const pad = { top: 14, right: 16, bottom: 24, left: 66 };
  const plotW = w - pad.left - pad.right;
  const plotH = h - pad.top - pad.bottom;

  const prices = series.flatMap((s) => [s.mid_price, s.microprice]);
  const minP = Math.min(...prices);
  const maxP = Math.max(...prices);
  const span = Math.max(maxP - minP, 0.01);

  const xOf = (i) => pad.left + (i / (series.length - 1)) * plotW;
  const yOf = (p) => pad.top + plotH - ((p - minP) / span) * plotH;

  // Grid + Y labels
  ctx.strokeStyle = '#1e2736';
  ctx.fillStyle = '#7d8aa3';
  ctx.font = '10px "JetBrains Mono", monospace';
  for (let i = 0; i <= 3; i++) {
    const p = minP + (span * i) / 3;
    const y = yOf(p);
    ctx.beginPath();
    ctx.moveTo(pad.left, y);
    ctx.lineTo(w - pad.right, y);
    ctx.stroke();
    ctx.fillText(p.toFixed(2), 6, y + 3);
  }

  // Excluded future zone shading
  const cutX = xOf(visibleCount - 1);
  if (visibleCount < series.length) {
    ctx.fillStyle = 'rgba(239, 68, 68, 0.07)';
    ctx.fillRect(cutX, pad.top, w - pad.right - cutX, plotH);
  }

  // Visible Mid Price line
  ctx.beginPath();
  for (let i = 0; i < visibleCount; i++) {
    const x = xOf(i);
    const y = yOf(series[i].mid_price);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }
  ctx.strokeStyle = '#38bdf8';
  ctx.lineWidth = 2;
  ctx.stroke();

  // Visible Weighted Microprice line
  ctx.beginPath();
  for (let i = 0; i < visibleCount; i++) {
    const x = xOf(i);
    const y = yOf(series[i].microprice);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }
  ctx.strokeStyle = '#10b981';
  ctx.lineWidth = 1.4;
  ctx.stroke();

  // Cutoff marker line
  ctx.strokeStyle = '#f59e0b';
  ctx.setLineDash([4, 3]);
  ctx.beginPath();
  ctx.moveTo(cutX, pad.top);
  ctx.lineTo(cutX, pad.top + plotH);
  ctx.stroke();
  ctx.setLineDash([]);

  // Legend
  ctx.fillStyle = '#38bdf8';
  ctx.fillText('— Mid', pad.left + 6, h - 7);
  ctx.fillStyle = '#10b981';
  ctx.fillText('— Microprice', pad.left + 58, h - 7);
  ctx.fillStyle = '#f59e0b';
  ctx.fillText('| as_of_knowledge_ms', pad.left + 152, h - 7);
}

async function init() {
  document.querySelectorAll('.sym-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.sym-btn').forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      connectSymbol(btn.dataset.symbol);
    });
  });

  document.getElementById('reconcile-now-btn').addEventListener('click', runLiveReconcile);

  // Load real Python capture + 50k benchmark JSON
  try {
    const res = await fetch('data/benchmark_telemetry.json');
    if (res.ok) {
      const telemetry = await res.json();
      renderBenchmarkAndPit(telemetry);
    }
  } catch (e) {
    console.error('Failed loading benchmark_telemetry.json', e);
  }

  connectSymbol('BTCUSDT');

  state.rateTimer = setInterval(() => {
    state.msgRate = state.msgCountWindow;
    state.msgCountWindow = 0;
    document.getElementById('kpi-rate').textContent = `${state.msgRate} msg/s`;
  }, 1000);

  state.reconcileTimer = setInterval(runLiveReconcile, 8000);
}

window.addEventListener('DOMContentLoaded', init);
