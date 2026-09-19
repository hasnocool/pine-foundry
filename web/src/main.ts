/// web/src/main.ts
/// <reference types="vite/client" />

type Field =
  | "price" | "change" | "change_pct_prev_close" | "change_pct_1m"
  | "change_pct_5m" | "change_pct_15m" | "day_volume" | "volume_1m"
  | "shares_float" | "shares_outstanding" | "market_cap" | "issue_type"
  | "spread_bps" | "book_imbalance" | "liquidity_score" | "trade_imbalance"
  | "cvd" | "cross_venue_dislocation_bps" | "news_count_5m" | "news_count_15m"
  | "news_velocity" | "news_sources_15m" | "stream_age_ms";
type Filter = { field: Field; enabled: boolean; min: number | null; max: number | null; equals?: string | null };
type Column = { field: Field; width: number; visible: boolean };
type Definition = {
  name: string;
  universe: { issue_types: string[]; asset_classes?: string[]; session: string };
  filters: Filter[];
  sort: { field: Field; direction: "asc" | "desc" };
  columns: Column[];
  version: number;
};
type Row = {
  symbol: string;
  price: number;
  change: number | null;
  change_pct_prev_close: number | null;
  change_pct_1m: number | null;
  change_pct_5m: number | null;
  change_pct_15m: number | null;
  day_volume: number;
  volume_1m: number;
  shares_float: number | null;
  shares_outstanding: number | null;
  market_cap: number | null;
  spread_bps: number | null;
  book_imbalance: number | null;
  liquidity_score: number;
  trade_imbalance: number | null;
  cvd: number;
  cross_venue_dislocation_bps: number | null;
  news_count_5m: number;
  news_count_15m: number;
  news_velocity: number;
  news_sources_15m: number;
  stream_age_ms: number;
};
type Preset = { id: string; name: string; builtin: boolean; definition: Definition };
type NewsArticle = {
  id: string;
  provider: string;
  ticker: string | null;
  title: string;
  description: string | null;
  url: string;
  source: string | null;
  subreddit: string | null;
  published_at: string | null;
  event_type?: string;
  cluster_id?: string;
};
type EvidenceVenue = {
  provider: string;
  venue: string;
  last_price: number | null;
  bid: number | null;
  ask: number | null;
  age_ms: number;
  sequence: number | null;
  book: {
    best_bid: number | null;
    best_ask: number | null;
    spread_bps: number | null;
    mid: number | null;
    microprice: number | null;
    bid_depth_5: number;
    ask_depth_5: number;
    bid_depth_10: number;
    ask_depth_10: number;
    book_imbalance: number | null;
    liquidity_score: number;
  };
};
type SymbolEvidence = {
  symbol: string;
  row: Row;
  catalyst: string | null;
  catalysts: Array<{
    id: string;
    timestamp_ms: number;
    category: string;
    source: string;
    confidence: number;
    title: string;
    url: string | null;
  }>;
  venues: EvidenceVenue[];
  recent_news: NewsArticle[];
};
type Event =
  | { type: "snapshot"; total_matches: number; rows: Row[] }
  | { type: "result_added"; row: Row }
  | { type: "result_updated"; row: Row }
  | { type: "result_removed"; symbol: string }
  | { type: "resync_required" };

import "./styles.css";

const API = import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:3000";
const labels: Record<Field, string> = {
  price: "Price", change: "Change", change_pct_prev_close: "Chg% (Prev-Close)",
  change_pct_1m: "Chg% (1m)", change_pct_5m: "Chg% (5m)", change_pct_15m: "Chg% (15m)",
  day_volume: "Day Volume", volume_1m: "Volume (1m)", shares_float: "Shares Float",
  shares_outstanding: "Shares Outstanding", market_cap: "Market Cap", issue_type: "Issue Type",
  spread_bps: "Spread (bps)", book_imbalance: "Book Imbalance",
  liquidity_score: "Liquidity", trade_imbalance: "Trade Imbalance",
  cvd: "CVD", cross_venue_dislocation_bps: "Cross-Venue (bps)",
  news_count_5m: "News (5m)", news_count_15m: "News (15m)",
  news_velocity: "News Velocity %", news_sources_15m: "News Sources (15m)",
  stream_age_ms: "Stream Age (ms)"
};
const pctFields = new Set<Field>([
  "change_pct_prev_close","change_pct_1m","change_pct_5m","change_pct_15m",
  "book_imbalance","trade_imbalance","news_velocity"
]);
const state = {
  scanId: "",
  definition: null as Definition | null,
  rows: new Map<string, Row>(),
  totalMatches: 0,
  presets: [] as Preset[],
  socket: null as WebSocket | null,
  news: [] as NewsArticle[],
  newsQuery: "BTC",
  evidence: null as SymbolEvidence | null,
};
const app = document.querySelector<HTMLDivElement>("#app")!;

function fmt(v: number | null | undefined, mode = "num") {
  if (v == null || !Number.isFinite(v)) return "—";
  if (mode === "price") return v.toFixed(2);
  if (mode === "pct") return `${v.toFixed(2)}%`;
  if (Math.abs(v) >= 1e9) return `${(v/1e9).toFixed(2)}B`;
  if (Math.abs(v) >= 1e6) return `${(v/1e6).toFixed(2)}M`;
  if (Math.abs(v) >= 1e3) return `${(v/1e3).toFixed(1)}K`;
  return String(Math.round(v));
}

function escapeHtml(value: string) {
  return value.replace(/[&<>"']/g, char => {
    const entities: Record<string, string> = {
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    };
    return entities[char] ?? char;
  });
}

function value(row: Row, field: Field): number | undefined {
  if (field === "issue_type") return undefined;
  return row[field] as number | undefined;
}

function cell(row: Row, field: Field) {
  if (field === "price") return `$${fmt(row.price, "price")}`;
  if (field === "change") return fmt(row.change, "price");
  if (pctFields.has(field)) return fmt(value(row, field), "pct");
  if (field === "market_cap") return `$${fmt(row.market_cap)}`;
  return fmt(value(row, field));
}

function visibleRows() {
  const def = state.definition!;
  const dir = def.sort.direction === "asc" ? 1 : -1;
  return [...state.rows.values()].sort((a,b) => {
    const av = value(a, def.sort.field), bv = value(b, def.sort.field);
    if (av == null && bv == null) return a.symbol.localeCompare(b.symbol);
    if (av == null) return -1;
    if (bv == null) return 1;
    return (av - bv) * dir || a.symbol.localeCompare(b.symbol);
  });
}

function defaultColumns(): Column[] {
  return [
    "price","change","change_pct_5m","day_volume","volume_1m",
    "shares_float","market_cap","spread_bps","book_imbalance",
    "trade_imbalance","liquidity_score","news_count_5m","news_velocity",
    "news_sources_15m","stream_age_ms"
  ].map(field => ({ field: field as Field, width: 120, visible: true }));
}

function renderEvidence() {
  if (!state.evidence) return "";
  const e = state.evidence;
  return `
    <section class="panel evidence-panel">
      <div class="evidence-head">
        <div><strong>${escapeHtml(e.symbol)} evidence</strong><span class="muted">${escapeHtml(e.catalyst ?? "no classified catalyst")}</span></div>
        <button id="evidence-close">Close</button>
      </div>
      <div class="venue-grid">
        ${e.venues.map(v => `
          <div class="venue-card">
            <strong>${escapeHtml(v.venue)}</strong>
            <span class="muted">${escapeHtml(v.provider)}</span>
            <div>Last: ${fmt(v.last_price, "price")} · Bid: ${fmt(v.bid, "price")} · Ask: ${fmt(v.ask, "price")}</div>
            <div>Spread: ${fmt(v.book.spread_bps)} bps · Imbalance: ${fmt(v.book.book_imbalance, "pct")}</div>
            <div>Liquidity: ${fmt(v.book.liquidity_score)} · Age: ${fmt(v.age_ms)} ms</div>
          </div>`).join("")}
      </div>
      <div class="evidence-catalysts">
        <strong>Recent catalysts</strong>
        ${e.catalysts.slice(0,8).map(c => `
          <div class="catalyst-row">
            <span>${escapeHtml(c.category)}</span>
            <strong>${escapeHtml(c.title)}</strong>
            <small>${escapeHtml(c.source)} · ${(c.confidence * 100).toFixed(0)}%</small>
          </div>`).join("") || "<p class='muted'>No recent catalysts.</p>"}
      </div>
      <div class="evidence-news">
        <strong>Related news</strong>
        ${e.recent_news.slice(0,6).map(article => `
          <a href="${escapeHtml(article.url)}" target="_blank" rel="noopener noreferrer">
            ${escapeHtml(article.title)}
            <span>${escapeHtml(article.event_type ?? article.provider)}</span>
          </a>`).join("") || "<p class='muted'>No recent related news.</p>"}
      </div>
    </section>`;
}

function renderNews() {
  return `
    <section class="panel news-panel">
      <div class="news-head">
        <div>
          <strong>News</strong>
          <span class="muted">Reddit · Google News · NewsAPI</span>
        </div>
        <div class="news-controls">
          <input id="news-query" value="${escapeHtml(state.newsQuery)}" placeholder="ticker or broad query">
          <button id="news-search">Search</button>
        </div>
      </div>
      <div class="news-list">
        ${state.news.length ? state.news.slice(0,12).map(article => `
          <article class="news-item">
            <a href="${escapeHtml(article.url)}" target="_blank" rel="noopener noreferrer">
              <strong>${escapeHtml(article.title)}</strong>
            </a>
            <div class="news-meta">${escapeHtml(article.source ?? article.subreddit ?? article.provider)} · ${escapeHtml(article.event_type ?? "general")} · ${escapeHtml(article.published_at ?? "")}</div>
            ${article.description ? `<p>${escapeHtml(article.description)}</p>` : ""}
          </article>`).join("") : "<p class='muted'>No cached news results yet.</p>"}
      </div>
    </section>`;
}

function render() {
  const def = state.definition;
  if (!def) return;
  app.innerHTML = `
    <div class="shell">
      <header class="topbar">
        <div><h1>Pine Foundry</h1><p>Real-time market scanner</p></div>
        <div class="live"><i></i>${state.socket?.readyState === WebSocket.OPEN ? "LIVE" : "OFFLINE"}</div>
      </header>
      ${renderNews()}
      ${renderEvidence()}
      <main class="layout">
        <aside class="panel filters">
          <div class="heading"><strong>Scanner</strong><select id="preset">
            ${state.presets.map(p => `<option value="${p.id}" ${p.name === def.name ? "selected" : ""}>${p.name}</option>`).join("")}
          </select></div>
          <h3>Filters</h3>
          ${def.filters.length ? def.filters.map((f,i) => `
            <div class="filter">
              <label><input type="checkbox" data-enabled="${i}" ${f.enabled ? "checked" : ""}> ${labels[f.field]}</label>
              <div class="bounds">
                <input data-min="${i}" placeholder="min" value="${f.min ?? ""}">
                <input data-max="${i}" placeholder="max" value="${f.max ?? ""}">
              </div>
            </div>`).join("") : "<p class='muted'>No filters.</p>"}
          <h3>Columns</h3>
          <div class="columns">${def.columns.map((c,i) => `<label><input type="checkbox" data-column="${i}" ${c.visible ? "checked" : ""}> ${labels[c.field]}</label>`).join("")}</div>
        </aside>
        <section class="panel results">
          <div class="result-head"><div><strong>${def.name}</strong><span>${state.rows.size} of ${state.totalMatches}</span></div><button id="custom">Custom scanner</button></div>
          <div class="table-wrap"><table><thead><tr>
            <th>Symbol</th>
            ${def.columns.filter(c=>c.visible).map(c=>`<th data-sort="${c.field}">${labels[c.field]}</th>`).join("")}
          </tr></thead><tbody>
            ${visibleRows().slice(0,250).map(r => `<tr data-symbol="${escapeHtml(r.symbol)}"><td class="symbol">${r.symbol}</td>${def.columns.filter(c=>c.visible).map(c=>`<td>${cell(r,c.field)}</td>`).join("")}</tr>`).join("")}
          </tbody></table></div>
        </section>
      </main>
    </div>`;
  wire();
}

function wire() {
  document.querySelectorAll<HTMLElement>("tbody tr[data-symbol]").forEach(row => row.addEventListener("click", () => {
    void loadEvidence(row.dataset.symbol ?? "");
  }));
  document.querySelector("#evidence-close")?.addEventListener("click", () => {
    state.evidence = null;
    render();
  });
  document.querySelector<HTMLInputElement>("#news-query")?.addEventListener("keydown", e => {
    if (e.key === "Enter") void loadNews((e.target as HTMLInputElement).value);
  });
  document.querySelector("#news-search")?.addEventListener("click", () => {
    const query = document.querySelector<HTMLInputElement>("#news-query")?.value ?? state.newsQuery;
    void loadNews(query);
  });
  document.querySelector<HTMLSelectElement>("#preset")?.addEventListener("change", async e => {
    const p = state.presets.find(x => x.id === (e.target as HTMLSelectElement).value);
    if (!p) return;
    state.definition = structuredClone(p.definition);
    await save();
  });
  document.querySelector("#custom")?.addEventListener("click", async () => {
    state.definition = {
      name: "Custom Scanner",
      universe: { issue_types: ["common_stock"], asset_classes: ["equity"], session: "regular" },
      filters: [],
      sort: { field: "change_pct_5m", direction: "desc" },
      columns: defaultColumns(),
      version: 1
    };
    await save();
  });
  document.querySelectorAll<HTMLInputElement>("[data-enabled]").forEach(el => el.addEventListener("change", async () => {
    state.definition!.filters[Number(el.dataset.enabled)].enabled = el.checked;
    await save();
  }));
  document.querySelectorAll<HTMLInputElement>("[data-min],[data-max]").forEach(el => el.addEventListener("change", async () => {
    const i = Number(el.dataset.min ?? el.dataset.max);
    const raw = el.value.trim();
    const n = raw === "" ? null : Number(raw);
    const filter = state.definition!.filters[i];
    if (el.dataset.min !== undefined) filter.min = n != null && Number.isFinite(n) ? n : null;
    else filter.max = n != null && Number.isFinite(n) ? n : null;
    await save();
  }));
  document.querySelectorAll<HTMLInputElement>("[data-column]").forEach(el => el.addEventListener("change", async () => {
    state.definition!.columns[Number(el.dataset.column)].visible = el.checked;
    await save();
  }));
  document.querySelectorAll<HTMLElement>("th[data-sort]").forEach(el => el.addEventListener("click", async () => {
    const field = el.dataset.sort as Field;
    if (state.definition!.sort.field === field) {
      state.definition!.sort.direction = state.definition!.sort.direction === "asc" ? "desc" : "asc";
    } else {
      state.definition!.sort = { field, direction: "desc" };
    }
    await save();
  }));
}

async function save() {
  const response = await fetch(`${API}/api/scans/${state.scanId}`, {
    method: "PUT",
    headers: {"content-type":"application/json"},
    body: JSON.stringify(state.definition)
  });
  if (!response.ok) throw new Error(await response.text());
  state.rows.clear();
  await snapshot();
}

async function snapshot() {
  const response = await fetch(`${API}/api/scans/${state.scanId}/snapshot`);
  apply(await response.json() as Event);
}

function apply(event: Event) {
  if (event.type === "snapshot") {
    state.rows.clear();
    event.rows.forEach(r => state.rows.set(r.symbol, r));
    state.totalMatches = event.total_matches;
  } else if (event.type === "result_added" || event.type === "result_updated") {
    state.rows.set(event.row.symbol, event.row);
    state.totalMatches = Math.max(state.totalMatches, state.rows.size);
  } else if (event.type === "result_removed") {
    state.rows.delete(event.symbol);
  } else if (event.type === "resync_required") {
    void snapshot();
  }
  render();
}

function connect() {
  state.socket?.close();
  state.socket = new WebSocket(`${API.replace(/^http/, "ws")}/ws/scanner/${state.scanId}`);
  state.socket.onopen = () => render();
  state.socket.onclose = () => render();
  state.socket.onmessage = e => apply(JSON.parse(e.data) as Event);
}

async function boot() {
  const [presetResponse, scanResponse] = await Promise.all([
    fetch(`${API}/api/presets`),
    fetch(`${API}/api/scans`)
  ]);
  state.presets = await presetResponse.json();
  const scans: Array<{id: string; definition: Definition}> = await scanResponse.json();
  if (scans.length) {
    state.scanId = scans[0].id;
    state.definition = scans[0].definition;
  } else {
    const create = await fetch(`${API}/api/scans`, {
      method: "POST",
      headers: {"content-type":"application/json"},
      body: JSON.stringify(state.presets[0].definition)
    });
    const scan = await create.json();
    state.scanId = scan.id;
    state.definition = scan.definition;
  }
  render();
  await snapshot();
  await loadNews(state.newsQuery);
  connect();
}

async function loadEvidence(symbol: string) {
  if (!symbol) return;
  try {
    const response = await fetch(`${API}/api/evidence/${encodeURIComponent(symbol)}`);
    if (!response.ok) throw new Error(await response.text());
    state.evidence = await response.json() as SymbolEvidence;
  } catch {
    state.evidence = null;
  }
  render();
}

async function loadNews(query: string) {
  const clean = query.trim();
  if (!clean) return;
  state.newsQuery = clean;
  try {
    const looksLikeTicker = /^\$?[A-Za-z0-9._=/-]{1,15}$/.test(clean);
    const url = looksLikeTicker
NaN
      : `${API}/api/news/search?q=${encodeURIComponent(clean)}&limit=25`;
    const response = await fetch(url);
    if (!response.ok) throw new Error(await response.text());
    state.news = await response.json() as NewsArticle[];
  } catch {
    state.news = [];
  }
  render();
}

boot().catch(e => { app.innerHTML = `<pre class="error">${String(e)}</pre>`; });
