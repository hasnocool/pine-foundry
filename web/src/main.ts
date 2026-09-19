/// web/src/main.ts
/// <reference types="vite/client" />

type Field =
  | "price" | "change" | "change_pct_prev_close" | "change_pct_1m"
  | "change_pct_5m" | "change_pct_15m" | "day_volume" | "volume_1m"
  | "shares_float" | "shares_outstanding" | "market_cap" | "issue_type";
type Filter = { field: Field; enabled: boolean; min: number | null; max: number | null; equals?: string | null };
type Column = { field: Field; width: number; visible: boolean };
type Definition = {
  name: string;
  universe: { issue_types: string[]; session: string };
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
};
type Preset = { id: string; name: string; builtin: boolean; definition: Definition };
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
  shares_outstanding: "Shares Outstanding", market_cap: "Market Cap", issue_type: "Issue Type"
};
const pctFields = new Set<Field>(["change_pct_prev_close","change_pct_1m","change_pct_5m","change_pct_15m"]);
const state = {
  scanId: "",
  definition: null as Definition | null,
  rows: new Map<string, Row>(),
  totalMatches: 0,
  presets: [] as Preset[],
  socket: null as WebSocket | null,
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
  return ["price","change","change_pct_5m","day_volume","volume_1m","shares_float","market_cap"]
    .map(field => ({ field: field as Field, width: 120, visible: true }));
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
            ${visibleRows().slice(0,250).map(r => `<tr><td class="symbol">${r.symbol}</td>${def.columns.filter(c=>c.visible).map(c=>`<td>${cell(r,c.field)}</td>`).join("")}</tr>`).join("")}
          </tbody></table></div>
        </section>
      </main>
    </div>`;
  wire();
}

function wire() {
  document.querySelector<HTMLSelectElement>("#preset")?.addEventListener("change", async e => {
    const p = state.presets.find(x => x.id === (e.target as HTMLSelectElement).value);
    if (!p) return;
    state.definition = structuredClone(p.definition);
    await save();
  });
  document.querySelector("#custom")?.addEventListener("click", async () => {
    state.definition = {
      name: "Custom Scanner",
      universe: { issue_types: ["common_stock"], session: "regular" },
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
  connect();
}

boot().catch(e => { app.innerHTML = `<pre class="error">${String(e)}</pre>`; });
