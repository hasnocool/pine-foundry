// src/main.rs
mod providers;
use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use providers::{ProviderHealth, PublicProviderRouter, PublicQuote};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{fs, sync::{broadcast, RwLock}, time::{self, Duration}};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "pine-foundry", version, about = "Local real-time market scanner")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    Serve,
    Presets,
    Providers,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
enum IssueType { CommonStock, Etf, Adr, Reit, Etn, Warrant, Preferred, Right, Unit, Other }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MarketSession { PreMarket, Regular, AfterHours, Closed }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
enum Field {
    Price, Change, ChangePctPrevClose, ChangePct1m, ChangePct5m, ChangePct15m,
    DayVolume, Volume1m, SharesFloat, SharesOutstanding, MarketCap, IssueType,
}

impl Field {
    fn label(self) -> &'static str {
        match self {
            Self::Price => "Price",
            Self::Change => "Change",
            Self::ChangePctPrevClose => "Chg% (Prev-Close)",
            Self::ChangePct1m => "Chg% (1m)",
            Self::ChangePct5m => "Chg% (5m)",
            Self::ChangePct15m => "Chg% (15m)",
            Self::DayVolume => "Day Volume",
            Self::Volume1m => "Volume (1m)",
            Self::SharesFloat => "Shares Float",
            Self::SharesOutstanding => "Shares Outstanding",
            Self::MarketCap => "Market Cap",
            Self::IssueType => "Issue Type",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SortDirection { Asc, Desc }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SortSpec {
    field: Field,
    direction: SortDirection,
}

impl Default for SortSpec {
    fn default() -> Self {
        Self { field: Field::ChangePct5m, direction: SortDirection::Desc }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ColumnSpec {
    field: Field,
    #[serde(default = "default_width")]
    width: u16,
    #[serde(default = "default_visible")]
    visible: bool,
}
fn default_width() -> u16 { 120 }
fn default_visible() -> bool { true }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct UniverseSpec {
    #[serde(default)]
    issue_types: Vec<IssueType>,
    #[serde(default = "default_session")]
    session: MarketSession,
}
fn default_session() -> MarketSession { MarketSession::Regular }
impl Default for UniverseSpec {
    fn default() -> Self { Self { issue_types: vec![], session: MarketSession::Regular } }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FilterSpec {
    field: Field,
    #[serde(default = "default_enabled")]
    enabled: bool,
    min: Option<f64>,
    max: Option<f64>,
    #[serde(default)]
    equals: Option<String>,
}
fn default_enabled() -> bool { true }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ScanDefinition {
    name: String,
    #[serde(default)]
    universe: UniverseSpec,
    #[serde(default)]
    filters: Vec<FilterSpec>,
    #[serde(default)]
    sort: SortSpec,
    #[serde(default)]
    columns: Vec<ColumnSpec>,
    #[serde(default = "default_version")]
    version: u32,
}
fn default_version() -> u32 { 1 }

#[derive(Debug, Clone)]
struct MinuteBucket {
    start_ms: i64,
    close_price: f64,
    volume: f64,
}

#[derive(Debug, Clone)]
struct SecurityState {
    symbol: String,
    issue_type: IssueType,
    session: MarketSession,
    last_price: f64,
    previous_close: Option<f64>,
    day_volume: f64,
    shares_float: Option<f64>,
    shares_outstanding: Option<f64>,
    market_cap: Option<f64>,
    last_updated_ms: i64,
    minute_buckets: VecDeque<MinuteBucket>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Metrics {
    change: Option<f64>,
    change_pct_prev_close: Option<f64>,
    change_pct_1m: Option<f64>,
    change_pct_5m: Option<f64>,
    change_pct_15m: Option<f64>,
    volume_1m: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ScannerRow {
    symbol: String,
    issue_type: IssueType,
    price: f64,
    change: Option<f64>,
    change_pct_prev_close: Option<f64>,
    change_pct_1m: Option<f64>,
    change_pct_5m: Option<f64>,
    change_pct_15m: Option<f64>,
    day_volume: f64,
    volume_1m: f64,
    shares_float: Option<f64>,
    shares_outstanding: Option<f64>,
    market_cap: Option<f64>,
    session: MarketSession,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MarketEvent {
    Quote { symbol: String, ts_ms: i64, price: f64, session: MarketSession },
    Trade { symbol: String, ts_ms: i64, price: f64, size: f64, session: MarketSession },
    Reference {
        symbol: String, ts_ms: i64, issue_type: Option<IssueType>,
        shares_float: Option<f64>, shares_outstanding: Option<f64>,
        market_cap: Option<f64>, previous_close: Option<f64>, day_volume: Option<f64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ScannerEvent {
    Snapshot { scan_id: Uuid, total_matches: usize, rows: Vec<ScannerRow> },
    ResultAdded { scan_id: Uuid, row: ScannerRow },
    ResultRemoved { scan_id: Uuid, symbol: String },
    ResultUpdated { scan_id: Uuid, row: ScannerRow },
    ResyncRequired { scan_id: Uuid },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Preset {
    id: Uuid,
    name: String,
    #[serde(default)]
    builtin: bool,
    #[serde(default = "default_version")]
    version: u32,
    definition: ScanDefinition,
}

#[derive(Debug, Clone)]
struct ScanRuntime {
    id: Uuid,
    definition: ScanDefinition,
    matches: HashSet<String>,
    tx: broadcast::Sender<ScannerEvent>,
}

#[derive(Clone)]
struct AppState {
    market: Arc<RwLock<HashMap<String, SecurityState>>>,
    scans: Arc<RwLock<HashMap<Uuid, ScanRuntime>>>,
    presets: Arc<PresetStore>,
    providers: Arc<PublicProviderRouter>,
}

struct PresetStore {
    path: PathBuf,
    items: RwLock<Vec<Preset>>,
}

#[derive(Debug, Serialize)]
struct ScanSummary {
    id: Uuid,
    definition: ScanDefinition,
    total_matches: usize,
}

fn default_columns() -> Vec<ColumnSpec> {
    [
        Field::Price, Field::Change, Field::ChangePctPrevClose, Field::ChangePct1m,
        Field::ChangePct5m, Field::ChangePct15m, Field::DayVolume, Field::Volume1m,
        Field::SharesFloat, Field::SharesOutstanding, Field::MarketCap,
    ].into_iter().map(|field| ColumnSpec { field, width: 120, visible: true }).collect()
}

fn base_definition(name: &str) -> ScanDefinition {
    ScanDefinition {
        name: name.to_string(),
        universe: UniverseSpec { issue_types: vec![IssueType::CommonStock], session: MarketSession::Regular },
        filters: vec![],
        sort: SortSpec::default(),
        columns: default_columns(),
        version: 1,
    }
}

fn between(field: Field, min: Option<f64>, max: Option<f64>) -> FilterSpec {
    FilterSpec { field, enabled: true, min, max, equals: None }
}

fn builtin_presets() -> Vec<Preset> {
    let mut p = Vec::new();

    let mut s = base_definition("Small Cap Gainers (5m)");
    s.filters = vec![between(Field::SharesFloat, None, Some(20_000_000.0)), between(Field::ChangePct5m, Some(2.0), None), between(Field::DayVolume, Some(100_000.0), None), between(Field::Volume1m, Some(10_000.0), None)];
    s.sort = SortSpec { field: Field::ChangePct5m, direction: SortDirection::Desc };
    p.push(Preset { id: Uuid::from_u128(1), name: s.name.clone(), builtin: true, version: 1, definition: s });

    let mut l = base_definition("Large Cap Gainers (5m)");
    l.filters = vec![between(Field::MarketCap, Some(1_000_000_000.0), None), between(Field::ChangePct5m, Some(2.0), None), between(Field::DayVolume, Some(250_000.0), None), between(Field::Volume1m, Some(20_000.0), None)];
    l.sort = SortSpec { field: Field::ChangePct5m, direction: SortDirection::Desc };
    p.push(Preset { id: Uuid::from_u128(2), name: l.name.clone(), builtin: true, version: 1, definition: l });

    let mut sf = base_definition("Small Cap Fallers (5m)");
    sf.filters = vec![between(Field::SharesFloat, None, Some(20_000_000.0)), between(Field::ChangePct5m, None, Some(-2.0)), between(Field::DayVolume, Some(100_000.0), None), between(Field::Volume1m, Some(10_000.0), None)];
    sf.sort = SortSpec { field: Field::ChangePct5m, direction: SortDirection::Asc };
    p.push(Preset { id: Uuid::from_u128(3), name: sf.name.clone(), builtin: true, version: 1, definition: sf });

    let mut lf = base_definition("Large Cap Fallers (5m)");
    lf.filters = vec![between(Field::MarketCap, Some(1_000_000_000.0), None), between(Field::ChangePct5m, None, Some(-2.0)), between(Field::DayVolume, Some(250_000.0), None), between(Field::Volume1m, Some(20_000.0), None)];
    lf.sort = SortSpec { field: Field::ChangePct5m, direction: SortDirection::Asc };
    p.push(Preset { id: Uuid::from_u128(4), name: lf.name.clone(), builtin: true, version: 1, definition: lf });

    let mut gg = base_definition("Gap & Go");
    gg.filters = vec![between(Field::ChangePctPrevClose, Some(2.0), None), between(Field::DayVolume, Some(250_000.0), None), between(Field::Volume1m, Some(20_000.0), None)];
    gg.sort = SortSpec { field: Field::ChangePctPrevClose, direction: SortDirection::Desc };
    p.push(Preset { id: Uuid::from_u128(5), name: gg.name.clone(), builtin: true, version: 1, definition: gg });

    let mut gf = base_definition("Gap & Fade");
    gf.filters = vec![between(Field::ChangePctPrevClose, None, Some(-2.0)), between(Field::DayVolume, Some(250_000.0), None), between(Field::Volume1m, Some(20_000.0), None)];
    gf.sort = SortSpec { field: Field::ChangePctPrevClose, direction: SortDirection::Asc };
    p.push(Preset { id: Uuid::from_u128(6), name: gf.name.clone(), builtin: true, version: 1, definition: gf });

    let mut fl = base_definition("Low Float");
    fl.filters = vec![between(Field::SharesFloat, None, Some(5_000_000.0)), between(Field::Volume1m, Some(10_000.0), None)];
    fl.sort = SortSpec { field: Field::Volume1m, direction: SortDirection::Desc };
    p.push(Preset { id: Uuid::from_u128(7), name: fl.name.clone(), builtin: true, version: 1, definition: fl });

    let mut mb = base_definition("Momentum Breakout");
    mb.filters = vec![between(Field::ChangePct5m, Some(1.5), None), between(Field::ChangePct1m, Some(0.5), None), between(Field::Volume1m, Some(20_000.0), None)];
    mb.sort = SortSpec { field: Field::ChangePct5m, direction: SortDirection::Desc };
    p.push(Preset { id: Uuid::from_u128(8), name: mb.name.clone(), builtin: true, version: 1, definition: mb });

    let mut em = base_definition("Extended Movers");
    em.filters = vec![between(Field::ChangePct15m, Some(3.0), None), between(Field::Volume1m, Some(10_000.0), None)];
    em.sort = SortSpec { field: Field::ChangePct15m, direction: SortDirection::Desc };
    p.push(Preset { id: Uuid::from_u128(9), name: em.name.clone(), builtin: true, version: 1, definition: em });

    p
}

impl SecurityState {
    fn new(symbol: &str, price: f64, float: f64, outstanding: f64, cap: f64, now: i64) -> Self {
        Self {
            symbol: symbol.to_string(),
            issue_type: IssueType::CommonStock,
            session: MarketSession::Regular,
            last_price: price,
            previous_close: Some(price * 0.98),
            day_volume: 600_000.0,
            shares_float: Some(float),
            shares_outstanding: Some(outstanding),
            market_cap: Some(cap),
            last_updated_ms: now,
            minute_buckets: VecDeque::with_capacity(20),
        }
    }

    fn blank(symbol: &str, price: f64, session: MarketSession, now: i64) -> Self {
        Self {
            symbol: symbol.to_string(),
            issue_type: IssueType::CommonStock,
            session,
            last_price: price,
            previous_close: None,
            day_volume: 0.0,
            shares_float: None,
            shares_outstanding: None,
            market_cap: None,
            last_updated_ms: now,
            minute_buckets: VecDeque::with_capacity(20),
        }
    }
}

impl PresetStore {
    async fn load(path: PathBuf) -> Self {
        let custom = match fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice::<Vec<Preset>>(&bytes).unwrap_or_default(),
            Err(_) => vec![],
        };
        let mut items = builtin_presets();
        items.extend(custom);
        Self { path, items: RwLock::new(items) }
    }

    async fn list(&self) -> Vec<Preset> { self.items.read().await.clone() }

    async fn create(&self, mut preset: Preset) -> Result<Preset, String> {
        preset.id = Uuid::new_v4();
        preset.builtin = false;
        let result = preset.clone();
        let items = {
            let mut guard = self.items.write().await;
            guard.push(result.clone());
            guard.clone()
        };
        self.persist_custom(&items).await?;
        Ok(result)
    }

    async fn delete(&self, id: Uuid) -> Result<bool, String> {
        let (changed, items) = {
            let mut guard = self.items.write().await;
            if guard.iter().any(|p| p.id == id && p.builtin) {
                return Ok(false);
            }
            let before = guard.len();
            guard.retain(|p| p.id != id);
            (before != guard.len(), guard.clone())
        };
        if changed { self.persist_custom(&items).await?; }
        Ok(changed)
    }

    async fn persist_custom(&self, all: &[Preset]) -> Result<(), String> {
        let custom: Vec<_> = all.iter().filter(|p| !p.builtin).cloned().collect();
        if let Some(parent) = self.path.parent() { fs::create_dir_all(parent).await.map_err(|e| e.to_string())?; }
        let temp = self.path.with_extension("tmp");
        fs::write(&temp, serde_json::to_vec_pretty(&custom).map_err(|e| e.to_string())?).await.map_err(|e| e.to_string())?;
        fs::rename(&temp, &self.path).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}

fn metrics(s: &SecurityState) -> Metrics {
    let pct = |from: f64| if from.abs() < f64::EPSILON { None } else { Some((s.last_price / from - 1.0) * 100.0) };
    let change = s.previous_close.map(|p| s.last_price - p);
    let change_pct_prev_close = s.previous_close.and_then(pct);
    let find_price = |mins: i64| {
        let target = s.last_updated_ms - mins * 60_000;
        s.minute_buckets.iter().rev().find(|b| b.start_ms <= target).map(|b| b.close_price)
    };
    let volume_1m = s.minute_buckets.back().map(|b| b.volume).unwrap_or(0.0);
    Metrics {
        change,
        change_pct_prev_close,
        change_pct_1m: find_price(1).and_then(pct),
        change_pct_5m: find_price(5).and_then(pct),
        change_pct_15m: find_price(15).and_then(pct),
        volume_1m,
    }
}

fn row(s: &SecurityState) -> ScannerRow {
    let m = metrics(s);
    ScannerRow {
        symbol: s.symbol.clone(),
        issue_type: s.issue_type,
        price: s.last_price,
        change: m.change,
        change_pct_prev_close: m.change_pct_prev_close,
        change_pct_1m: m.change_pct_1m,
        change_pct_5m: m.change_pct_5m,
        change_pct_15m: m.change_pct_15m,
        day_volume: s.day_volume,
        volume_1m: m.volume_1m,
        shares_float: s.shares_float,
        shares_outstanding: s.shares_outstanding,
        market_cap: s.market_cap,
        session: s.session,
        updated_at_ms: s.last_updated_ms,
    }
}

fn field_value(field: Field, s: &SecurityState) -> Option<f64> {
    let m = metrics(s);
    match field {
        Field::Price => Some(s.last_price),
        Field::Change => m.change,
        Field::ChangePctPrevClose => m.change_pct_prev_close,
        Field::ChangePct1m => m.change_pct_1m,
        Field::ChangePct5m => m.change_pct_5m,
        Field::ChangePct15m => m.change_pct_15m,
        Field::DayVolume => Some(s.day_volume),
        Field::Volume1m => Some(m.volume_1m),
        Field::SharesFloat => s.shares_float,
        Field::SharesOutstanding => s.shares_outstanding,
        Field::MarketCap => s.market_cap,
        Field::IssueType => None,
    }
}

fn matches_scan(def: &ScanDefinition, s: &SecurityState) -> bool {
    if !def.universe.issue_types.is_empty() && !def.universe.issue_types.contains(&s.issue_type) { return false; }
    if def.universe.session != MarketSession::Closed && def.universe.session != s.session { return false; }

    def.filters.iter().filter(|f| f.enabled).all(|f| {
        if f.field == Field::IssueType {
            return f.equals.as_deref().map(|v| format!("{:?}", s.issue_type).to_lowercase() == v).unwrap_or(true);
        }
        let Some(v) = field_value(f.field, s) else { return false; };
        f.min.map(|x| v >= x).unwrap_or(true) && f.max.map(|x| v <= x).unwrap_or(true)
    })
}

fn compare_rows(a: &ScannerRow, b: &ScannerRow, sort: &SortSpec) -> std::cmp::Ordering {
    let val = |r: &ScannerRow| -> Option<f64> {
        match sort.field {
            Field::Price => Some(r.price),
            Field::Change => r.change,
            Field::ChangePctPrevClose => r.change_pct_prev_close,
            Field::ChangePct1m => r.change_pct_1m,
            Field::ChangePct5m => r.change_pct_5m,
            Field::ChangePct15m => r.change_pct_15m,
            Field::DayVolume => Some(r.day_volume),
            Field::Volume1m => Some(r.volume_1m),
            Field::SharesFloat => r.shares_float,
            Field::SharesOutstanding => r.shares_outstanding,
            Field::MarketCap => r.market_cap,
            Field::IssueType => None,
        }
    };
    let order = match (val(a), val(b)) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    };
    let order = if sort.direction == SortDirection::Asc { order } else { order.reverse() };
    if order == std::cmp::Ordering::Equal { a.symbol.cmp(&b.symbol) } else { order }
}

fn minute_update(s: &mut SecurityState, ts_ms: i64, price: f64, volume: f64) {
    let start = (ts_ms / 60_000) * 60_000;
    match s.minute_buckets.back_mut() {
        Some(b) if b.start_ms == start => { b.close_price = price; b.volume += volume; }
        _ => {
            s.minute_buckets.push_back(MinuteBucket { start_ms: start, close_price: price, volume });
            while s.minute_buckets.len() > 20 { s.minute_buckets.pop_front(); }
        }
    }
}

fn apply_event(s: &mut SecurityState, event: &MarketEvent) {
    match event {
        MarketEvent::Quote { ts_ms, price, session, .. } => {
            s.last_price = *price; s.session = *session; s.last_updated_ms = *ts_ms; minute_update(s, *ts_ms, *price, 0.0);
        }
        MarketEvent::Trade { ts_ms, price, size, session, .. } => {
            s.last_price = *price; s.session = *session; s.last_updated_ms = *ts_ms; s.day_volume += *size; minute_update(s, *ts_ms, *price, *size);
        }
        MarketEvent::Reference { ts_ms, issue_type, shares_float, shares_outstanding, market_cap, previous_close, day_volume, .. } => {
            s.last_updated_ms = *ts_ms;
            if let Some(v) = issue_type { s.issue_type = *v; }
            if shares_float.is_some() { s.shares_float = *shares_float; }
            if shares_outstanding.is_some() { s.shares_outstanding = *shares_outstanding; }
            if market_cap.is_some() { s.market_cap = *market_cap; }
            if previous_close.is_some() { s.previous_close = *previous_close; }
            if let Some(volume) = day_volume { s.day_volume = *volume; }
        }
    }
}

impl ScanRuntime {
    fn new(id: Uuid, definition: ScanDefinition, states: impl Iterator<Item = SecurityState>) -> Self {
        let (tx, _) = broadcast::channel(2048);
        let matches = states.filter(|s| matches_scan(&definition, s)).map(|s| s.symbol).collect();
        Self { id, definition, matches, tx }
    }

    fn rebuild(&mut self, states: impl Iterator<Item = SecurityState>) {
        self.matches = states.filter(|s| matches_scan(&self.definition, s)).map(|s| s.symbol).collect();
    }

    fn snapshot(&self, states: impl Iterator<Item = SecurityState>) -> ScannerEvent {
        let mut rows: Vec<_> = states.filter(|s| self.matches.contains(&s.symbol)).map(|s| row(&s)).collect();
        rows.sort_by(|a, b| compare_rows(a, b, &self.definition.sort));
        let total_matches = rows.len();
        ScannerEvent::Snapshot { scan_id: self.id, total_matches, rows }
    }
}

#[derive(Debug, Serialize)]
struct Health {
    ok: bool,
    service: &'static str,
    feed_mode: String,
    providers: Vec<ProviderHealth>,
}

async fn health(State(s): State<AppState>) -> Json<Health> {
    Json(Health {
        ok: true,
        service: "pine-foundry",
        feed_mode: env::var("PINE_FOUNDRY_FEED").unwrap_or_else(|_| "auto".into()),
        providers: s.providers.health().await,
    })
}

async fn provider_health(State(s): State<AppState>) -> Json<Vec<ProviderHealth>> {
    Json(s.providers.health().await)
}

async fn provider_routes() -> Json<Vec<providers::PublicProviderRoute>> {
    Json(PublicProviderRouter::routes())
}


async fn api_yahoo_quote(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<Option<PublicQuote>>, (StatusCode, String)> {
    s.providers
        .yahoo_chart(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_nasdaq_quote(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<Option<PublicQuote>>, (StatusCode, String)> {
    s.providers
        .nasdaq_quote(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_nasdaq_info(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .nasdaq_info(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_tradingview_symbol(
    State(s): State<AppState>,
    Path((exchange, symbol)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let fields = [
        "close",
        "change",
        "change_abs",
        "volume",
        "market_cap_basic",
        "float_shares_outstanding",
        "relative_volume_10d_calc",
        "RSI",
        "Recommend.All",
        "Recommend.MA",
    ];
    let exchange_symbol = format!("{exchange}:{symbol}");
    s.providers
        .tradingview_symbol(&exchange_symbol, &fields)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_binance_ticker(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .binance_24h(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_binance_klines(
    State(s): State<AppState>,
    Path((symbol, interval)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .binance_klines(&symbol, &interval, 500)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_binance_depth(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .binance_depth(&symbol, 1000)
        .await
        .map(Json)
        .map_err(internal_error)
}
\nasync fn list_presets(State(s): State<AppState>) -> Json<Vec<Preset>> { Json(s.presets.list().await) }

async fn create_preset(State(s): State<AppState>, Json(p): Json<Preset>) -> Result<Json<Preset>, (StatusCode, String)> {
    s.presets.create(p).await.map(Json).map_err(internal_error)
}

async fn delete_preset(State(s): State<AppState>, Path(id): Path<Uuid>) -> Result<StatusCode, (StatusCode, String)> {
    if s.presets.delete(id).await.map_err(internal_error)? { Ok(StatusCode::NO_CONTENT) } else { Err((StatusCode::NOT_FOUND, "preset not found or builtin".into())) }
}

async fn list_scans(State(s): State<AppState>) -> Json<Vec<ScanSummary>> {
    let scans = s.scans.read().await;
    Json(scans.values().map(|x| ScanSummary { id: x.id, definition: x.definition.clone(), total_matches: x.matches.len() }).collect())
}

async fn create_scan(State(s): State<AppState>, Json(def): Json<ScanDefinition>) -> Json<ScanSummary> {
    let id = Uuid::new_v4();
    let states = s.market.read().await.values().cloned().collect::<Vec<_>>();
    let runtime = ScanRuntime::new(id, def.clone(), states.into_iter());
    let summary = ScanSummary { id, definition: def, total_matches: runtime.matches.len() };
    s.scans.write().await.insert(id, runtime);
    Json(summary)
}

async fn update_scan(State(s): State<AppState>, Path(id): Path<Uuid>, Json(def): Json<ScanDefinition>) -> Result<Json<ScanSummary>, (StatusCode, String)> {
    let states = s.market.read().await.values().cloned().collect::<Vec<_>>();
    let mut scans = s.scans.write().await;
    let scan = scans.get_mut(&id).ok_or((StatusCode::NOT_FOUND, "scan not found".into()))?;
    scan.definition = def.clone();
    scan.rebuild(states.into_iter());
    let _ = scan.tx.send(ScannerEvent::ResyncRequired { scan_id: id });
    Ok(Json(ScanSummary { id, definition: def, total_matches: scan.matches.len() }))
}

async fn delete_scan(State(s): State<AppState>, Path(id): Path<Uuid>) -> Result<StatusCode, (StatusCode, String)> {
    if s.scans.write().await.remove(&id).is_some() { Ok(StatusCode::NO_CONTENT) } else { Err((StatusCode::NOT_FOUND, "scan not found".into())) }
}

async fn scan_snapshot(State(s): State<AppState>, Path(id): Path<Uuid>) -> Result<Json<ScannerEvent>, (StatusCode, String)> {
    let states = s.market.read().await.values().cloned().collect::<Vec<_>>();
    let scans = s.scans.read().await;
    let scan = scans.get(&id).ok_or((StatusCode::NOT_FOUND, "scan not found".into()))?;
    Ok(Json(scan.snapshot(states.into_iter())))
}

async fn scanner_ws(State(s): State<AppState>, Path(id): Path<Uuid>, ws: WebSocketUpgrade) -> impl IntoResponse {
    let rx = { let scans = s.scans.read().await; scans.get(&id).map(|scan| scan.tx.subscribe()) };
    match rx { Some(rx) => ws.on_upgrade(move |socket| socket_loop(socket, s, id, rx)), None => StatusCode::NOT_FOUND.into_response() }
}

async fn socket_loop(mut socket: WebSocket, s: AppState, id: Uuid, mut rx: broadcast::Receiver<ScannerEvent>) {
    if let Ok(snapshot) = make_snapshot(&s, id).await { if send_event(&mut socket, &snapshot).await.is_err() { return; } }
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(t))) if t.as_str() == "resync" => {
                    if let Ok(snapshot) = make_snapshot(&s, id).await { if send_event(&mut socket, &snapshot).await.is_err() { return; } }
                }
                Some(Ok(Message::Ping(p))) => { if socket.send(Message::Pong(p)).await.is_err() { return; } }
                Some(Ok(Message::Close(_))) | None => return,
                Some(Ok(_)) => {}
                Some(Err(_)) => return,
            },
            event = rx.recv() => match event {
                Ok(event) => if send_event(&mut socket, &event).await.is_err() { return; },
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    if send_event(&mut socket, &ScannerEvent::ResyncRequired { scan_id: id }).await.is_err() { return; }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}

async fn send_event(socket: &mut WebSocket, event: &ScannerEvent) -> Result<(), ()> {
    let bytes = serde_json::to_string(event).map_err(|_| ())?;
    socket.send(Message::Text(bytes.into())).await.map_err(|_| ())
}

async fn make_snapshot(s: &AppState, id: Uuid) -> Result<ScannerEvent, String> {
    let states = s.market.read().await.values().cloned().collect::<Vec<_>>();
    let scans = s.scans.read().await;
    scans.get(&id).map(|scan| scan.snapshot(states.into_iter())).ok_or_else(|| "scan not found".into())
}

async fn publish_market_state(s: &AppState, updated: SecurityState) {
    let symbol = updated.symbol.clone();
    let mut scans = s.scans.write().await;
    for scan in scans.values_mut() {
        let before = scan.matches.contains(&symbol);
        let after = matches_scan(&scan.definition, &updated);
        match (before, after) {
            (false, true) => {
                scan.matches.insert(symbol.clone());
                let _ = scan.tx.send(ScannerEvent::ResultAdded { scan_id: scan.id, row: row(&updated) });
            }
            (true, false) => {
                scan.matches.remove(&symbol);
                let _ = scan.tx.send(ScannerEvent::ResultRemoved { scan_id: scan.id, symbol: symbol.clone() });
            }
            (true, true) => {
                let _ = scan.tx.send(ScannerEvent::ResultUpdated { scan_id: scan.id, row: row(&updated) });
            }
            (false, false) => {}
        }
    }
}

async fn ingest_public_quote(s: &AppState, quote: PublicQuote) {
    let session = configured_live_session();
    let updated = {
        let mut market = s.market.write().await;
        let state = market.entry(quote.symbol.clone())
            .or_insert_with(|| SecurityState::blank(&quote.symbol, quote.price, session, quote.ts_ms));
        let old_volume = state.day_volume;
        state.last_price = quote.price;
        state.previous_close = quote.previous_close.or(state.previous_close);
        state.shares_float = quote.shares_float.or(state.shares_float);
        state.shares_outstanding = quote.shares_outstanding.or(state.shares_outstanding);
        state.market_cap = quote.market_cap.or(state.market_cap);
        state.session = session;
        if let Some(volume) = quote.volume {
            state.day_volume = volume.max(0.0);
        }
        let volume_delta = (state.day_volume - old_volume).max(0.0);
        state.last_updated_ms = quote.ts_ms;
        minute_update(state, quote.ts_ms, quote.price, volume_delta);
        state.clone()
    };
    publish_market_state(s, updated).await;
}

async fn ingest(s: &AppState, event: MarketEvent) {
    let symbol = match &event {
        MarketEvent::Quote { symbol, .. } | MarketEvent::Trade { symbol, .. } | MarketEvent::Reference { symbol, .. } => symbol,
    };
    let updated = {
        let mut market = s.market.write().await;
        if let Some(state) = market.get_mut(symbol) { apply_event(state, &event); Some(state.clone()) } else { None }
    };
    let Some(updated) = updated else { return; };

    let mut scans = s.scans.write().await;
    for scan in scans.values_mut() {
        let before = scan.matches.contains(symbol);
        let after = matches_scan(&scan.definition, &updated);
        match (before, after) {
            (false, true) => {
                scan.matches.insert(symbol.clone());
                let _ = scan.tx.send(ScannerEvent::ResultAdded { scan_id: scan.id, row: row(&updated) });
            }
            (true, false) => {
                scan.matches.remove(symbol);
                let _ = scan.tx.send(ScannerEvent::ResultRemoved { scan_id: scan.id, symbol: symbol.clone() });
            }
            (true, true) => {
                let _ = scan.tx.send(ScannerEvent::ResultUpdated { scan_id: scan.id, row: row(&updated) });
            }
            (false, false) => {}
        }
    }
}

async fn live_feed(s: AppState) {
    let provider = s.providers.clone();
    let poll_secs = env::var("PINE_FOUNDRY_POLL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(5)
        .max(1);
    let page_size = env::var("PINE_FOUNDRY_TV_PAGE_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(5_000)
        .clamp(100, 10_000);
    let max_rows = env::var("PINE_FOUNDRY_TV_MAX_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20_000)
        .min(50_000);
    let mut interval = time::interval(Duration::from_secs(poll_secs));

    loop {
        interval.tick().await;
        let mut delivered = 0usize;
        let mut start = 0usize;

        while start < max_rows {
            let end = (start + page_size).min(max_rows);
            match provider.tradingview_scan_us(start, end).await {
                Ok(quotes) => {
                    let count = quotes.len();
                    for quote in quotes {
                        ingest_public_quote(&s, quote).await;
                    }
                    delivered += count;
                    if count < page_size {
                        break;
                    }
                }
                Err(_) => break,
            }
            start += page_size;
        }

        if delivered == 0 {
            let symbols: Vec<String> = {
                let market = s.market.read().await;
                market.keys().take(100).cloned().collect()
            };

            match provider.yahoo_spark(&symbols).await {
                Ok(quotes) if !quotes.is_empty() => {
                    delivered = quotes.len();
                    for quote in quotes {
                        ingest_public_quote(&s, quote).await;
                    }
                }
                _ => {
                    let results = futures_util::stream::iter(symbols)
                        .map(|symbol| {
                            let provider = provider.clone();
                            async move {
                                provider.nasdaq_quote(&symbol).await.ok().flatten()
                            }
                        })
                        .buffer_unordered(8)
                        .collect::<Vec<_>>()
                        .await;
                    for quote in results.into_iter().flatten() {
                        ingest_public_quote(&s, quote).await;
                        delivered += 1;
                    }
                }
            }
        }

        if delivered == 0 {
            eprintln!("live feed: all public providers failed this cycle; retaining last state");
        }
    }
}

fn configured_live_session() -> MarketSession {
    match env::var("PINE_FOUNDRY_SESSION")
        .unwrap_or_else(|_| "regular".into())
        .to_ascii_lowercase()
        .as_str()
    {
        "pre" | "premarket" | "pre_market" => MarketSession::PreMarket,
        "after" | "afterhours" | "after_hours" => MarketSession::AfterHours,
        "closed" => MarketSession::Closed,
        _ => MarketSession::Regular,
    }
}

fn seed_market() -> HashMap<String, SecurityState> {
    let now = now_ms();
    let specs = [
        ("ALFA", 4.20, 4_800_000.0, 8_500_000.0, 35_000_000.0),
        ("BETA", 6.50, 12_000_000.0, 18_000_000.0, 115_000_000.0),
        ("CYAN", 9.25, 2_600_000.0, 4_300_000.0, 39_000_000.0),
        ("DASH", 12.50, 28_000_000.0, 42_000_000.0, 525_000_000.0),
        ("ECHO", 18.75, 95_000_000.0, 140_000_000.0, 2_625_000_000.0),
        ("FIRE", 2.85, 1_900_000.0, 4_000_000.0, 11_400_000.0),
        ("GIGA", 24.00, 55_000_000.0, 85_000_000.0, 2_040_000_000.0),
        ("HALO", 7.80, 8_300_000.0, 12_000_000.0, 93_600_000.0),
        ("IRIS", 15.60, 14_000_000.0, 22_000_000.0, 343_000_000.0),
        ("JOLT", 5.10, 3_400_000.0, 6_000_000.0, 30_600_000.0),
        ("KILO", 31.25, 120_000_000.0, 155_000_000.0, 4_843_750_000.0),
        ("LUMA", 10.40, 4_900_000.0, 9_000_000.0, 93_600_000.0),
    ];
    specs.into_iter().map(|(sym, price, flt, out, cap)| {
        let mut state = SecurityState::new(sym, price, flt, out, cap, now);
        for i in 0..16 {
            let start = ((now - (15 - i) * 60_000) / 60_000) * 60_000;
            let historical_price = price * (1.0 - 0.008 + i as f64 * 0.001);
            state.minute_buckets.push_back(MinuteBucket {
                start_ms: start,
                close_price: historical_price,
                volume: 15_000.0 + i as f64 * 5_000.0,
            });
        }
        (sym.to_string(), state)
    }).collect()
}

async fn mock_feed(s: AppState) {
    let symbols: Vec<String> = s.market.read().await.keys().cloned().collect();
    let bases: HashMap<String, f64> = symbols.iter().enumerate().map(|(i, x)| (x.clone(), 3.0 + i as f64 * 1.7)).collect();
    let mut tick = 0_u64;
    let mut interval = time::interval(Duration::from_millis(250));
    loop {
        interval.tick().await;
        tick = tick.wrapping_add(1);
        let ts = now_ms();
        for (i, symbol) in symbols.iter().enumerate() {
            if (tick + i as u64) % 3 != 0 { continue; }
            let base = bases[symbol];
            let wave = ((tick as f64 / 20.0) + i as f64 * 0.7).sin();
            let direction = if i % 5 == 0 { 0.014 } else if i % 5 == 1 { -0.006 } else { 0.003 };
            let price = (base * (1.0 + wave * 0.02 + direction * tick as f64 / 2500.0)).max(0.25);
            let size = 20_000.0 + ((tick + i as u64 * 17) % 16) as f64 * 2_500.0;
            ingest(&s, MarketEvent::Trade {
                symbol: symbol.clone(), ts_ms: ts, price, size, session: MarketSession::Regular,
            }).await;
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("clock before epoch").as_millis() as i64
}

fn internal_error(e: String) -> (StatusCode, String) { (StatusCode::INTERNAL_SERVER_ERROR, e) }

async fn shutdown_signal() {
    let ctrl_c = async { tokio::signal::ctrl_c().await.expect("ctrl-c handler"); };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("sigterm").recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}

async fn run_server() {
    let address = env::var("PINE_FOUNDRY_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let data_dir = PathBuf::from(env::var("PINE_FOUNDRY_DATA_DIR").unwrap_or_else(|_| "data".into()));
    let provider = Arc::new(PublicProviderRouter::new().expect("public provider client"));
    let state = AppState {
        market: Arc::new(RwLock::new(seed_market())),
        scans: Arc::new(RwLock::new(HashMap::new())),
        presets: Arc::new(PresetStore::load(data_dir.join("presets.json")).await),
        providers: provider,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/providers", get(provider_routes))
        .route("/api/providers/health", get(provider_health))
        .route("/api/providers/yahoo/:symbol", get(api_yahoo_quote))
        .route("/api/providers/nasdaq/:symbol", get(api_nasdaq_quote))
        .route("/api/providers/nasdaq/:symbol/info", get(api_nasdaq_info))
        .route("/api/providers/tradingview/:exchange/:symbol", get(api_tradingview_symbol))
        .route("/api/providers/binance/:symbol/ticker", get(api_binance_ticker))
        .route("/api/providers/binance/:symbol/klines/:interval", get(api_binance_klines))
        .route("/api/providers/binance/:symbol/depth", get(api_binance_depth))
        .route("/api/presets", get(list_presets).post(create_preset))
        .route("/api/presets/:id", delete(delete_preset))
        .route("/api/scans", get(list_scans).post(create_scan))
        .route("/api/scans/:id", put(update_scan).delete(delete_scan))
        .route("/api/scans/:id/snapshot", get(scan_snapshot))
        .route("/ws/scanner/:id", get(scanner_ws))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    match env::var("PINE_FOUNDRY_FEED").unwrap_or_else(|_| "auto".into()).to_ascii_lowercase().as_str() {
        "mock" => {
            tokio::spawn(mock_feed(state.clone()));
        }
        _ => {
            tokio::spawn(live_feed(state.clone()));
        }
    }
    let addr: SocketAddr = address.parse().expect("PINE_FOUNDRY_ADDR must be host:port");
    println!("Pine Foundry listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await.expect("server");
}

fn print_presets() {
    for p in builtin_presets() {
        println!("{}\t{}", p.name, p.id);
    }
}

fn print_providers() {
    for route in PublicProviderRouter::routes() {
        println!("{:?}\t{}\t{}", route.provider, route.route, route.purpose);
    }
}

#[tokio::main]
async fn main() {
    match Cli::parse().command.unwrap_or(Command::Serve) {
        Command::Serve => run_server().await,
        Command::Presets => print_presets(),
        Command::Providers => print_providers(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_filter_matches() {
        let now = 1_800_000_000_000_i64;
        let state = SecurityState::new("TEST", 8.0, 5_000_000.0, 7_000_000.0, 56_000_000.0, now);
        let mut def = base_definition("test");
        def.filters.push(between(Field::Price, Some(5.0), Some(10.0)));
        assert!(matches_scan(&def, &state));
        def.filters[0].min = Some(9.0);
        assert!(!matches_scan(&def, &state));
    }

    #[test]
    fn metrics_use_historical_buckets() {
        let now = 1_800_000_000_000_i64;
        let mut state = SecurityState::new("TEST", 10.0, 5_000_000.0, 7_000_000.0, 56_000_000.0, now);
        for i in 0..16 {
            state.minute_buckets.push_back(MinuteBucket {
                start_ms: ((now - (15 - i) * 60_000) / 60_000) * 60_000,
                close_price: 9.0 + i as f64 * 0.05,
                volume: 10_000.0,
            });
        }
        let m = metrics(&state);
        assert!(m.change_pct_5m.is_some());
        assert_eq!(m.volume_1m, 10_000.0);
    }
}
