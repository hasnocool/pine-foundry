// src/main.rs
mod providers;
mod streams;
mod news;
mod orderbook;
mod journal;
mod filings;
mod canada;
use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post, put},
    Json, Router,
};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use canada::{CanadianDisclosureHealth, CanadianDisclosureRouter};
use filings::{FilingEvent, FilingHealth, SecFilingRouter};
use journal::{EventJournal, JournalRecord};
use news::{NewsArticle, NewsProviderHealth, NewsRouter};
use orderbook::{BookLevel, BookMetrics, OrderBookState};
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
    Replay { date: String },
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
    SpreadBps, BookImbalance, LiquidityScore, TradeImbalance, Cvd,
    CrossVenueDislocationBps, NewsCount5m, NewsCount15m, NewsVelocity,
    NewsSources15m, StreamAgeMs, RelativeVolume15m, Volatility15mPct,
    ExecutableBuy1000, ExecutableSell1000,
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
            Self::SpreadBps => "Spread (bps)",
            Self::BookImbalance => "Book Imbalance",
            Self::LiquidityScore => "Liquidity",
            Self::TradeImbalance => "Trade Imbalance",
            Self::Cvd => "CVD",
            Self::CrossVenueDislocationBps => "Cross-Venue (bps)",
            Self::NewsCount5m => "News (5m)",
            Self::NewsCount15m => "News (15m)",
            Self::NewsVelocity => "News Velocity %",
            Self::NewsSources15m => "News Sources (15m)",
            Self::StreamAgeMs => "Stream Age (ms)",
            Self::RelativeVolume15m => "Relative Volume (15m)",
            Self::Volatility15mPct => "Volatility (15m) %",
            Self::ExecutableBuy1000 => "Executable Buy $1K",
            Self::ExecutableSell1000 => "Executable Sell $1K",
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
    #[serde(default)]
    asset_classes: Vec<providers::AssetClass>,
    #[serde(default = "default_session")]
    session: MarketSession,
}
fn default_session() -> MarketSession { MarketSession::Regular }
impl Default for UniverseSpec {
    fn default() -> Self {
        Self {
            issue_types: vec![],
            asset_classes: vec![],
            session: MarketSession::Regular,
        }
    }
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
struct VenueState {
    provider: providers::ProviderId,
    venue: String,
    last_price: Option<f64>,
    bid: Option<f64>,
    ask: Option<f64>,
    day_volume: f64,
    last_event_ms: i64,
    last_sequence: Option<u64>,
}

impl VenueState {
    fn new(provider: providers::ProviderId, venue: &str, now: i64) -> Self {
        Self {
            provider,
            venue: venue.to_string(),
            last_price: None,
            bid: None,
            ask: None,
            day_volume: 0.0,
            last_event_ms: now,
            last_sequence: None,
        }
    }
}

#[derive(Debug, Clone)]
struct SecurityState {
    symbol: String,
    asset_class: providers::AssetClass,
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
    venues: HashMap<String, VenueState>,
    books: HashMap<String, OrderBookState>,
    trade_count: u64,
    buy_volume: f64,
    sell_volume: f64,
    cvd: f64,
    news_events: VecDeque<(i64, String)>,
    catalysts: VecDeque<CatalystEvent>,
    last_catalyst: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Metrics {
    change: Option<f64>,
    change_pct_prev_close: Option<f64>,
    change_pct_1m: Option<f64>,
    change_pct_5m: Option<f64>,
    change_pct_15m: Option<f64>,
    volume_1m: f64,
    spread_bps: Option<f64>,
    book_imbalance: Option<f64>,
    liquidity_score: f64,
    trade_imbalance: Option<f64>,
    cvd: f64,
    cross_venue_dislocation_bps: Option<f64>,
    news_count_5m: f64,
    news_count_15m: f64,
    news_velocity: f64,
    news_sources_15m: f64,
    stream_age_ms: f64,
    relative_volume_15m: f64,
    volatility_15m_pct: f64,
    executable_buy_1000: f64,
    executable_sell_1000: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalystEvent {
    id: Uuid,
    symbol: String,
    timestamp_ms: i64,
    category: String,
    source: String,
    confidence: f64,
    title: String,
    url: Option<String>,
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
    spread_bps: Option<f64>,
    book_imbalance: Option<f64>,
    liquidity_score: f64,
    trade_imbalance: Option<f64>,
    cvd: f64,
    cross_venue_dislocation_bps: Option<f64>,
    news_count_5m: f64,
    news_count_15m: f64,
    news_velocity: f64,
    news_sources_15m: f64,
    stream_age_ms: f64,
    relative_volume_15m: f64,
    volatility_15m_pct: f64,
    executable_buy_1000: f64,
    executable_sell_1000: f64,
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
    news: Arc<NewsRouter>,
    filings: Arc<SecFilingRouter>,
    canada: Arc<CanadianDisclosureRouter>,
    journal: Arc<EventJournal>,
    streams: Arc<streams::StreamHealthStore>,
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

#[derive(Debug, Serialize)]
struct VenueEvidence {
    provider: providers::ProviderId,
    venue: String,
    last_price: Option<f64>,
    bid: Option<f64>,
    ask: Option<f64>,
    age_ms: i64,
    sequence: Option<u64>,
    book: BookMetrics,
}

#[derive(Debug, Serialize)]
struct SymbolEvidence {
    symbol: String,
    row: ScannerRow,
    catalyst: Option<String>,
    catalysts: Vec<CatalystEvent>,
    venues: Vec<VenueEvidence>,
    recent_news: Vec<NewsArticle>,
}

fn default_columns() -> Vec<ColumnSpec> {
    [
        Field::Price, Field::Change, Field::ChangePctPrevClose, Field::ChangePct1m,
        Field::ChangePct5m, Field::ChangePct15m, Field::DayVolume, Field::Volume1m,
        Field::SharesFloat, Field::SharesOutstanding, Field::MarketCap,
        Field::SpreadBps, Field::BookImbalance, Field::LiquidityScore,
        Field::TradeImbalance, Field::Cvd, Field::CrossVenueDislocationBps,
        Field::NewsCount5m, Field::NewsCount15m, Field::NewsVelocity,
        Field::NewsSources15m, Field::StreamAgeMs, Field::RelativeVolume15m,
        Field::Volatility15mPct, Field::ExecutableBuy1000, Field::ExecutableSell1000,
    ].into_iter().map(|field| ColumnSpec { field, width: 120, visible: true }).collect()
}

fn base_definition(name: &str) -> ScanDefinition {
    ScanDefinition {
        name: name.to_string(),
        universe: UniverseSpec {
            issue_types: vec![IssueType::CommonStock],
            asset_classes: vec![providers::AssetClass::Equity],
            session: MarketSession::Regular,
        },
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

    let mut cm = ScanDefinition {
        name: "Crypto Momentum".to_string(),
        universe: UniverseSpec {
            issue_types: vec![IssueType::Other],
            asset_classes: vec![providers::AssetClass::Crypto],
            session: MarketSession::Regular,
        },
        filters: vec![
            between(Field::ChangePct5m, Some(0.5), None),
            between(Field::Volume1m, Some(1.0), None),
            between(Field::TradeImbalance, Some(0.05), None),
        ],
        sort: SortSpec { field: Field::ChangePct5m, direction: SortDirection::Desc },
        columns: default_columns(),
        version: 1,
    };
    p.push(Preset { id: Uuid::from_u128(10), name: cm.name.clone(), builtin: true, version: 1, definition: cm });

    let mut co = ScanDefinition {
        name: "Crypto Order Flow".to_string(),
        universe: UniverseSpec {
            issue_types: vec![IssueType::Other],
            asset_classes: vec![providers::AssetClass::Crypto],
            session: MarketSession::Regular,
        },
        filters: vec![
            between(Field::BookImbalance, Some(0.10), None),
            between(Field::TradeImbalance, Some(0.10), None),
            between(Field::LiquidityScore, Some(1.0), None),
        ],
        sort: SortSpec { field: Field::BookImbalance, direction: SortDirection::Desc },
        columns: default_columns(),
        version: 1,
    };
    p.push(Preset { id: Uuid::from_u128(11), name: co.name.clone(), builtin: true, version: 1, definition: co });

    let mut fx = ScanDefinition {
        name: "FX Momentum".to_string(),
        universe: UniverseSpec {
            issue_types: vec![IssueType::Other],
            asset_classes: vec![providers::AssetClass::Fx],
            session: MarketSession::Regular,
        },
        filters: vec![],
        sort: SortSpec { field: Field::Price, direction: SortDirection::Asc },
        columns: default_columns(),
        version: 1,
    };
    p.push(Preset { id: Uuid::from_u128(12), name: fx.name.clone(), builtin: true, version: 1, definition: fx });

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
            asset_class: providers::AssetClass::Equity,
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
            venues: HashMap::new(),
            books: HashMap::new(),
            trade_count: 0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            cvd: 0.0,
            news_events: VecDeque::new(),
            catalysts: VecDeque::new(),
            last_catalyst: None,
        }
    }

    fn blank(symbol: &str, price: f64, session: MarketSession, now: i64) -> Self {
        Self {
            symbol: symbol.to_string(),
            asset_class: providers::AssetClass::Equity,
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
            venues: HashMap::new(),
            books: HashMap::new(),
            trade_count: 0,
            buy_volume: 0.0,
            sell_volume: 0.0,
            cvd: 0.0,
            news_events: VecDeque::new(),
            last_catalyst: None,
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

fn current_book_metrics(s: &SecurityState) -> BookMetrics {
    let primary = crypto_primary_provider();
    let mut selected: Option<&OrderBookState> = None;
    let mut fallback: Option<&OrderBookState> = None;

    for (key, book) in &s.books {
        if fallback.is_none() {
            fallback = Some(book);
        }
        if key.to_ascii_lowercase().contains(&primary) {
            selected = Some(book);
            break;
        }
    }

    selected.or(fallback).map(OrderBookState::metrics).unwrap_or_default()
}

fn cross_venue_dislocation_bps(s: &SecurityState) -> Option<f64> {
    let prices = s.venues.values().filter_map(|venue| venue.last_price).filter(|p| *p > 0.0).collect::<Vec<_>>();
    if prices.len() < 2 {
        return None;
    }
    let min = prices.iter().copied().fold(f64::INFINITY, f64::min);
    let max = prices.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let mid = (min + max) / 2.0;
    (mid > 0.0).then_some((max - min) / mid * 10_000.0)
}

fn news_metrics(s: &SecurityState) -> (f64, f64, f64, f64) {
    let now = now_ms();
    let five = now - 5 * 60_000;
    let fifteen = now - 15 * 60_000;
    let hour = now - 60 * 60_000;

    let mut count_5m = 0.0;
    let mut count_15m = 0.0;
    let mut count_hour = 0.0;
    let mut sources = HashSet::new();

    for (timestamp, source) in &s.news_events {
        if *timestamp < hour {
            continue;
        }
        count_hour += 1.0;
        if *timestamp >= five {
            count_5m += 1.0;
        }
        if *timestamp >= fifteen {
            count_15m += 1.0;
            sources.insert(source.clone());
        }
    }

    let baseline = (count_hour / 4.0).max(0.25);
    let velocity = (count_15m / baseline - 1.0) * 100.0;
    (count_5m, count_15m, velocity.max(-100.0), sources.len() as f64)
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
    let book = current_book_metrics(s);
    let (news_count_5m, news_count_15m, news_velocity, news_sources_15m) = news_metrics(s);
    let avg_volume_15m = if s.minute_buckets.is_empty() {
        0.0
    } else {
        s.minute_buckets.iter().map(|bucket| bucket.volume).sum::<f64>()
            / s.minute_buckets.len() as f64
    };
    let relative_volume_15m = if avg_volume_15m > 0.0 {
        volume_1m / avg_volume_15m
    } else {
        0.0
    };
    let closes = s.minute_buckets.iter().map(|bucket| bucket.close_price).collect::<Vec<_>>();
    let mut returns = Vec::new();
    for window in closes.windows(2) {
        if let [a, b] = window {
            if *a > 0.0 && *b > 0.0 {
                returns.push((*b / *a).ln());
            }
        }
    }
    let volatility_15m_pct = if returns.len() > 1 {
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>()
            / (returns.len() - 1) as f64;
        variance.sqrt() * 100.0
    } else {
        0.0
    };
    let total_trade_volume = s.buy_volume + s.sell_volume;
    let trade_imbalance = if total_trade_volume > 0.0 {
        Some((s.buy_volume - s.sell_volume) / total_trade_volume)
    } else {
        None
    };

    Metrics {
        change,
        change_pct_prev_close,
        change_pct_1m: find_price(1).and_then(pct),
        change_pct_5m: find_price(5).and_then(pct),
        change_pct_15m: find_price(15).and_then(pct),
        volume_1m,
        spread_bps: book.spread_bps,
        book_imbalance: book.book_imbalance,
        liquidity_score: book.liquidity_score,
        trade_imbalance,
        cvd: s.cvd,
        cross_venue_dislocation_bps: cross_venue_dislocation_bps(s),
        news_count_5m,
        news_count_15m,
        news_velocity,
        news_sources_15m,
        stream_age_ms: (now_ms() - s.last_updated_ms).max(0) as f64,
        relative_volume_15m,
        volatility_15m_pct,
        executable_buy_1000: book.executable_buy_1000,
        executable_sell_1000: book.executable_sell_1000,
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
        spread_bps: m.spread_bps,
        book_imbalance: m.book_imbalance,
        liquidity_score: m.liquidity_score,
        trade_imbalance: m.trade_imbalance,
        cvd: m.cvd,
        cross_venue_dislocation_bps: m.cross_venue_dislocation_bps,
        news_count_5m: m.news_count_5m,
        news_count_15m: m.news_count_15m,
        news_velocity: m.news_velocity,
        news_sources_15m: m.news_sources_15m,
        stream_age_ms: m.stream_age_ms,
        relative_volume_15m: m.relative_volume_15m,
        volatility_15m_pct: m.volatility_15m_pct,
        executable_buy_1000: m.executable_buy_1000,
        executable_sell_1000: m.executable_sell_1000,
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
        Field::SpreadBps => m.spread_bps,
        Field::BookImbalance => m.book_imbalance,
        Field::LiquidityScore => Some(m.liquidity_score),
        Field::TradeImbalance => m.trade_imbalance,
        Field::Cvd => Some(m.cvd),
        Field::CrossVenueDislocationBps => m.cross_venue_dislocation_bps,
        Field::NewsCount5m => Some(m.news_count_5m),
        Field::NewsCount15m => Some(m.news_count_15m),
        Field::NewsVelocity => Some(m.news_velocity),
        Field::NewsSources15m => Some(m.news_sources_15m),
        Field::StreamAgeMs => Some(m.stream_age_ms),
        Field::RelativeVolume15m => Some(m.relative_volume_15m),
        Field::Volatility15mPct => Some(m.volatility_15m_pct),
        Field::ExecutableBuy1000 => Some(m.executable_buy_1000),
        Field::ExecutableSell1000 => Some(m.executable_sell_1000),
    }
}

fn crypto_primary_provider() -> String {
    env::var("PINE_FOUNDRY_CRYPTO_PRIMARY")
        .unwrap_or_else(|_| "binance".to_string())
        .to_ascii_lowercase()
}

fn matches_scan(def: &ScanDefinition, s: &SecurityState) -> bool {
    if !def.universe.asset_classes.is_empty() && !def.universe.asset_classes.contains(&s.asset_class) { return false; }
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
            Field::SpreadBps => r.spread_bps,
            Field::BookImbalance => r.book_imbalance,
            Field::LiquidityScore => Some(r.liquidity_score),
            Field::TradeImbalance => r.trade_imbalance,
            Field::Cvd => Some(r.cvd),
            Field::CrossVenueDislocationBps => r.cross_venue_dislocation_bps,
            Field::NewsCount5m => Some(r.news_count_5m),
            Field::NewsCount15m => Some(r.news_count_15m),
            Field::NewsVelocity => Some(r.news_velocity),
            Field::NewsSources15m => Some(r.news_sources_15m),
            Field::StreamAgeMs => Some(r.stream_age_ms),
            Field::RelativeVolume15m => Some(r.relative_volume_15m),
            Field::Volatility15mPct => Some(r.volatility_15m_pct),
            Field::ExecutableBuy1000 => Some(r.executable_buy_1000),
            Field::ExecutableSell1000 => Some(r.executable_sell_1000),
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

fn update_venue(
    s: &mut SecurityState,
    provider: providers::ProviderId,
    venue: &str,
    price: Option<f64>,
    bid: Option<f64>,
    ask: Option<f64>,
    day_volume: Option<f64>,
    ts_ms: i64,
    sequence: Option<u64>,
) {
    let key = venue.to_ascii_lowercase();
    let entry = s
        .venues
        .entry(key)
        .or_insert_with(|| VenueState::new(provider, venue, ts_ms));
    if price.is_some() {
        entry.last_price = price;
    }
    if bid.is_some() {
        entry.bid = bid;
    }
    if ask.is_some() {
        entry.ask = ask;
    }
    if let Some(volume) = day_volume {
        entry.day_volume = volume;
    }
    entry.last_event_ms = ts_ms;
    entry.last_sequence = sequence.or(entry.last_sequence);

    let is_primary = matches!(
        provider,
        providers::ProviderId::Binance
            if crypto_primary_provider() == "binance"
    ) || matches!(
        provider,
        providers::ProviderId::Kraken
            if crypto_primary_provider() == "kraken"
    ) || matches!(
        provider,
        providers::ProviderId::Coinbase
            if crypto_primary_provider() == "coinbase"
    );

    if is_primary || s.last_price <= 0.0 {
        if let Some(value) = price {
            s.last_price = value;
        } else if let (Some(bid), Some(ask)) = (bid, ask) {
            s.last_price = (bid + ask) / 2.0;
        }
    }
}

fn apply_event(
    s: &mut SecurityState,
    event: &MarketEvent,
    provider: providers::ProviderId,
    venue: &str,
    sequence: Option<u64>,
    side: Option<streams::TradeSide>,
) {
    match event {
        MarketEvent::Quote { ts_ms, price, session, .. } => {
            s.session = *session;
            s.last_updated_ms = *ts_ms;
            minute_update(s, *ts_ms, *price, 0.0);
            update_venue(s, provider, venue, Some(*price), None, None, None, *ts_ms, sequence);
            let crypto_provider = matches!(
                provider,
                providers::ProviderId::Binance
                    | providers::ProviderId::Kraken
                    | providers::ProviderId::Coinbase
            );
            if !crypto_provider || crypto_primary_provider() == provider.as_str() {
                s.last_price = *price;
            }
        }
        MarketEvent::Trade { ts_ms, price, size, session, .. } => {
            s.session = *session;
            s.last_updated_ms = *ts_ms;
            s.day_volume += *size;
            s.trade_count += 1;
            match side {
                Some(streams::TradeSide::Buy) => {
                    s.buy_volume += *size;
                    s.cvd += *size;
                }
                Some(streams::TradeSide::Sell) => {
                    s.sell_volume += *size;
                    s.cvd -= *size;
                }
                None => {}
            }
            minute_update(s, *ts_ms, *price, *size);
            update_venue(s, provider, venue, Some(*price), None, None, None, *ts_ms, sequence);
            let crypto_provider = matches!(
                provider,
                providers::ProviderId::Binance
                    | providers::ProviderId::Kraken
                    | providers::ProviderId::Coinbase
            );
            if !crypto_provider || crypto_primary_provider() == provider.as_str() {
                s.last_price = *price;
            }
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
struct JournalHealth {
    queue_dropped: u64,
}

async fn journal_health(State(s): State<AppState>) -> Json<JournalHealth> {
    Json(JournalHealth { queue_dropped: s.journal.dropped() })
}

#[derive(Debug, Serialize)]
struct Health {
    ok: bool,
    service: &'static str,
    feed_mode: String,
    providers: Vec<ProviderHealth>,
    streams: Vec<streams::StreamHealth>,
    news: Vec<NewsProviderHealth>,
    filings: FilingHealth,
    canada: CanadianDisclosureHealth,
}

async fn health(State(s): State<AppState>) -> Json<Health> {
    Json(Health {
        ok: true,
        service: "pine-foundry",
        feed_mode: env::var("PINE_FOUNDRY_FEED").unwrap_or_else(|_| "auto".into()),
        providers: s.providers.health().await,
        streams: s.streams.health().await,
        news: s.news.health().await,
        filings: s.filings.health().await,
        canada: s.canada.health().await,
    })
}

async fn provider_health(State(s): State<AppState>) -> Json<Vec<ProviderHealth>> {
    Json(s.providers.health().await)
}

async fn catalysts_for_symbol(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<Vec<CatalystEvent>>, (StatusCode, String)> {
    let market = s.market.read().await;
    let state = market
        .get(&symbol.to_ascii_uppercase())
        .ok_or((StatusCode::NOT_FOUND, "symbol not found".to_string()))?;
    Ok(Json(state.catalysts.iter().rev().cloned().collect()))
}

async fn symbol_evidence(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<SymbolEvidence>, (StatusCode, String)> {
    let state = {
        let market = s.market.read().await;
        market.get(&symbol.to_ascii_uppercase()).cloned()
    }
    .ok_or((StatusCode::NOT_FOUND, "symbol not found".to_string()))?;

    let now = now_ms();
    let mut venues = Vec::new();
    for (key, venue) in &state.venues {
        let book = state.books.get(key).map(OrderBookState::metrics).unwrap_or_default();
        venues.push(VenueEvidence {
            provider: venue.provider,
            venue: venue.venue.clone(),
            last_price: venue.last_price,
            bid: venue.bid,
            ask: venue.ask,
            age_ms: now.saturating_sub(venue.last_event_ms),
            sequence: venue.last_sequence,
            book,
        });
    }
    venues.sort_by(|a, b| a.venue.cmp(&b.venue));

    let recent_news = s
        .news
        .cache()
        .await
        .into_iter()
        .filter(|article| article.ticker.as_deref().map(|value| value.eq_ignore_ascii_case(&symbol)).unwrap_or(false))
        .take(20)
        .collect::<Vec<_>>();

    Ok(Json(SymbolEvidence {
        symbol: state.symbol.clone(),
        row: row(&state),
        catalyst: state.last_catalyst.clone(),
        catalysts: state.catalysts.iter().rev().take(20).cloned().collect(),
        venues,
        recent_news,
    }))
}

async fn canadian_disclosure_search(
    State(s): State<AppState>,
    Path(ticker): Path<String>,
) -> Result<Json<Vec<NewsArticle>>, (StatusCode, String)> {
    s.canada
        .search_ticker(&ticker, 25)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn stream_health(State(s): State<AppState>) -> Json<Vec<streams::StreamHealth>> {
    Json(s.streams.health().await)
}

async fn filing_health(State(s): State<AppState>) -> Json<FilingHealth> {
    Json(s.filings.health().await)
}

async fn filing_search(
    State(s): State<AppState>,
    Path(ticker): Path<String>,
) -> Result<Json<Vec<FilingEvent>>, (StatusCode, String)> {
    s.filings
        .search_ticker(&ticker)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn provider_routes() -> Json<Vec<providers::PublicProviderRoute>> {
    Json(PublicProviderRouter::routes())
}


async fn api_yahoo_quote(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<Option<PublicQuote>>, (StatusCode, String)> {
    s.providers
        .yahoo_chart(&symbol, providers::AssetClass::Equity)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_yahoo_fx(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<Option<PublicQuote>>, (StatusCode, String)> {
    s.providers
        .yahoo_chart(&symbol, providers::AssetClass::Fx)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_tradingview_canada(
    State(s): State<AppState>,
    Path((start, end)): Path<(usize, usize)>,
) -> Result<Json<Vec<PublicQuote>>, (StatusCode, String)> {
    s.providers
        .tradingview_scan_canada(start, end)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_tradingview_fx(
    State(s): State<AppState>,
    Path((start, end)): Path<(usize, usize)>,
) -> Result<Json<Vec<PublicQuote>>, (StatusCode, String)> {
    s.providers
        .tradingview_scan_fx(start, end)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_kraken_ticker(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<PublicQuote>, (StatusCode, String)> {
    s.providers
        .kraken_ticker(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_kraken_ohlc(
    State(s): State<AppState>,
    Path((symbol, interval)): Path<(String, u16)>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .kraken_ohlc(&symbol, interval)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_kraken_depth(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .kraken_depth(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_kraken_trades(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .kraken_trades(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_coinbase_ticker(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<PublicQuote>, (StatusCode, String)> {
    s.providers
        .coinbase_ticker(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_coinbase_candles(
    State(s): State<AppState>,
    Path((symbol, granularity)): Path<(String, u32)>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .coinbase_candles(&symbol, granularity)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_coinbase_book(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .coinbase_book(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_coinbase_trades(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .coinbase_trades(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_binance_normalized_quote(
    State(s): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<PublicQuote>, (StatusCode, String)> {
    s.providers
        .binance_quote(&symbol)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_bank_of_canada_fx(
    State(s): State<AppState>,
    Path((base, quote)): Path<(String, String)>,
) -> Result<Json<PublicQuote>, (StatusCode, String)> {
    s.providers
        .bank_of_canada_fx(&base, &quote)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_bank_of_canada_series(
    State(s): State<AppState>,
    Path(series): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    s.providers
        .bank_of_canada_series(&series)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_frankfurter_rate(
    State(s): State<AppState>,
    Path((base, quote)): Path<(String, String)>,
) -> Result<Json<PublicQuote>, (StatusCode, String)> {
    s.providers
        .frankfurter_rate(&base, &quote)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_frankfurter_rates(
    State(s): State<AppState>,
    Path((base, quotes)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let quote_list = quotes.split(',').map(str::to_owned).collect::<Vec<_>>();
    s.providers
        .frankfurter_rates(&base, &quote_list)
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


#[derive(Debug, Deserialize)]
struct NewsSearchQuery {
    q: String,
    ticker: Option<String>,
    limit: Option<usize>,
}

async fn api_news_search(
    State(s): State<AppState>,
    Query(query): Query<NewsSearchQuery>,
) -> Result<Json<Vec<NewsArticle>>, (StatusCode, String)> {
    s.news
        .search_all(
            &query.q,
            query.ticker.as_deref(),
            query.limit.unwrap_or(25),
        )
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_news_ticker(
    State(s): State<AppState>,
    Path(ticker): Path<String>,
) -> Result<Json<Vec<NewsArticle>>, (StatusCode, String)> {
    s.news
        .search_ticker(&ticker, 25)
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_news_reddit(
    State(s): State<AppState>,
    Query(query): Query<NewsSearchQuery>,
) -> Result<Json<Vec<NewsArticle>>, (StatusCode, String)> {
    let mut articles = s
        .news
        .reddit_rss_search(&query.q, query.ticker.as_deref(), query.limit.unwrap_or(25))
        .await
        .map_err(internal_error)?;
    if let Ok(mut json_articles) = s
        .news
        .reddit_json_search(&query.q, query.ticker.as_deref(), query.limit.unwrap_or(25))
        .await
    {
        articles.append(&mut json_articles);
    }
    Ok(Json(articles))
}

async fn api_news_google(
    State(s): State<AppState>,
    Query(query): Query<NewsSearchQuery>,
) -> Result<Json<Vec<NewsArticle>>, (StatusCode, String)> {
    s.news
        .google_news_search(&query.q, query.ticker.as_deref(), query.limit.unwrap_or(25))
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_news_newsapi(
    State(s): State<AppState>,
    Query(query): Query<NewsSearchQuery>,
) -> Result<Json<Vec<NewsArticle>>, (StatusCode, String)> {
    s.news
        .newsapi_search(&query.q, query.ticker.as_deref(), query.limit.unwrap_or(25))
        .await
        .map(Json)
        .map_err(internal_error)
}

async fn api_news_cache(State(s): State<AppState>) -> Json<Vec<NewsArticle>> {
    Json(s.news.cache().await)
}

async fn api_news_clusters(State(s): State<AppState>) -> Json<Vec<news::StoryCluster>> {
    Json(s.news.clusters().await)
}

async fn api_news_health(State(s): State<AppState>) -> Json<Vec<NewsProviderHealth>> {
    Json(s.news.health().await)
}

async fn list_presets(State(s): State<AppState>) -> Json<Vec<Preset>> { Json(s.presets.list().await) }

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
    let session = match quote.asset_class {
        providers::AssetClass::Equity => configured_live_session(),
        providers::AssetClass::Crypto | providers::AssetClass::Fx => MarketSession::Regular,
    };
    let symbol = quote.symbol.clone();
    let provider = quote.source;
    let venue = quote.venue.to_string();
    let ts_ms = quote.ts_ms;
    let updated = {
        let mut market = s.market.write().await;
        let state = market
            .entry(symbol.clone())
            .or_insert_with(|| SecurityState::blank(&symbol, quote.price, session, ts_ms));
        let old_volume = state.day_volume;

        state.asset_class = quote.asset_class;
        state.issue_type = match quote.issue_type {
            "common_stock" => IssueType::CommonStock,
            "etf" => IssueType::Etf,
            "adr" => IssueType::Adr,
            "reit" => IssueType::Reit,
            "etn" => IssueType::Etn,
            "warrant" => IssueType::Warrant,
            "preferred" => IssueType::Preferred,
            "right" => IssueType::Right,
            "unit" => IssueType::Unit,
            _ => IssueType::Other,
        };
        state.previous_close = quote.previous_close.or(state.previous_close);
        state.shares_float = quote.shares_float.or(state.shares_float);
        state.shares_outstanding = quote.shares_outstanding.or(state.shares_outstanding);
        state.market_cap = quote.market_cap.or(state.market_cap);
        state.session = session;

        if let Some(volume) = quote.volume {
            state.day_volume = volume.max(0.0);
        }

        let volume_delta = (state.day_volume - old_volume).max(0.0);
        state.last_updated_ms = ts_ms;
        minute_update(state, ts_ms, quote.price, volume_delta);
        update_venue(
            state,
            provider,
            &venue,
            Some(quote.price),
            None,
            None,
            quote.volume,
            ts_ms,
            None,
        );
        state.clone()
    };

    if let Ok(payload) = serde_json::to_value(&quote) {
        s.journal.append(JournalRecord {
            event_id: Uuid::new_v4().to_string(),
            received_at_ms: now_ms(),
            kind: "quote".to_string(),
            symbol: Some(symbol),
            provider: Some(provider.as_str().to_string()),
            venue: Some(venue),
            sequence: None,
            payload,
        });
    }
    publish_market_state(s, updated).await;
}

pub(crate) async fn ingest_book(
    s: &AppState,
    symbol: String,
    provider: providers::ProviderId,
    venue: String,
    first_sequence: Option<u64>,
    sequence: Option<u64>,
    snapshot: bool,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    ts_ms: i64,
) -> bool {
    let mid_hint = match (bids.first(), asks.first()) {
        (Some(bid), Some(ask)) => (bid.price + ask.price) / 2.0,
        (Some(bid), None) => bid.price,
        (None, Some(ask)) => ask.price,
        _ => 0.0,
    };
    let (updated, accepted) = {
        let mut market = s.market.write().await;
        let state = market
            .entry(symbol.clone())
            .or_insert_with(|| SecurityState::blank(&symbol, mid_hint, MarketSession::Regular, ts_ms));
        let key = venue.to_ascii_lowercase();
        let book = state.books.entry(key.clone()).or_default();
        let accepted = if snapshot {
            book.replace(bids.clone(), asks.clone(), sequence, ts_ms);
            true
        } else {
            book.apply_update_range(&bids, &asks, first_sequence, sequence, ts_ms)
        };

        let book_metrics = book.metrics();
        update_venue(
            state,
            provider,
            &venue,
            book_metrics.mid,
            book_metrics.best_bid,
            book_metrics.best_ask,
            None,
            ts_ms,
            sequence,
        );
        state.last_updated_ms = ts_ms;
        (state.clone(), accepted)
    };

    if let Ok(payload) = serde_json::to_value(serde_json::json!({
        "symbol": symbol,
        "provider": provider,
        "venue": venue,
        "first_sequence": first_sequence,
        "sequence": sequence,
        "snapshot": snapshot,
        "bids": bids,
        "asks": asks,
        "ts_ms": ts_ms
    })) {
        s.journal.append(JournalRecord {
            event_id: Uuid::new_v4().to_string(),
            received_at_ms: now_ms(),
            kind: "book".to_string(),
            symbol: Some(updated.symbol.clone()),
            provider: Some(provider.as_str().to_string()),
            venue: Some(venue),
            sequence,
            payload,
        });
    }

    publish_market_state(s, updated).await;
    accepted
}


pub(crate) async fn ingest_filing_events(s: &AppState, filings: &[FilingEvent]) {
    if filings.is_empty() {
        return;
    }

    let now = now_ms();
    let mut updates = Vec::new();
    {
        let mut market = s.market.write().await;
        for filing in filings {
            let symbol = filing.ticker.to_ascii_uppercase();
            let Some(state) = market.get_mut(&symbol) else { continue; };
            state.news_events.push_back((
                filing.acceptance_datetime.as_deref()
                    .and_then(news::parse_date_for_state)
                    .unwrap_or(now),
                format!("SEC:{}", filing.form),
            ));
            while let Some((timestamp, _)) = state.news_events.front() {
                if *timestamp < now - 60 * 60_000 {
                    state.news_events.pop_front();
                } else {
                    break;
                }
            }
            state.last_catalyst = Some(filing.event_type.clone());
            state.catalysts.push_back(CatalystEvent {
                id: Uuid::new_v4(),
                symbol: symbol.clone(),
                timestamp_ms: filing.acceptance_datetime.as_deref()
                    .and_then(news::parse_date_for_state)
                    .unwrap_or(now),
                category: filing.event_type.clone(),
                source: "SEC EDGAR".to_string(),
                confidence: 0.95,
                title: format!("SEC {} filing", filing.form),
                url: Some(filing.url.clone()),
            });
            while state.catalysts.len() > 100 { state.catalysts.pop_front(); }
            state.last_updated_ms = now.max(state.last_updated_ms);
            updates.push(state.clone());
        }
    }

    for state in updates {
        publish_market_state(s, state).await;
    }
}

pub(crate) async fn ingest_news_articles(s: &AppState, articles: &[NewsArticle]) {
    if articles.is_empty() {
        return;
    }

    let now = now_ms();
    let mut updates = Vec::new();
    {
        let mut market = s.market.write().await;
        for article in articles {
            let Some(ticker) = article.ticker.as_ref() else { continue; };
            let symbol = ticker.to_ascii_uppercase();
            let Some(state) = market.get_mut(&symbol) else { continue; };
            let timestamp = article.published_at_ms.unwrap_or(now);
            let source = article.source.clone().unwrap_or_else(|| article.provider.clone());

            state.news_events.push_back((timestamp, source));
            while let Some((timestamp, _)) = state.news_events.front() {
                if *timestamp < now - 60 * 60_000 {
                    state.news_events.pop_front();
                } else {
                    break;
                }
            }
            state.last_catalyst = Some(article.event_type.clone());
            state.catalysts.push_back(CatalystEvent {
                id: Uuid::new_v4(),
                symbol: symbol.clone(),
                timestamp_ms: timestamp,
                category: article.event_type.clone(),
                source: source.clone(),
                confidence: 0.60,
                title: article.title.clone(),
                url: Some(article.url.clone()),
            });
            while state.catalysts.len() > 100 { state.catalysts.pop_front(); }
            state.last_updated_ms = now.max(state.last_updated_ms);
            updates.push(state.clone());
        }
    }

    for article in articles {
        if let Ok(payload) = serde_json::to_value(article) {
            s.journal.append(JournalRecord {
                event_id: Uuid::new_v4().to_string(),
                received_at_ms: now,
                kind: "news".to_string(),
                symbol: article.ticker.clone(),
                provider: Some(article.provider.clone()),
                venue: article.source.clone(),
                sequence: None,
                payload,
            });
        }
    }

    for state in updates {
        publish_market_state(s, state).await;
    }
}

pub(crate) async fn ingest(
    s: &AppState,
    event: MarketEvent,
    provider: providers::ProviderId,
    venue: String,
    sequence: Option<u64>,
    side: Option<streams::TradeSide>,
) {
    let symbol = match &event {
        MarketEvent::Quote { symbol, .. }
        | MarketEvent::Trade { symbol, .. }
        | MarketEvent::Reference { symbol, .. } => symbol,
    };
    let updated = {
        let mut market = s.market.write().await;
        if let Some(state) = market.get_mut(symbol) {
            apply_event(state, &event, provider, &venue, sequence, side);
            Some(state.clone())
        } else {
            None
        }
    };
    let Some(updated) = updated else { return; };

    if let Ok(payload) = serde_json::to_value(&event) {
        let kind = match &event {
            MarketEvent::Quote { .. } => "quote",
            MarketEvent::Trade { .. } => "trade",
            MarketEvent::Reference { .. } => "reference",
        };
        s.journal.append(JournalRecord {
            event_id: Uuid::new_v4().to_string(),
            received_at_ms: now_ms(),
            kind: kind.to_string(),
            symbol: Some(symbol.clone()),
            provider: Some(provider.as_str().to_string()),
            venue: Some(venue),
            sequence,
            payload,
        });
    }

    publish_market_state(s, updated).await;
}

fn csv_env(name: &str, default_value: &str) -> Vec<String> {
    env::var(name)
        .unwrap_or_else(|_| default_value.to_string())
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}


async fn live_fx_feed(s: AppState) {
    let pairs = csv_env(
        "PINE_FOUNDRY_FX_PAIRS",
        "EURUSD=X,USDCAD=X,GBPUSD=X,USDJPY=X",
    );
    let poll_secs = env::var("PINE_FOUNDRY_FX_POLL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(15)
        .max(5);
    let mut interval = time::interval(Duration::from_secs(poll_secs));

    loop {
        interval.tick().await;
        for pair in &pairs {
            let mut quote = s.providers.yahoo_chart(pair, providers::AssetClass::Fx).await.ok().flatten();
            if quote.is_none() {
                if let Some(pair_name) = pair.strip_suffix("=X") {
                    if pair_name.len() >= 6 {
                        let (base, quote_ccy) = pair_name.split_at(3);
                        quote = match s.providers.bank_of_canada_fx(base, quote_ccy).await {
                            Ok(value) => Some(value),
                            Err(_) => s.providers.frankfurter_rate(base, quote_ccy).await.ok(),
                        };
                    }
                }
            }

            if let Some(quote) = quote {
                ingest_public_quote(&s, quote).await;
            }
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

        let mut canada_start = 0usize;
        while canada_start < max_rows {
            let canada_end = (canada_start + page_size).min(max_rows);
            match provider.tradingview_scan_canada(canada_start, canada_end).await {
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
            canada_start += page_size;
        }

        if delivered == 0 {
            let symbols: Vec<String> = {
                let market = s.market.read().await;
                market.keys().take(100).cloned().collect()
            };

            match provider.yahoo_spark(&symbols, providers::AssetClass::Equity).await {
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
            ingest(
                &s,
                MarketEvent::Trade {
                    symbol: symbol.clone(), ts_ms: ts, price, size, session: MarketSession::Regular,
                },
                providers::ProviderId::TradingView,
                "Mock".to_string(),
                Some(tick),
                if direction >= 0.0 { Some(streams::TradeSide::Buy) } else { Some(streams::TradeSide::Sell) },
            ).await;
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
    let journal = Arc::new(EventJournal::spawn(data_dir.join("events")));
    let news = Arc::new(NewsRouter::new().expect("public news client"));
    let filings = Arc::new(SecFilingRouter::new(journal.clone()).expect("SEC client"));
    let canada = Arc::new(CanadianDisclosureRouter::new(news.clone()));
    let stream_store = Arc::new(streams::StreamHealthStore::new());
    let state = AppState {
        market: Arc::new(RwLock::new(seed_market())),
        scans: Arc::new(RwLock::new(HashMap::new())),
        presets: Arc::new(PresetStore::load(data_dir.join("presets.json")).await),
        providers: provider,
        news,
        filings,
        canada,
        journal,
        streams: stream_store,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/providers", get(provider_routes))
        .route("/api/providers/health", get(provider_health))
        .route("/api/streams/health", get(stream_health))
        .route("/api/journal/health", get(journal_health))
        .route("/api/evidence/:symbol", get(symbol_evidence))
        .route("/api/catalysts/:symbol", get(catalysts_for_symbol))
        .route("/api/filings/health", get(filing_health))
        .route("/api/filings/:ticker", get(filing_search))
        .route("/api/canada/disclosures/:ticker", get(canadian_disclosure_search))
        .route("/api/providers/yahoo/:symbol", get(api_yahoo_quote))
        .route("/api/providers/yahoo/fx/:symbol", get(api_yahoo_fx))
        .route("/api/providers/tradingview/canada/:start/:end", get(api_tradingview_canada))
        .route("/api/providers/tradingview/forex/:start/:end", get(api_tradingview_fx))
        .route("/api/providers/nasdaq/:symbol", get(api_nasdaq_quote))
        .route("/api/providers/nasdaq/:symbol/info", get(api_nasdaq_info))
        .route("/api/providers/tradingview/:exchange/:symbol", get(api_tradingview_symbol))
        .route("/api/providers/binance/:symbol/ticker", get(api_binance_ticker))
        .route("/api/providers/binance/:symbol/klines/:interval", get(api_binance_klines))
        .route("/api/providers/binance/:symbol/depth", get(api_binance_depth))
        .route("/api/providers/binance/:symbol/quote", get(api_binance_normalized_quote))
        .route("/api/providers/kraken/:symbol/ticker", get(api_kraken_ticker))
        .route("/api/providers/kraken/:symbol/ohlc/:interval", get(api_kraken_ohlc))
        .route("/api/providers/kraken/:symbol/depth", get(api_kraken_depth))
        .route("/api/providers/kraken/:symbol/trades", get(api_kraken_trades))
        .route("/api/providers/coinbase/:symbol/ticker", get(api_coinbase_ticker))
        .route("/api/providers/coinbase/:symbol/candles/:granularity", get(api_coinbase_candles))
        .route("/api/providers/coinbase/:symbol/book", get(api_coinbase_book))
        .route("/api/providers/coinbase/:symbol/trades", get(api_coinbase_trades))
        .route("/api/providers/frankfurter/:base/:quote", get(api_frankfurter_rate))
        .route("/api/providers/frankfurter/:base/rates/:quotes", get(api_frankfurter_rates))
        .route("/api/providers/bank-of-canada/:base/:quote", get(api_bank_of_canada_fx))
        .route("/api/providers/bank-of-canada/series/:series", get(api_bank_of_canada_series))
        .route("/api/news/search", get(api_news_search))
        .route("/api/news/ticker/:ticker", get(api_news_ticker))
        .route("/api/news/reddit", get(api_news_reddit))
        .route("/api/news/google", get(api_news_google))
        .route("/api/news/newsapi", get(api_news_newsapi))
        .route("/api/news/cache", get(api_news_cache))
        .route("/api/news/clusters", get(api_news_clusters))
        .route("/api/news/health", get(api_news_health))
        .route("/api/presets", get(list_presets).post(create_preset))
        .route("/api/presets/:id", delete(delete_preset))
        .route("/api/scans", get(list_scans).post(create_scan))
        .route("/api/scans/:id", put(update_scan).delete(delete_scan))
        .route("/api/scans/:id/snapshot", get(scan_snapshot))
        .route("/ws/scanner/:id", get(scanner_ws))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    tokio::spawn(news::run_news_feed(state.news.clone(), state.clone()));
    tokio::spawn(filings::run_sec_feed(state.filings.clone(), state.clone()));
    tokio::spawn(canada::run_canadian_disclosure_feed(state.canada.clone(), state.clone()));
    match env::var("PINE_FOUNDRY_FEED").unwrap_or_else(|_| "auto".into()).to_ascii_lowercase().as_str() {
        "mock" => {
            tokio::spawn(mock_feed(state.clone()));
        }
        _ => {
            tokio::spawn(live_feed(state.clone()));
            tokio::spawn(streams::run_crypto_websocket_feeds(state.clone()));
            tokio::spawn(live_fx_feed(state.clone()));
        }
    }
    let addr: SocketAddr = address.parse().expect("PINE_FOUNDRY_ADDR must be host:port");
    println!("Pine Foundry listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await.expect("server");
}


fn parse_provider_id(value: Option<&str>) -> providers::ProviderId {
    match value.unwrap_or_default().to_ascii_lowercase().as_str() {
        "tradingview" => providers::ProviderId::TradingView,
        "yahoo" => providers::ProviderId::Yahoo,
        "nasdaq" => providers::ProviderId::Nasdaq,
        "kraken" => providers::ProviderId::Kraken,
        "coinbase" => providers::ProviderId::Coinbase,
        "frankfurter" => providers::ProviderId::Frankfurter,
        "bank_of_canada" => providers::ProviderId::BankOfCanada,
        _ => providers::ProviderId::Binance,
    }
}

async fn run_replay(date: String) {
    let data_dir = PathBuf::from(env::var("PINE_FOUNDRY_DATA_DIR").unwrap_or_else(|_| "data".into()));
    let records = match EventJournal::read_day(&data_dir.join("events"), &date).await {
        Ok(records) => records,
        Err(error) => {
            eprintln!("replay: {error}");
            return;
        }
    };

    let journal = Arc::new(EventJournal::spawn(data_dir.join("replay-events")));
    let replay_news = Arc::new(NewsRouter::new().expect("replay news client"));
    let state = AppState {
        market: Arc::new(RwLock::new(seed_market())),
        scans: Arc::new(RwLock::new(HashMap::new())),
        presets: Arc::new(PresetStore::load(data_dir.join("replay-presets.json")).await),
        providers: Arc::new(PublicProviderRouter::new().expect("public provider client")),
        news: replay_news.clone(),
        filings: Arc::new(SecFilingRouter::new(journal.clone()).expect("SEC client")),
        canada: Arc::new(CanadianDisclosureRouter::new(replay_news)),
        journal,
        streams: Arc::new(streams::StreamHealthStore::new()),
    };

    let mut counts = HashMap::<String, usize>::new();
    for record in records {
        *counts.entry(record.kind.clone()).or_default() += 1;
        match record.kind.as_str() {
            "trade" | "quote" | "reference" => {
                if let Ok(event) = serde_json::from_value::<MarketEvent>(record.payload) {
                    ingest(
                        &state,
                        event,
                        parse_provider_id(record.provider.as_deref()),
                        record.venue.unwrap_or_else(|| "replay".to_string()),
                        record.sequence,
                        None,
                    )
                    .await;
                }
            }
            "book" => {
                let symbol = record
                    .payload
                    .get("symbol")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();
                let bids = serde_json::from_value::<Vec<BookLevel>>(
                    record.payload.get("bids").cloned().unwrap_or_else(|| serde_json::json!([])),
                )
                .unwrap_or_default();
                let asks = serde_json::from_value::<Vec<BookLevel>>(
                    record.payload.get("asks").cloned().unwrap_or_else(|| serde_json::json!([])),
                )
                .unwrap_or_default();
                let ts_ms = record.payload.get("ts_ms").and_then(|value| value.as_i64()).unwrap_or(record.received_at_ms);
                let snapshot = record.payload.get("snapshot").and_then(|value| value.as_bool()).unwrap_or(true);
                ingest_book(
                    &state,
                    symbol,
                    parse_provider_id(record.provider.as_deref()),
                    record.venue.unwrap_or_else(|| "replay".to_string()),
                    record.payload.get("first_sequence").and_then(|value| value.as_u64()),
                    record.sequence,
                    snapshot,
                    bids,
                    asks,
                    ts_ms,
                )
                .await;
            }
            "news" | "canada_disclosure" => {
                if let Ok(article) = serde_json::from_value::<NewsArticle>(record.payload) {
                    ingest_news_articles(&state, &[article]).await;
                }
            }
            "filing" => {
                if let Ok(filing) = serde_json::from_value::<FilingEvent>(record.payload) {
                    ingest_filing_events(&state, &[filing]).await;
                }
            }
            _ => {}
        }
    }

    let mut rows = state.market.read().await.values().map(row).collect::<Vec<_>>();
    rows.sort_by(|a, b| b.change_pct_5m.partial_cmp(&a.change_pct_5m).unwrap_or(std::cmp::Ordering::Equal));
    println!("replayed {date}: {} records", counts.values().sum::<usize>());
    for (kind, count) in counts {
        println!("{kind}: {count}");
    }
    println!("top symbols:");
    for item in rows.into_iter().take(20) {
        println!(
            "{} price={} chg5m={:?} spread_bps={:?} book_imbalance={:?} news15m={} stream_age_ms={}",
            item.symbol,
            item.price,
            item.change_pct_5m,
            item.spread_bps,
            item.book_imbalance,
            item.news_count_15m,
            item.stream_age_ms
        );
    }
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
    let _ = dotenvy::dotenv();
    match Cli::parse().command.unwrap_or(Command::Serve) {
        Command::Serve => run_server().await,
        Command::Presets => print_presets(),
        Command::Providers => print_providers(),
        Command::Replay { date } => run_replay(date).await,
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
