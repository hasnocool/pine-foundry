// src/streams.rs
use crate::{
    ingest, ingest_book, ingest_public_quote, now_ms, providers, AppState, MarketEvent,
    MarketSession,
};
use crate::orderbook::BookLevel;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::HashMap, env, sync::Arc};
use tokio::{
    sync::RwLock,
    time::{sleep, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone, Copy)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamHealth {
    pub stream: String,
    pub connected: bool,
    pub stale: bool,
    pub last_message_ms: Option<i64>,
    pub last_trade_ms: Option<i64>,
    pub last_book_ms: Option<i64>,
    pub last_trade_sequence: Option<u64>,
    pub last_book_sequence: Option<u64>,
    pub reconnects: u64,
    pub trade_sequence_gap_count: u64,
    pub book_sequence_gap_count: u64,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct StreamHealthStore {
    inner: Arc<RwLock<HashMap<String, StreamHealth>>>,
}

impl StreamHealthStore {
    pub fn new() -> Self {
        let mut map = HashMap::new();
        for name in ["binance", "kraken", "coinbase"] {
            map.insert(
                name.to_string(),
                StreamHealth {
                    stream: name.to_string(),
                    connected: false,
                    stale: true,
                    last_message_ms: None,
                    last_trade_ms: None,
                    last_book_ms: None,
                    last_trade_sequence: None,
                    last_book_sequence: None,
                    reconnects: 0,
                    trade_sequence_gap_count: 0,
                    book_sequence_gap_count: 0,
                    last_error: None,
                },
            );
        }
        Self {
            inner: Arc::new(RwLock::new(map)),
        }
    }

    async fn connected(&self, name: &str) {
        let mut map = self.inner.write().await;
        if let Some(item) = map.get_mut(name) {
            item.connected = true;
            item.reconnects = item.reconnects.saturating_add(1);
            item.last_error = None;
        }
    }

    async fn disconnected(&self, name: &str, error: Option<String>) {
        let mut map = self.inner.write().await;
        if let Some(item) = map.get_mut(name) {
            item.connected = false;
            item.last_error = error;
        }
    }

    async fn message(&self, name: &str) {
        let mut map = self.inner.write().await;
        if let Some(item) = map.get_mut(name) {
            item.last_message_ms = Some(now_ms());
        }
    }

    async fn trade(&self, name: &str, ts_ms: i64, sequence: Option<u64>) {
        let mut map = self.inner.write().await;
        if let Some(item) = map.get_mut(name) {
            item.last_trade_ms = Some(ts_ms);
            item.last_message_ms = Some(now_ms());
            update_sequence(&mut item.last_trade_sequence, &mut item.trade_sequence_gap_count, sequence);
        }
    }

    async fn book(&self, name: &str, ts_ms: i64, sequence: Option<u64>) {
        let mut map = self.inner.write().await;
        if let Some(item) = map.get_mut(name) {
            item.last_book_ms = Some(ts_ms);
            item.last_message_ms = Some(now_ms());
            update_sequence(&mut item.last_book_sequence, &mut item.book_sequence_gap_count, sequence);
        }
    }

    pub async fn health(&self) -> Vec<StreamHealth> {
        let stale_after = env::var("PINE_FOUNDRY_STREAM_STALE_SECS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(10)
            .clamp(1, 3600)
            * 1000;
        let now = now_ms();
        let mut items = self.inner.read().await.values().cloned().collect::<Vec<_>>();
        for item in &mut items {
            item.stale = item
                .last_message_ms
                .map(|timestamp| now.saturating_sub(timestamp) > stale_after)
                .unwrap_or(true);
        }
        items.sort_by(|a, b| a.stream.cmp(&b.stream));
        items
    }
}

fn update_sequence(
    last_sequence: &mut Option<u64>,
    gap_count: &mut u64,
    sequence: Option<u64>,
) {
    if let Some(current) = sequence {
        if let Some(previous) = *last_sequence {
            if current <= previous || current > previous.saturating_add(1) {
                *gap_count = gap_count.saturating_add(1);
            }
        }
        if last_sequence.map(|previous| current > previous).unwrap_or(true) {
            *last_sequence = Some(current);
        }
    }
}

pub async fn run_crypto_websocket_feeds(state: AppState) {
    let _ = tokio::join!(
        run_binance(state.clone()),
        run_kraken(state.clone()),
        run_coinbase(state),
    );
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

fn reconnect_secs() -> u64 {
    env::var("PINE_FOUNDRY_CRYPTO_WS_RECONNECT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(3)
        .clamp(1, 60)
}

fn reconnect_delay(attempt: u32) -> Duration {
    let base = reconnect_secs().saturating_mul(1u64 << attempt.min(5));
    let capped = base.min(60);
    let jitter_ms = (now_ms().unsigned_abs() % 500) as u64;
    Duration::from_millis(capped * 1000 + jitter_ms)
}

fn kraken_pair(symbol: &str) -> String {
    let upper = symbol.to_ascii_uppercase();
    if let Some(base) = upper.strip_suffix("USDT") {
        format!("{base}/USD")
    } else if let Some(base) = upper.strip_suffix("USDC") {
        format!("{base}/USD")
    } else if upper.contains('/') {
        upper
    } else {
        upper
    }
}

fn coinbase_product(symbol: &str) -> String {
    let upper = symbol.to_ascii_uppercase();
    if let Some(base) = upper.strip_suffix("USDT") {
        format!("{base}-USD")
    } else if let Some(base) = upper.strip_suffix("USDC") {
        format!("{base}-USD")
    } else if upper.contains('-') {
        upper
    } else {
        upper
    }
}

fn wire_symbol_index(symbols: &[String]) -> HashMap<String, String> {
    symbols
        .iter()
        .map(|symbol| (symbol.to_ascii_uppercase(), symbol.clone()))
        .collect()
}

fn as_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(string) => string.parse::<f64>().ok(),
        _ => None,
    }
}

fn as_u64(value: Option<&Value>) -> Option<u64> {
    let value = value?;
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(string) => string.parse::<u64>().ok(),
        _ => None,
    }
}

fn timestamp_ms(value: Option<&Value>) -> i64 {
    if let Some(number) = as_f64(value) {
        return number as i64;
    }
    let Some(Value::String(value)) = value else {
        return now_ms();
    };
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.timestamp_millis())
        .unwrap_or_else(|_| now_ms())
}

fn parse_levels(value: Option<&Value>) -> Vec<BookLevel> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    levels
        .iter()
        .filter_map(|entry| {
            let array = entry.as_array()?;
            Some(BookLevel {
                price: as_f64(array.first())?,
                quantity: as_f64(array.get(1))?,
            })
        })
        .collect()
}

fn parse_object_levels(
    value: Option<&Value>,
    price_key: &str,
    quantity_key: &str,
) -> Vec<BookLevel> {
    let Some(levels) = value.and_then(Value::as_array) else {
        return Vec::new();
    };
    levels
        .iter()
        .filter_map(|entry| {
            Some(BookLevel {
                price: as_f64(entry.get(price_key))?,
                quantity: as_f64(entry.get(quantity_key))?,
            })
        })
        .collect()
}

async fn ensure_crypto_symbol(
    state: &AppState,
    symbol: &str,
    price: f64,
    source: providers::ProviderId,
    venue: &'static str,
) {
    if state.market.read().await.contains_key(symbol) {
        return;
    }
    ingest_public_quote(
        state,
        providers::PublicQuote {
            symbol: symbol.to_string(),
            asset_class: providers::AssetClass::Crypto,
            issue_type: "other",
            venue,
            price,
            previous_close: None,
            change_pct: None,
            volume: None,
            market_cap: None,
            shares_float: None,
            shares_outstanding: None,
            ts_ms: now_ms(),
            session: "regular".to_string(),
            source,
        },
    )
    .await;
}

async fn resync_book(state: &AppState, symbol: &str, provider: providers::ProviderId) {
    let (bids, asks, sequence) = match provider {
        providers::ProviderId::Binance => {
            let Ok(value) = state.providers.binance_depth(symbol, 100).await else { return; };
            (
                parse_levels(value.get("bids")),
                parse_levels(value.get("asks")),
                as_u64(value.get("lastUpdateId")),
            )
        }
        providers::ProviderId::Kraken => {
            let Ok(value) = state.providers.kraken_depth(symbol).await else { return; };
            let Some(result) = value.get("result").and_then(Value::as_object) else { return; };
            let Some(book) = result.values().next() else { return; };
            (
                parse_levels(book.get("bids")),
                parse_levels(book.get("asks")),
                None,
            )
        }
        providers::ProviderId::Coinbase => {
            let product = coinbase_product(symbol);
            let Ok(value) = state.providers.coinbase_book(&product).await else { return; };
            (
                parse_levels(value.get("bids")),
                parse_levels(value.get("asks")),
                as_u64(value.get("sequence")),
            )
        }
        _ => return,
    };

    if bids.is_empty() && asks.is_empty() {
        return;
    }

    let ts_ms = now_ms();
    let _ = ingest_book(
        state,
        symbol.to_string(),
        provider,
        match provider {
            providers::ProviderId::Binance => "Binance",
            providers::ProviderId::Kraken => "Kraken",
            providers::ProviderId::Coinbase => "Coinbase",
            _ => "Crypto",
        }.to_string(),
        None,
        sequence,
        true,
        bids,
        asks,
        ts_ms,
    ).await;
}

async fn ingest_crypto_trade(
    state: &AppState,
    symbol: String,
    ts_ms: i64,
    price: f64,
    size: f64,
    source: providers::ProviderId,
    venue: String,
    sequence: Option<u64>,
    side: Option<TradeSide>,
) {
    ensure_crypto_symbol(state, &symbol, price, source, match source {
        providers::ProviderId::Binance => "Binance WS",
        providers::ProviderId::Kraken => "Kraken WS",
        providers::ProviderId::Coinbase => "Coinbase WS",
        _ => "Crypto WS",
    })
    .await;

    ingest(
        state,
        MarketEvent::Trade {
            symbol,
            ts_ms,
            price,
            size: size.max(0.0),
            session: MarketSession::Regular,
        },
        source,
        venue,
        sequence,
        side,
    )
    .await;
}

async fn run_binance(state: AppState) {
    let symbols = csv_env(
        "PINE_FOUNDRY_CRYPTO_SYMBOLS",
        "BTCUSDT,ETHUSDT,SOLUSDT",
    );
    if symbols.is_empty() {
        return;
    }

    let mapping = wire_symbol_index(&symbols);
    let streams = symbols
        .iter()
        .flat_map(|symbol| {
            let lower = symbol.to_ascii_lowercase();
            [
                format!("{lower}@aggTrade"),
                format!("{lower}@depth@100ms"),
            ]
        })
        .collect::<Vec<_>>();
    let url = format!(
        "wss://stream.binance.com:9443/stream?streams={}",
        streams.join("/")
    );

    let mut reconnect_attempts = 0u32;
    loop {
        eprintln!("crypto websocket: connecting Binance");
        match connect_async(&url).await {
            Ok((mut socket, _)) => {
                reconnect_attempts = 0;
                state.streams.connected("binance").await;

                for symbol in &symbols {
                    resync_book(&state, symbol, providers::ProviderId::Binance).await;
                }

                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            state.streams.message("binance").await;
                            let Ok(value) = serde_json::from_str::<Value>(text.as_ref()) else {
                                continue;
                            };
                            let data = value.get("data").unwrap_or(&value);
                            let event_type = data.get("e").and_then(Value::as_str).unwrap_or_default();
                            let wire = data
                                .get("s")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_ascii_uppercase();
                            let symbol = mapping.get(&wire).cloned().unwrap_or(wire);

                            if event_type == "aggTrade" {
                                let Some(price) = as_f64(data.get("p")) else { continue; };
                                let size = as_f64(data.get("q")).unwrap_or(0.0);
                                let ts_ms = data.get("T").and_then(Value::as_i64).unwrap_or_else(now_ms);
                                let sequence = as_u64(data.get("a"));
                                let side = match data.get("m").and_then(Value::as_bool) {
                                    Some(true) => Some(TradeSide::Sell),
                                    Some(false) => Some(TradeSide::Buy),
                                    None => None,
                                };
                                state.streams.trade("binance", ts_ms, sequence).await;
                                ingest_crypto_trade(
                                    &state,
                                    symbol,
                                    ts_ms,
                                    price,
                                    size,
                                    providers::ProviderId::Binance,
                                    "Binance".to_string(),
                                    sequence,
                                    side,
                                )
                                .await;
                            } else if event_type == "depthUpdate" || data.get("lastUpdateId").is_some() {
                                let first_sequence = as_u64(data.get("U"));
                                let sequence = as_u64(data.get("u")).or_else(|| as_u64(data.get("lastUpdateId")));
                                let ts_ms = data.get("E").and_then(Value::as_i64).unwrap_or_else(now_ms);
                                let bids = parse_levels(data.get("b").or_else(|| data.get("bids")));
                                let asks = parse_levels(data.get("a").or_else(|| data.get("asks")));
                                state.streams.book("binance", ts_ms, sequence).await;
                                let accepted = ingest_book(
                                    &state,
                                    symbol.clone(),
                                    providers::ProviderId::Binance,
                                    "Binance".to_string(),
                                    first_sequence,
                                    sequence,
                                    false,
                                    bids,
                                    asks,
                                    ts_ms,
                                ).await;
                                if !accepted {
                                    resync_book(&state, &symbol, providers::ProviderId::Binance).await;
                                }
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Ok(_) => {}
                        Err(error) => {
                            state
                                .streams
                                .disconnected("binance", Some(error.to_string()))
                                .await;
                            break;
                        }
                    }
                }
                state
                    .streams
                    .disconnected("binance", Some("socket closed".to_string()))
                    .await;
            }
            Err(error) => {
                state
                    .streams
                    .disconnected("binance", Some(error.to_string()))
                    .await;
            }
        }

        reconnect_attempts = reconnect_attempts.saturating_add(1);
        sleep(reconnect_delay(reconnect_attempts)).await;
    }
}

async fn run_kraken(state: AppState) {
    let symbols = csv_env(
        "PINE_FOUNDRY_CRYPTO_SYMBOLS",
        "BTCUSDT,ETHUSDT,SOLUSDT",
    );
    if symbols.is_empty() {
        return;
    }

    let pairs = symbols
        .iter()
        .map(|symbol| kraken_pair(symbol))
        .collect::<Vec<_>>();
    let mapping = pairs
        .iter()
        .cloned()
        .zip(symbols.iter().cloned())
        .collect::<HashMap<_, _>>();

    let mut reconnect_attempts = 0u32;
    loop {
        eprintln!("crypto websocket: connecting Kraken");
        match connect_async("wss://ws.kraken.com/v2").await {
            Ok((mut socket, _)) => {
                reconnect_attempts = 0;
                state.streams.connected("kraken").await;
                let trade_subscription = json!({
                    "method": "subscribe",
                    "params": {
                        "channel": "trade",
                        "symbol": pairs.clone(),
                        "snapshot": false
                    }
                });
                let book_subscription = json!({
                    "method": "subscribe",
                    "params": {
                        "channel": "book",
                        "symbol": pairs.clone(),
                        "depth": 10,
                        "snapshot": true
                    }
                });

                if socket
                    .send(Message::Text(trade_subscription.to_string().into()))
                    .await
                    .is_err()
                    || socket
                        .send(Message::Text(book_subscription.to_string().into()))
                        .await
                        .is_err()
                {
                    state
                        .streams
                        .disconnected("kraken", Some("subscription send failed".to_string()))
                        .await;
                    sleep(reconnect_delay(reconnect_attempts)).await;
                    continue;
                }

                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            state.streams.message("kraken").await;
                            let Ok(value) = serde_json::from_str::<Value>(text.as_ref()) else {
                                continue;
                            };
                            let channel = value.get("channel").and_then(Value::as_str).unwrap_or_default();
                            let Some(data) = value.get("data").and_then(Value::as_array) else {
                                continue;
                            };

                            if channel == "trade" {
                                for trade in data {
                                    let pair = trade.get("symbol").and_then(Value::as_str).unwrap_or_default();
                                    let Some(symbol) = mapping.get(pair).cloned() else { continue; };
                                    let Some(price) = as_f64(trade.get("price")) else { continue; };
                                    let size = as_f64(trade.get("qty")).unwrap_or(0.0);
                                    let ts_ms = timestamp_ms(trade.get("timestamp"));
                                    let sequence = as_u64(trade.get("trade_id"));
                                    let side = match trade.get("side").and_then(Value::as_str) {
                                        Some("buy") => Some(TradeSide::Buy),
                                        Some("sell") => Some(TradeSide::Sell),
                                        _ => None,
                                    };
                                    state.streams.trade("kraken", ts_ms, sequence).await;
                                    ingest_crypto_trade(
                                        &state,
                                        symbol,
                                        ts_ms,
                                        price,
                                        size,
                                        providers::ProviderId::Kraken,
                                        "Kraken".to_string(),
                                        sequence,
                                        side,
                                    )
                                    .await;
                                }
                            } else if channel == "book" {
                                let snapshot =
                                    value.get("type").and_then(Value::as_str) == Some("snapshot");
                                for book in data {
                                    let pair = book.get("symbol").and_then(Value::as_str).unwrap_or_default();
                                    let Some(symbol) = mapping.get(pair).cloned() else { continue; };
                                    let ts_ms = timestamp_ms(book.get("timestamp"));
                                    let bids = parse_object_levels(book.get("bids"), "price", "qty");
                                    let asks = parse_object_levels(book.get("asks"), "price", "qty");
                                    state.streams.book("kraken", ts_ms, None).await;
                                    let accepted = ingest_book(
                                        &state,
                                        symbol.clone(),
                                        providers::ProviderId::Kraken,
                                        "Kraken".to_string(),
                                        None,
                                        None,
                                        snapshot,
                                        bids,
                                        asks,
                                        ts_ms,
                                    ).await;
                                    if !snapshot && !accepted {
                                        resync_book(&state, &symbol, providers::ProviderId::Kraken).await;
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Ok(_) => {}
                        Err(error) => {
                            state
                                .streams
                                .disconnected("kraken", Some(error.to_string()))
                                .await;
                            break;
                        }
                    }
                }
                state
                    .streams
                    .disconnected("kraken", Some("socket closed".to_string()))
                    .await;
            }
            Err(error) => {
                state
                    .streams
                    .disconnected("kraken", Some(error.to_string()))
                    .await;
            }
        }

        reconnect_attempts = reconnect_attempts.saturating_add(1);
        sleep(reconnect_delay(reconnect_attempts)).await;
    }
}

async fn run_coinbase(state: AppState) {
    let symbols = csv_env(
        "PINE_FOUNDRY_CRYPTO_SYMBOLS",
        "BTCUSDT,ETHUSDT,SOLUSDT",
    );
    if symbols.is_empty() {
        return;
    }

    let products = symbols
        .iter()
        .map(|symbol| coinbase_product(symbol))
        .collect::<Vec<_>>();
    let mapping = products
        .iter()
        .cloned()
        .zip(symbols.iter().cloned())
        .collect::<HashMap<_, _>>();

    let mut reconnect_attempts = 0u32;
    loop {
        eprintln!("crypto websocket: connecting Coinbase Advanced Trade");
        match connect_async("wss://advanced-trade-ws.coinbase.com").await {
            Ok((mut socket, _)) => {
                reconnect_attempts = 0;
                state.streams.connected("coinbase").await;
                let trade_subscription = json!({
                    "type": "subscribe",
                    "product_ids": products.clone(),
                    "channel": "market_trades"
                });
                let book_subscription = json!({
                    "type": "subscribe",
                    "product_ids": products.clone(),
                    "channel": "level2"
                });
                let heartbeat_subscription = json!({
                    "type": "subscribe",
                    "channel": "heartbeats"
                });

                if socket
                    .send(Message::Text(trade_subscription.to_string().into()))
                    .await
                    .is_err()
                    || socket
                        .send(Message::Text(book_subscription.to_string().into()))
                        .await
                        .is_err()
                {
                    state
                        .streams
                        .disconnected("coinbase", Some("subscription send failed".to_string()))
                        .await;
                    sleep(Duration::from_secs(reconnect_secs())).await;
                    continue;
                }
                let _ = socket
                    .send(Message::Text(heartbeat_subscription.to_string().into()))
                    .await;

                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            state.streams.message("coinbase").await;
                            let Ok(value) = serde_json::from_str::<Value>(text.as_ref()) else {
                                continue;
                            };
                            let channel = value.get("channel").and_then(Value::as_str).unwrap_or_default();

                            if channel == "market_trades" {
                                if let Some(events) = value.get("events").and_then(Value::as_array) {
                                    for event in events {
                                        let Some(trades) = event.get("trades").and_then(Value::as_array) else { continue; };
                                        for trade in trades {
                                            let product = trade.get("product_id").and_then(Value::as_str).unwrap_or_default();
                                            let Some(symbol) = mapping.get(product).cloned() else { continue; };
                                            let Some(price) = as_f64(trade.get("price")) else { continue; };
                                            let size = as_f64(trade.get("size")).unwrap_or(0.0);
                                            let ts_ms = timestamp_ms(trade.get("time"));
                                            let sequence = as_u64(trade.get("trade_id"));
                                            let side = match trade.get("side").and_then(Value::as_str) {
                                                Some("BUY") | Some("buy") => Some(TradeSide::Buy),
                                                Some("SELL") | Some("sell") => Some(TradeSide::Sell),
                                                _ => None,
                                            };
                                            state.streams.trade("coinbase", ts_ms, sequence).await;
                                            ingest_crypto_trade(
                                                &state,
                                                symbol,
                                                ts_ms,
                                                price,
                                                size,
                                                providers::ProviderId::Coinbase,
                                                "Coinbase".to_string(),
                                                sequence,
                                                side,
                                            )
                                            .await;
                                        }
                                    }
                                }
                            } else if channel == "l2_data" || channel == "level2" {
                                let sequence = as_u64(value.get("sequence_num"));
                                if let Some(events) = value.get("events").and_then(Value::as_array) {
                                    for event in events {
                                        let snapshot = event.get("type").and_then(Value::as_str) == Some("snapshot");
                                        let product = event.get("product_id").and_then(Value::as_str).unwrap_or_default();
                                        let Some(symbol) = mapping.get(product).cloned() else { continue; };
                                        let mut bids = Vec::new();
                                        let mut asks = Vec::new();
                                        if let Some(updates) = event.get("updates").and_then(Value::as_array) {
                                            for update in updates {
                                                let side = update.get("side").and_then(Value::as_str).unwrap_or_default();
                                                let level = BookLevel {
                                                    price: as_f64(update.get("price_level")).unwrap_or(0.0),
                                                    quantity: as_f64(update.get("new_quantity")).unwrap_or(0.0),
                                                };
                                                if side.eq_ignore_ascii_case("bid") {
                                                    bids.push(level);
                                                } else if side.eq_ignore_ascii_case("offer")
                                                    || side.eq_ignore_ascii_case("ask")
                                                {
                                                    asks.push(level);
                                                }
                                            }
                                        }
                                        let ts_ms = timestamp_ms(event.get("event_time"));
                                        state.streams.book("coinbase", ts_ms, None).await;
                                        let accepted = ingest_book(
                                            &state,
                                            symbol.clone(),
                                            providers::ProviderId::Coinbase,
                                            "Coinbase".to_string(),
                                            None,
                                            sequence,
                                            snapshot,
                                            bids,
                                            asks,
                                            ts_ms,
                                        ).await;
                                        if !snapshot && !accepted {
                                            resync_book(&state, &symbol, providers::ProviderId::Coinbase).await;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(payload)) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Ok(_) => {}
                        Err(error) => {
                            state
                                .streams
                                .disconnected("coinbase", Some(error.to_string()))
                                .await;
                            break;
                        }
                    }
                }
                state
                    .streams
                    .disconnected("coinbase", Some("socket closed".to_string()))
                    .await;
            }
            Err(error) => {
                state
                    .streams
                    .disconnected("coinbase", Some(error.to_string()))
                    .await;
            }
        }

        reconnect_attempts = reconnect_attempts.saturating_add(1);
        sleep(reconnect_delay(reconnect_attempts)).await;
    }
}
