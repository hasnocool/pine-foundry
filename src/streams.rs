// src/streams.rs
use crate::{
    ingest, ingest_public_quote, now_ms, providers, AppState, MarketEvent, MarketSession,
};
use chrono::DateTime;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashMap, env};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

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

fn timestamp_ms(value: Option<&Value>) -> i64 {
    if let Some(number) = as_f64(value) {
        return number as i64;
    }
    let Some(Value::String(value)) = value else {
        return now_ms();
    };
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.timestamp_millis())
        .unwrap_or_else(|_| now_ms())
}

async fn ingest_crypto_trade(
    state: &AppState,
    symbol: String,
    ts_ms: i64,
    price: f64,
    size: f64,
    source: providers::ProviderId,
    venue: &'static str,
) {
    let known = state.market.read().await.contains_key(&symbol);
    if !known {
        ingest_public_quote(
            state,
            providers::PublicQuote {
                symbol: symbol.clone(),
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
                ts_ms,
                session: "regular".to_string(),
                source,
            },
        )
        .await;
    }

    ingest(
        state,
        MarketEvent::Trade {
            symbol,
            ts_ms,
            price,
            size: size.max(0.0),
            session: MarketSession::Regular,
        },
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
        .map(|symbol| format!("{}@aggTrade", symbol.to_ascii_lowercase()))
        .collect::<Vec<_>>();
    let url = format!(
        "wss://stream.binance.com:9443/stream?streams={}",
        streams.join("/")
    );

    loop {
        eprintln!("crypto websocket: connecting Binance");
        match connect_async(&url).await {
            Ok((mut socket, _)) => {
                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            let Ok(value) = serde_json::from_str::<Value>(text.as_ref()) else {
                                continue;
                            };
                            let data = value.get("data").unwrap_or(&value);
                            if data.get("e").and_then(Value::as_str) != Some("aggTrade") {
                                continue;
                            }

                            let wire = data
                                .get("s")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_ascii_uppercase();
                            let symbol = mapping
                                .get(&wire)
                                .cloned()
                                .unwrap_or_else(|| wire.clone());
                            let Some(price) = as_f64(data.get("p")) else {
                                continue;
                            };
                            let size = as_f64(data.get("q")).unwrap_or(0.0);
                            let ts_ms = data
                                .get("T")
                                .and_then(Value::as_i64)
                                .unwrap_or_else(now_ms);

                            ingest_crypto_trade(
                                &state,
                                symbol,
                                ts_ms,
                                price,
                                size,
                                providers::ProviderId::Binance,
                                "Binance WS",
                            )
                            .await;
                        }
                        Ok(Message::Ping(payload)) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => break,
                        Ok(_) => {}
                        Err(error) => {
                            eprintln!("crypto websocket: Binance error: {error}");
                            break;
                        }
                    }
                }
            }
            Err(error) => eprintln!("crypto websocket: Binance connect error: {error}"),
        }

        sleep(Duration::from_secs(reconnect_secs())).await;
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

    loop {
        eprintln!("crypto websocket: connecting Kraken");
        match connect_async("wss://ws.kraken.com/v2").await {
            Ok((mut socket, _)) => {
                let subscription = json!({
                    "method": "subscribe",
                    "params": {
                        "channel": "trade",
                        "symbol": pairs.clone(),
                        "snapshot": false
                    }
                });
                if socket
                    .send(Message::Text(subscription.to_string().into()))
                    .await
                    .is_err()
                {
                    sleep(Duration::from_secs(reconnect_secs())).await;
                    continue;
                }

                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            let Ok(value) = serde_json::from_str::<Value>(text.as_ref()) else {
                                continue;
                            };
                            if value.get("channel").and_then(Value::as_str) != Some("trade") {
                                continue;
                            }
                            let Some(data) = value.get("data").and_then(Value::as_array) else {
                                continue;
                            };

                            for trade in data {
                                let pair = trade
                                    .get("symbol")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default();
                                let Some(symbol) = mapping.get(pair).cloned() else {
                                    continue;
                                };
                                let Some(price) = as_f64(trade.get("price")) else {
                                    continue;
                                };
                                let size = as_f64(trade.get("qty")).unwrap_or(0.0);
                                let ts_ms = timestamp_ms(trade.get("timestamp"));
                                ingest_crypto_trade(
                                    &state,
                                    symbol,
                                    ts_ms,
                                    price,
                                    size,
                                    providers::ProviderId::Kraken,
                                    "Kraken WS",
                                )
                                .await;
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
                            eprintln!("crypto websocket: Kraken error: {error}");
                            break;
                        }
                    }
                }
            }
            Err(error) => eprintln!("crypto websocket: Kraken connect error: {error}"),
        }

        sleep(Duration::from_secs(reconnect_secs())).await;
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

    loop {
        eprintln!("crypto websocket: connecting Coinbase Advanced Trade");
        match connect_async("wss://advanced-trade-ws.coinbase.com").await {
            Ok((mut socket, _)) => {
                let trade_subscription = json!({
                    "type": "subscribe",
                    "product_ids": products.clone(),
                    "channel": "market_trades"
                });
                let heartbeat_subscription = json!({
                    "type": "subscribe",
                    "channel": "heartbeats"
                });

                if socket
                    .send(Message::Text(trade_subscription.to_string().into()))
                    .await
                    .is_err()
                {
                    sleep(Duration::from_secs(reconnect_secs())).await;
                    continue;
                }
                let _ = socket
                    .send(Message::Text(heartbeat_subscription.to_string().into()))
                    .await;

                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            let Ok(value) = serde_json::from_str::<Value>(text.as_ref()) else {
                                continue;
                            };
                            if value.get("channel").and_then(Value::as_str)
                                != Some("market_trades")
                            {
                                continue;
                            }

                            let Some(events) = value.get("events").and_then(Value::as_array) else {
                                continue;
                            };
                            for event in events {
                                let Some(trades) =
                                    event.get("trades").and_then(Value::as_array)
                                else {
                                    continue;
                                };
                                for trade in trades {
                                    let product = trade
                                        .get("product_id")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default();
                                    let Some(symbol) = mapping.get(product).cloned() else {
                                        continue;
                                    };
                                    let Some(price) = as_f64(trade.get("price")) else {
                                        continue;
                                    };
                                    let size = as_f64(trade.get("size")).unwrap_or(0.0);
                                    let ts_ms = timestamp_ms(trade.get("time"));
                                    ingest_crypto_trade(
                                        &state,
                                        symbol,
                                        ts_ms,
                                        price,
                                        size,
                                        providers::ProviderId::Coinbase,
                                        "Coinbase WS",
                                    )
                                    .await;
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
                            eprintln!("crypto websocket: Coinbase error: {error}");
                            break;
                        }
                    }
                }
            }
            Err(error) => eprintln!("crypto websocket: Coinbase connect error: {error}"),
        }

        sleep(Duration::from_secs(reconnect_secs())).await;
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_common_exchange_symbols() {
        assert_eq!(kraken_pair("BTCUSDT"), "BTC/USD");
        assert_eq!(kraken_pair("ETHUSDC"), "ETH/USD");
        assert_eq!(coinbase_product("SOLUSDT"), "SOL-USD");
    }

    #[test]
    fn parses_rfc3339_timestamp() {
        let value = serde_json::json!("2026-09-19T12:34:56.000Z");
        assert_eq!(timestamp_ms(Some(&value)), 1_789_129_696_000);
    }
}
