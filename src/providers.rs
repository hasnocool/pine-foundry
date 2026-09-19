// src/providers.rs
use reqwest::{Client, Method};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderId {
    TradingView,
    Yahoo,
    Nasdaq,
    Binance,
}

impl ProviderId {
    fn as_str(self) -> &'static str {
        match self {
            Self::TradingView => "tradingview",
            Self::Yahoo => "yahoo",
            Self::Nasdaq => "nasdaq",
            Self::Binance => "binance",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderHealth {
    pub id: ProviderId,
    pub name: &'static str,
    pub auth_required: bool,
    pub status: &'static str,
    pub requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub last_success_ms: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicQuote {
    pub symbol: String,
    pub price: f64,
    pub previous_close: Option<f64>,
    pub change_pct: Option<f64>,
    pub volume: Option<f64>,
    pub market_cap: Option<f64>,
    pub shares_float: Option<f64>,
    pub shares_outstanding: Option<f64>,
    pub ts_ms: i64,
    pub session: String,
    pub source: ProviderId,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicProviderRoute {
    pub provider: ProviderId,
    pub route: &'static str,
    pub auth_required: bool,
    pub purpose: &'static str,
    pub scope: &'static str,
}

#[derive(Clone)]
pub struct PublicProviderRouter {
    client: Client,
    health: Arc<RwLock<HashMap<ProviderId, ProviderHealth>>>,
}

impl PublicProviderRouter {
    pub fn new() -> Result<Self, String> {
        let client = Client::builder()
            .user_agent("pine-foundry/0.1 public-market-data-client")
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(15))
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| format!("HTTP client: {e}"))?;

        let mut map = HashMap::new();
        map.insert(ProviderId::TradingView, health_for(ProviderId::TradingView));
        map.insert(ProviderId::Yahoo, health_for(ProviderId::Yahoo));
        map.insert(ProviderId::Nasdaq, health_for(ProviderId::Nasdaq));
        map.insert(ProviderId::Binance, health_for(ProviderId::Binance));

        Ok(Self {
            client,
            health: Arc::new(RwLock::new(map)),
        })
    }

    pub async fn health(&self) -> Vec<ProviderHealth> {
        let mut values: Vec<_> = self.health.read().await.values().cloned().collect();
        values.sort_by_key(|item| item.id.as_str());
        values
    }

    pub fn routes() -> Vec<PublicProviderRoute> {
        vec![
            PublicProviderRoute {
                provider: ProviderId::TradingView,
                route: "POST https://scanner.tradingview.com/america/scan",
                auth_required: false,
                purpose: "bulk U.S. equity screener/quotes",
                scope: "america",
            },
            PublicProviderRoute {
                provider: ProviderId::TradingView,
                route: "GET https://scanner.tradingview.com/symbol",
                auth_required: false,
                purpose: "single-symbol performance/technical fields",
                scope: "multi-market",
            },
            PublicProviderRoute {
                provider: ProviderId::Yahoo,
                route: "GET https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                auth_required: false,
                purpose: "intraday OHLCV/chart snapshots",
                scope: "global",
            },
            PublicProviderRoute {
                provider: ProviderId::Yahoo,
                route: "GET https://query1.finance.yahoo.com/v7/finance/spark",
                auth_required: false,
                purpose: "batch watchlist quote snapshots",
                scope: "global",
            },
            PublicProviderRoute {
                provider: ProviderId::Nasdaq,
                route: "GET https://api.nasdaq.com/api/quote/{symbol}/realtime?assetclass=stocks",
                auth_required: false,
                purpose: "U.S. stock real-time/near-real-time quote",
                scope: "NASDAQ/NYSE-facing",
            },
            PublicProviderRoute {
                provider: ProviderId::Nasdaq,
                route: "GET https://api.nasdaq.com/api/quote/{symbol}/info?assetclass=stocks",
                auth_required: false,
                purpose: "security metadata/reference information",
                scope: "NASDAQ/NYSE-facing",
            },
            PublicProviderRoute {
                provider: ProviderId::Binance,
                route: "GET https://data-api.binance.vision/api/v3/ticker/24hr",
                auth_required: false,
                purpose: "crypto 24h ticker",
                scope: "crypto",
            },
            PublicProviderRoute {
                provider: ProviderId::Binance,
                route: "GET https://data-api.binance.vision/api/v3/klines",
                auth_required: false,
                purpose: "crypto OHLCV candles",
                scope: "crypto",
            },
            PublicProviderRoute {
                provider: ProviderId::Binance,
                route: "GET https://data-api.binance.vision/api/v3/depth",
                auth_required: false,
                purpose: "crypto order book",
                scope: "crypto",
            },
        ]
    }

    async fn request_json(
        &self,
        provider: ProviderId,
        method: Method,
        url: &str,
        body: Option<Value>,
    ) -> Result<Value, String> {
        let mut last_error = String::from("unknown HTTP error");
        for attempt in 0..3 {
            let mut request = self.client.request(method.clone(), url);
            if let Some(ref value) = body {
                request = request.json(value);
            }
            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    match response.json::<Value>().await {
                        Ok(value) if status.is_success() => {
                            self.record_success(provider).await;
                            return Ok(value);
                        }
                        Ok(value) => {
                            last_error = format!("HTTP {status}: {}", value);
                        }
                        Err(error) => {
                            last_error = format!("HTTP {status}: {error}");
                        }
                    }
                }
                Err(error) => {
                    last_error = error.to_string();
                }
            }
            if attempt < 2 {
                tokio::time::sleep(std::time::Duration::from_millis(150 * (attempt + 1))).await;
            }
        }
        self.record_failure(provider, &last_error).await;
        Err(last_error)
    }

    async fn record_success(&self, provider: ProviderId) {
        let mut map = self.health.write().await;
        if let Some(item) = map.get_mut(&provider) {
            item.requests += 1;
            item.successes += 1;
            item.last_success_ms = Some(now_ms());
            item.last_error = None;
            item.status = "ok";
        }
    }

    async fn record_failure(&self, provider: ProviderId, error: &str) {
        let mut map = self.health.write().await;
        if let Some(item) = map.get_mut(&provider) {
            item.requests += 1;
            item.failures += 1;
            item.last_error = Some(error.chars().take(300).collect());
            item.status = "degraded";
        }
    }

    pub async fn tradingview_scan_us(
        &self,
        start: usize,
        end: usize,
    ) -> Result<Vec<PublicQuote>, String> {
        let columns = [
            "name",
            "close",
            "change",
            "change_abs",
            "volume",
            "market_cap_basic",
            "float_shares_outstanding",
            "type",
            "exchange",
        ];
        let payload = json!({
            "filter": [],
            "options": {"lang": "en"},
            "symbols": {"query": {"types": []}, "tickers": []},
            "columns": columns,
            "sort": {"sortBy": "volume", "sortOrder": "desc"},
            "range": [start, end]
        });
        let value = self.request_json(
            ProviderId::TradingView,
            Method::POST,
            "https://scanner.tradingview.com/america/scan",
            Some(payload),
        ).await?;

        let rows = value.get("data").and_then(Value::as_array)
            .ok_or_else(|| "TradingView response missing data".to_string())?;
        let now = now_ms();
        let mut quotes = Vec::with_capacity(rows.len());

        for row in rows {
            let symbol_full = row.get("s").and_then(Value::as_str).unwrap_or_default();
            let data = row.get("d").and_then(Value::as_array).cloned().unwrap_or_default();
            if symbol_full.is_empty() || data.len() < columns.len() {
                continue;
            }

            let get = |name: &str| -> Option<&Value> {
                let idx = columns.iter().position(|column| *column == name)?;
                data.get(idx)
            };

            let instrument_type = get("type").and_then(Value::as_str);
            if instrument_type.is_some_and(|kind| kind != "stock") {
                continue;
            }

            let symbol = symbol_full.rsplit(':').next().unwrap_or(symbol_full).to_string();
            let price = get("close").and_then(as_f64);
            let change_pct = get("change").and_then(as_f64);
            let Some(price) = price.filter(|value| value.is_finite() && *value > 0.0) else {
                continue;
            };

            let previous_close = change_pct
                .filter(|pct| pct.is_finite() && *pct > -99.999)
                .map(|pct| price / (1.0 + pct / 100.0));

            quotes.push(PublicQuote {
                symbol,
                price,
                previous_close,
                change_pct,
                volume: get("volume").and_then(as_f64),
                market_cap: get("market_cap_basic").and_then(as_f64),
                shares_float: get("float_shares_outstanding").and_then(as_f64),
                shares_outstanding: None,
                ts_ms: now,
                session: "regular".to_string(),
                source: ProviderId::TradingView,
            });
        }

        Ok(quotes)
    }

    pub async fn yahoo_chart(&self, symbol: &str) -> Result<Option<PublicQuote>, String> {
        let encoded = urlencoding::encode(symbol);
        let url = format!(
            "https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range=1d&interval=1m&includePrePost=true&events=div%2Csplits"
        );
        let value = self.request_json(ProviderId::Yahoo, Method::GET, &url, None).await?;
        let result = value.get("chart").and_then(|chart| chart.get("result"))
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .ok_or_else(|| "Yahoo chart missing result".to_string())?;

        let meta = result.get("meta").cloned().unwrap_or(Value::Null);
        let previous_close = meta.get("chartPreviousClose").and_then(as_f64)
            .or_else(|| meta.get("previousClose").and_then(as_f64));

        let timestamps = result.get("timestamp").and_then(Value::as_array).cloned().unwrap_or_default();
        let quote = result.get("indicators").and_then(|v| v.get("quote"))
            .and_then(Value::as_array).and_then(|items| items.first())
            .cloned().unwrap_or(Value::Null);
        let closes = quote.get("close").and_then(Value::as_array).cloned().unwrap_or_default();
        let volumes = quote.get("volume").and_then(Value::as_array).cloned().unwrap_or_default();

        let mut last_price = None;
        let mut volume = None;
        let mut timestamp = None;
        for index in (0..closes.len()).rev() {
            if let Some(price) = closes.get(index).and_then(as_f64) {
                last_price = Some(price);
                volume = volumes.get(index).and_then(as_f64);
                timestamp = timestamps.get(index).and_then(Value::as_i64).map(|s| s * 1000);
                break;
            }
        }
        let Some(price) = last_price else { return Ok(None); };

        Ok(Some(PublicQuote {
            symbol: symbol.to_string(),
            price,
            previous_close,
            change_pct: previous_close.map(|previous| (price / previous - 1.0) * 100.0),
            volume,
            market_cap: None,
            shares_float: None,
            shares_outstanding: None,
            ts_ms: timestamp.unwrap_or_else(now_ms),
            session: "regular".to_string(),
            source: ProviderId::Yahoo,
        }))
    }

    pub async fn yahoo_spark(&self, symbols: &[String]) -> Result<Vec<PublicQuote>, String> {
        if symbols.is_empty() {
            return Ok(vec![]);
        }
        let joined = symbols.join(",");
        let encoded = urlencoding::encode(&joined);
        let url = format!(
            "https://query1.finance.yahoo.com/v7/finance/spark?symbols={encoded}&range=1d&interval=1m"
        );
        let value = self.request_json(ProviderId::Yahoo, Method::GET, &url, None).await?;
        let results = value.get("spark").and_then(|v| v.get("result"))
            .and_then(Value::as_array)
            .ok_or_else(|| "Yahoo spark missing result".to_string())?;

        let mut quotes = Vec::with_capacity(results.len());
        for result in results {
            let meta = result.get("meta").cloned().unwrap_or(Value::Null);
            let symbol = meta.get("symbol").and_then(Value::as_str).unwrap_or_default();
            let price = meta.get("regularMarketPrice").and_then(as_f64);
            let previous_close = meta.get("chartPreviousClose").and_then(as_f64)
                .or_else(|| meta.get("previousClose").and_then(as_f64));
            let Some(price) = price.filter(|value| *value > 0.0) else { continue; };
            let timestamp = result.get("timestamp").and_then(Value::as_array)
                .and_then(|values| values.last())
                .and_then(Value::as_i64)
                .map(|seconds| seconds * 1000)
                .unwrap_or_else(now_ms);

            quotes.push(PublicQuote {
                symbol: symbol.to_string(),
                price,
                previous_close,
                change_pct: previous_close.map(|previous| (price / previous - 1.0) * 100.0),
                volume: None,
                market_cap: None,
                shares_float: None,
                shares_outstanding: None,
                ts_ms: timestamp,
                session: "regular".to_string(),
                source: ProviderId::Yahoo,
            });
        }
        Ok(quotes)
    }

    pub async fn nasdaq_quote(&self, symbol: &str) -> Result<Option<PublicQuote>, String> {
        let encoded = urlencoding::encode(symbol);
        let url = format!(
            "https://api.nasdaq.com/api/quote/{encoded}/realtime?assetclass=stocks"
        );
        let value = self.request_json(ProviderId::Nasdaq, Method::GET, &url, None).await?;
        let data = value.get("data").cloned().unwrap_or(Value::Null);
        let primary = data.get("primaryData").cloned().unwrap_or(data.clone());
        let price = primary.get("lastSalePrice").and_then(as_f64);
        let Some(price) = price.filter(|value| *value > 0.0) else { return Ok(None); };

        let change_pct = primary.get("percentageChange").and_then(as_f64)
            .or_else(|| primary.get("pctChange").and_then(as_f64));
        let volume = primary.get("volume").and_then(as_f64);
        let previous_close = change_pct
            .filter(|pct| *pct > -99.999)
            .map(|pct| price / (1.0 + pct / 100.0));

        Ok(Some(PublicQuote {
            symbol: symbol.to_string(),
            price,
            previous_close,
            change_pct,
            volume,
            market_cap: None,
            shares_float: None,
            shares_outstanding: None,
            ts_ms: now_ms(),
            session: "regular".to_string(),
            source: ProviderId::Nasdaq,
        }))
    }

    pub async fn nasdaq_info(&self, symbol: &str) -> Result<Value, String> {
        let encoded = urlencoding::encode(symbol);
        let url = format!("https://api.nasdaq.com/api/quote/{encoded}/info?assetclass=stocks");
        self.request_json(ProviderId::Nasdaq, Method::GET, &url, None).await
    }

    pub async fn tradingview_symbol(
        &self,
        exchange_symbol: &str,
        fields: &[&str],
    ) -> Result<Value, String> {
        let encoded_symbol = urlencoding::encode(exchange_symbol);
        let encoded_fields = urlencoding::encode(&fields.join(","));
        let url = format!(
            "https://scanner.tradingview.com/symbol?symbol={encoded_symbol}&fields={encoded_fields}&no_404=true"
        );
        self.request_json(ProviderId::TradingView, Method::GET, &url, None).await
    }

    pub async fn binance_24h(&self, symbol: &str) -> Result<Value, String> {
        let url = format!(
            "https://data-api.binance.vision/api/v3/ticker/24hr?symbol={}",
            symbol.to_ascii_uppercase()
        );
        self.request_json(ProviderId::Binance, Method::GET, &url, None).await
    }

    pub async fn binance_klines(
        &self,
        symbol: &str,
        interval: &str,
        limit: u16,
    ) -> Result<Value, String> {
        let url = format!(
            "https://data-api.binance.vision/api/v3/klines?symbol={}&interval={}&limit={}",
            symbol.to_ascii_uppercase(),
            interval,
            limit.min(1000)
        );
        self.request_json(ProviderId::Binance, Method::GET, &url, None).await
    }

    pub async fn binance_depth(&self, symbol: &str, limit: u16) -> Result<Value, String> {
        let url = format!(
            "https://data-api.binance.vision/api/v3/depth?symbol={}&limit={}",
            symbol.to_ascii_uppercase(),
            limit.min(5000)
        );
        self.request_json(ProviderId::Binance, Method::GET, &url, None).await
    }
}

fn health_for(id: ProviderId) -> ProviderHealth {
    let (name, auth_required) = match id {
        ProviderId::TradingView => ("TradingView public scanner", false),
        ProviderId::Yahoo => ("Yahoo Finance chart/spark", false),
        ProviderId::Nasdaq => ("Nasdaq public market API", false),
        ProviderId::Binance => ("Binance market-data-only host", false),
    };
    ProviderHealth {
        id,
        name,
        auth_required,
        status: "unprobed",
        requests: 0,
        successes: 0,
        failures: 0,
        last_success_ms: None,
        last_error: None,
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(string) => {
            let trimmed = string
                .trim()
                .trim_start_matches('$')
                .replace(',', "")
                .replace('%', "");
            trimmed.parse::<f64>().ok()
        }
        _ => None,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_millis() as i64
}
