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
    Kraken,
    Coinbase,
    Frankfurter,
    BankOfCanada,
}

impl ProviderId {
    fn as_str(self) -> &'static str {
        match self {
            Self::TradingView => "tradingview",
            Self::Yahoo => "yahoo",
            Self::Nasdaq => "nasdaq",
            Self::Binance => "binance",
            Self::Kraken => "kraken",
            Self::Coinbase => "coinbase",
            Self::Frankfurter => "frankfurter",
            Self::BankOfCanada => "bank_of_canada",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    Equity,
    Crypto,
    Fx,
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
    pub asset_class: AssetClass,
    pub issue_type: &'static str,
    pub venue: &'static str,
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
            .user_agent("pine-foundry/0.2 public-market-data-client")
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(15))
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| format!("HTTP client: {e}"))?;

        let ids = [
            ProviderId::TradingView,
            ProviderId::Yahoo,
            ProviderId::Nasdaq,
            ProviderId::Binance,
            ProviderId::Kraken,
            ProviderId::Coinbase,
            ProviderId::Frankfurter,
            ProviderId::BankOfCanada,
        ];
        let health = ids.into_iter().map(|id| (id, health_for(id))).collect();

        Ok(Self {
            client,
            health: Arc::new(RwLock::new(health)),
        })
    }

    pub async fn health(&self) -> Vec<ProviderHealth> {
        let mut values: Vec<_> = self.health.read().await.values().cloned().collect();
        values.sort_by_key(|item| item.id.as_str());
        values
    }

    pub fn routes() -> Vec<PublicProviderRoute> {
        vec![
            PublicProviderRoute { provider: ProviderId::TradingView, route: "POST https://scanner.tradingview.com/america/scan", auth_required: false, purpose: "bulk U.S. equity screener/quotes", scope: "U.S." },
            PublicProviderRoute { provider: ProviderId::TradingView, route: "POST https://scanner.tradingview.com/canada/scan", auth_required: false, purpose: "bulk TSX/TSXV/Canadian equity screener/quotes", scope: "Canada" },
            PublicProviderRoute { provider: ProviderId::TradingView, route: "POST https://scanner.tradingview.com/forex/scan", auth_required: false, purpose: "bulk FX screener/quotes", scope: "FX" },
            PublicProviderRoute { provider: ProviderId::TradingView, route: "GET https://scanner.tradingview.com/symbol", auth_required: false, purpose: "single-symbol metrics/technical fields", scope: "multi-market" },

            PublicProviderRoute { provider: ProviderId::Yahoo, route: "GET https://query1.finance.yahoo.com/v8/finance/chart/{symbol}", auth_required: false, purpose: "intraday OHLCV/chart snapshots", scope: "global" },
            PublicProviderRoute { provider: ProviderId::Yahoo, route: "GET https://query1.finance.yahoo.com/v7/finance/spark", auth_required: false, purpose: "batch watchlist quote snapshots", scope: "global" },

            PublicProviderRoute { provider: ProviderId::Nasdaq, route: "GET https://api.nasdaq.com/api/quote/{symbol}/realtime?assetclass=stocks", auth_required: false, purpose: "U.S. stock quote fallback", scope: "U.S." },
            PublicProviderRoute { provider: ProviderId::Nasdaq, route: "GET https://api.nasdaq.com/api/quote/{symbol}/info?assetclass=stocks", auth_required: false, purpose: "U.S. security reference information", scope: "U.S." },

            PublicProviderRoute { provider: ProviderId::Kraken, route: "GET https://api.kraken.com/0/public/Ticker?pair={symbol}", auth_required: false, purpose: "crypto ticker", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Kraken, route: "GET https://api.kraken.com/0/public/OHLC?pair={symbol}&interval=1", auth_required: false, purpose: "crypto OHLC", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Kraken, route: "GET https://api.kraken.com/0/public/Depth?pair={symbol}", auth_required: false, purpose: "crypto order book", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Kraken, route: "GET https://api.kraken.com/0/public/Trades?pair={symbol}", auth_required: false, purpose: "crypto recent trades", scope: "crypto" },

            PublicProviderRoute { provider: ProviderId::Coinbase, route: "GET https://api.exchange.coinbase.com/products/{product_id}/ticker", auth_required: false, purpose: "crypto ticker", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Coinbase, route: "GET https://api.exchange.coinbase.com/products/{product_id}/candles", auth_required: false, purpose: "crypto OHLC", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Coinbase, route: "GET https://api.exchange.coinbase.com/products/{product_id}/book?level=2", auth_required: false, purpose: "crypto order book", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Coinbase, route: "GET https://api.exchange.coinbase.com/products/{product_id}/trades", auth_required: false, purpose: "crypto recent trades", scope: "crypto" },

            PublicProviderRoute { provider: ProviderId::Binance, route: "GET https://data-api.binance.vision/api/v3/ticker/24hr", auth_required: false, purpose: "crypto 24h ticker", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Binance, route: "GET https://data-api.binance.vision/api/v3/klines", auth_required: false, purpose: "crypto OHLCV candles", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Binance, route: "GET https://data-api.binance.vision/api/v3/depth", auth_required: false, purpose: "crypto order book", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Binance, route: "WSS wss://stream.binance.com:9443/stream?streams=<symbol>@aggTrade", auth_required: false, purpose: "event-driven aggregate crypto trades", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Kraken, route: "WSS wss://ws.kraken.com/v2 channel=trade", auth_required: false, purpose: "event-driven crypto trades", scope: "crypto" },
            PublicProviderRoute { provider: ProviderId::Coinbase, route: "WSS wss://advanced-trade-ws.coinbase.com channel=market_trades", auth_required: false, purpose: "event-driven public crypto trades", scope: "crypto" },

            PublicProviderRoute { provider: ProviderId::Frankfurter, route: "GET https://api.frankfurter.dev/v2/rate/{base}/{quote}", auth_required: false, purpose: "daily FX reference rate", scope: "FX" },
            PublicProviderRoute { provider: ProviderId::Frankfurter, route: "GET https://api.frankfurter.dev/v2/rates", auth_required: false, purpose: "daily FX reference rates", scope: "FX" },
            PublicProviderRoute { provider: ProviderId::BankOfCanada, route: "GET https://www.bankofcanada.ca/valet/observations/{series}/json", auth_required: false, purpose: "official Canadian FX/economic observations", scope: "Canada/FX" },
            PublicProviderRoute { provider: ProviderId::BankOfCanada, route: "GET https://www.bankofcanada.ca/valet/fx_rss", auth_required: false, purpose: "Canadian FX RSS observations", scope: "Canada/FX" },
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

            match provider {
                ProviderId::Nasdaq => {
                    request = request
                        .header("accept", "application/json, text/plain, */*")
                        .header("accept-language", "en-US,en;q=0.9")
                        .header("origin", "https://www.nasdaq.com")
                        .header("referer", "https://www.nasdaq.com/");
                }
                ProviderId::TradingView => {
                    request = request
                        .header("accept", "application/json, text/plain, */*")
                        .header("origin", "https://www.tradingview.com")
                        .header("referer", "https://www.tradingview.com/");
                }
                _ => {}
            }

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
                            last_error = format!("HTTP {status}: {value}");
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

    pub async fn tradingview_scan_market(
        &self,
        market: &str,
        start: usize,
        end: usize,
        asset_class: AssetClass,
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
            "options": { "lang": "en" },
            "symbols": { "query": { "types": [] }, "tickers": [] },
            "columns": columns,
            "sort": { "sortBy": "volume", "sortOrder": "desc" },
            "range": [start, end]
        });

        let url = format!("https://scanner.tradingview.com/{market}/scan");
        let value = self
            .request_json(ProviderId::TradingView, Method::POST, &url, Some(payload))
            .await?;

        let rows = value
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| "TradingView response missing data".to_string())?;

        let now = now_ms();
        let mut quotes = Vec::with_capacity(rows.len());

        for row in rows {
            let symbol_full = row.get("s").and_then(Value::as_str).unwrap_or_default();
            let data = row
                .get("d")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();

            if symbol_full.is_empty() || data.len() < columns.len() {
                continue;
            }

            let get = |name: &str| -> Option<&Value> {
                let index = columns.iter().position(|column| *column == name)?;
                data.get(index)
            };

            if asset_class == AssetClass::Equity
                && get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind != "stock")
            {
                continue;
            }

            let symbol = canonical_symbol(market, symbol_full);
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
                asset_class,
                issue_type: if asset_class == AssetClass::Equity {
                    "common_stock"
                } else {
                    "other"
                },
                venue: if market == "canada" { "TSX/TSXV" } else { market },
                price,
                previous_close,
                change_pct,
                volume: get("volume").and_then(as_f64),
                market_cap: get("market_cap_basic").and_then(as_f64),
                shares_float: get("float_shares_outstanding").and_then(as_f64),
                shares_outstanding: None,
                ts_ms: now,
                session: session_for(asset_class),
                source: ProviderId::TradingView,
            });
        }

        Ok(quotes)
    }

    pub async fn tradingview_scan_us(
        &self,
        start: usize,
        end: usize,
    ) -> Result<Vec<PublicQuote>, String> {
        self.tradingview_scan_market("america", start, end, AssetClass::Equity)
            .await
    }

    pub async fn tradingview_scan_canada(
        &self,
        start: usize,
        end: usize,
    ) -> Result<Vec<PublicQuote>, String> {
        self.tradingview_scan_market("canada", start, end, AssetClass::Equity)
            .await
    }

    pub async fn tradingview_scan_fx(
        &self,
        start: usize,
        end: usize,
    ) -> Result<Vec<PublicQuote>, String> {
        self.tradingview_scan_market("forex", start, end, AssetClass::Fx)
            .await
    }

    pub async fn yahoo_chart(
        &self,
        symbol: &str,
        asset_class: AssetClass,
    ) -> Result<Option<PublicQuote>, String> {
        let encoded = urlencoding::encode(symbol);
        let url = format!(
            "https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range=1d&interval=1m&includePrePost=true&events=div%2Csplits"
        );

        let value = self
            .request_json(ProviderId::Yahoo, Method::GET, &url, None)
            .await?;

        let result = value
            .get("chart")
            .and_then(|chart| chart.get("result"))
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .ok_or_else(|| "Yahoo chart missing result".to_string())?;

        let meta = result.get("meta").cloned().unwrap_or(Value::Null);
        let previous_close = meta
            .get("chartPreviousClose")
            .and_then(as_f64)
            .or_else(|| meta.get("previousClose").and_then(as_f64));

        let timestamps = result
            .get("timestamp")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let quote = result
            .get("indicators")
            .and_then(|v| v.get("quote"))
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .cloned()
            .unwrap_or(Value::Null);
        let closes = quote
            .get("close")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let volumes = quote
            .get("volume")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        let mut last_price = None;
        let mut volume = None;
        let mut timestamp = None;

        for index in (0..closes.len()).rev() {
            if let Some(price) = closes.get(index).and_then(as_f64) {
                last_price = Some(price);
                volume = volumes.get(index).and_then(as_f64);
                timestamp = timestamps
                    .get(index)
                    .and_then(Value::as_i64)
                    .map(|seconds| seconds * 1000);
                break;
            }
        }

        let Some(price) = last_price else {
            return Ok(None);
        };

        Ok(Some(PublicQuote {
            symbol: symbol.to_string(),
            asset_class,
            issue_type: if asset_class == AssetClass::Equity {
                "common_stock"
            } else {
                "other"
            },
            venue: yahoo_venue(asset_class),
            price,
            previous_close,
            change_pct: previous_close.map(|previous| (price / previous - 1.0) * 100.0),
            volume,
            market_cap: None,
            shares_float: None,
            shares_outstanding: None,
            ts_ms: timestamp.unwrap_or_else(now_ms),
            session: session_for(asset_class),
            source: ProviderId::Yahoo,
        }))
    }

    pub async fn yahoo_spark(
        &self,
        symbols: &[String],
        asset_class: AssetClass,
    ) -> Result<Vec<PublicQuote>, String> {
        if symbols.is_empty() {
            return Ok(vec![]);
        }

        let joined = symbols.join(",");
        let encoded = urlencoding::encode(&joined);
        let url = format!(
            "https://query1.finance.yahoo.com/v7/finance/spark?symbols={encoded}&range=1d&interval=1m"
        );

        let value = self
            .request_json(ProviderId::Yahoo, Method::GET, &url, None)
            .await?;
        let results = value
            .get("spark")
            .and_then(|v| v.get("result"))
            .and_then(Value::as_array)
            .ok_or_else(|| "Yahoo spark missing result".to_string())?;

        let mut quotes = Vec::with_capacity(results.len());

        for result in results {
            let meta = result.get("meta").cloned().unwrap_or(Value::Null);
            let symbol = meta
                .get("symbol")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let price = meta.get("regularMarketPrice").and_then(as_f64);
            let previous_close = meta
                .get("chartPreviousClose")
                .and_then(as_f64)
                .or_else(|| meta.get("previousClose").and_then(as_f64));

            let Some(price) = price.filter(|value| *value > 0.0) else {
                continue;
            };

            let timestamp = result
                .get("timestamp")
                .and_then(Value::as_array)
                .and_then(|values| values.last())
                .and_then(Value::as_i64)
                .map(|seconds| seconds * 1000)
                .unwrap_or_else(now_ms);

            quotes.push(PublicQuote {
                symbol: symbol.to_string(),
                asset_class,
                issue_type: if asset_class == AssetClass::Equity {
                    "common_stock"
                } else {
                    "other"
                },
                venue: yahoo_venue(asset_class),
                price,
                previous_close,
                change_pct: previous_close.map(|previous| (price / previous - 1.0) * 100.0),
                volume: None,
                market_cap: None,
                shares_float: None,
                shares_outstanding: None,
                ts_ms: timestamp,
                session: session_for(asset_class),
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

        let value = self
            .request_json(ProviderId::Nasdaq, Method::GET, &url, None)
            .await?;
        let data = value.get("data").cloned().unwrap_or(Value::Null);
        let primary = data.get("primaryData").cloned().unwrap_or(data);
        let price = primary.get("lastSalePrice").and_then(as_f64);

        let Some(price) = price.filter(|value| *value > 0.0) else {
            return Ok(None);
        };

        let change_pct = primary
            .get("percentageChange")
            .and_then(as_f64)
            .or_else(|| primary.get("pctChange").and_then(as_f64));
        let volume = primary.get("volume").and_then(as_f64);
        let previous_close = change_pct
            .filter(|pct| *pct > -99.999)
            .map(|pct| price / (1.0 + pct / 100.0));

        Ok(Some(PublicQuote {
            symbol: symbol.to_string(),
            asset_class: AssetClass::Equity,
            issue_type: "common_stock",
            venue: "NASDAQ/NYSE",
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
        let url = format!(
            "https://api.nasdaq.com/api/quote/{encoded}/info?assetclass=stocks"
        );
        self.request_json(ProviderId::Nasdaq, Method::GET, &url, None)
            .await
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
        self.request_json(ProviderId::TradingView, Method::GET, &url, None)
            .await
    }

    pub async fn binance_quote(&self, symbol: &str) -> Result<PublicQuote, String> {
        let value = self.binance_24h(symbol).await?;
        let price = value.get("lastPrice").and_then(as_f64)
            .ok_or_else(|| "Binance ticker missing lastPrice".to_string())?;
        let change_pct = value.get("priceChangePercent").and_then(as_f64);
        let previous_close = change_pct
            .filter(|pct| *pct > -99.999)
            .map(|pct| price / (1.0 + pct / 100.0));
        let volume = value.get("volume").and_then(as_f64);

        Ok(PublicQuote {
            symbol: symbol.to_ascii_uppercase(),
            asset_class: AssetClass::Crypto,
            issue_type: "other",
            venue: "Binance",
            price,
            previous_close,
            change_pct,
            volume,
            market_cap: None,
            shares_float: None,
            shares_outstanding: None,
            ts_ms: now_ms(),
            session: "regular".to_string(),
            source: ProviderId::Binance,
        })
    }

    pub async fn binance_24h(&self, symbol: &str) -> Result<Value, String> {
        let url = format!(
            "https://data-api.binance.vision/api/v3/ticker/24hr?symbol={}",
            symbol.to_ascii_uppercase()
        );
        self.request_json(ProviderId::Binance, Method::GET, &url, None)
            .await
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
        self.request_json(ProviderId::Binance, Method::GET, &url, None)
            .await
    }

    pub async fn binance_depth(&self, symbol: &str, limit: u16) -> Result<Value, String> {
        let url = format!(
            "https://data-api.binance.vision/api/v3/depth?symbol={}&limit={}",
            symbol.to_ascii_uppercase(),
            limit.min(5000)
        );
        self.request_json(ProviderId::Binance, Method::GET, &url, None)
            .await
    }

    pub async fn kraken_ticker(&self, symbol: &str) -> Result<PublicQuote, String> {
        let pair = urlencoding::encode(symbol);
        let url = format!("https://api.kraken.com/0/public/Ticker?pair={pair}");
        let value = self
            .request_json(ProviderId::Kraken, Method::GET, &url, None)
            .await?;
        let result = value
            .get("result")
            .and_then(Value::as_object)
            .ok_or_else(|| "Kraken ticker missing result".to_string())?;
        let (_, ticker) = result
            .iter()
            .next()
            .ok_or_else(|| "Kraken ticker returned no pair".to_string())?;

        let ask = ticker
            .get("a")
            .and_then(Value::as_array)
            .and_then(|v| v.first())
            .and_then(as_f64)
            .ok_or_else(|| "Kraken ticker missing ask".to_string())?;
        let volume_24h = ticker
            .get("v")
            .and_then(Value::as_array)
            .and_then(|v| v.last())
            .and_then(as_f64);

        Ok(PublicQuote {
            symbol: symbol.to_ascii_uppercase(),
            asset_class: AssetClass::Crypto,
            issue_type: "other",
            venue: "Kraken",
            price: ask,
            previous_close: None,
            change_pct: None,
            volume: volume_24h,
            market_cap: None,
            shares_float: None,
            shares_outstanding: None,
            ts_ms: now_ms(),
            session: "regular".to_string(),
            source: ProviderId::Kraken,
        })
    }

    pub async fn kraken_ohlc(&self, symbol: &str, interval: u16) -> Result<Value, String> {
        let pair = urlencoding::encode(symbol);
        let url = format!(
            "https://api.kraken.com/0/public/OHLC?pair={pair}&interval={}",
            interval.max(1)
        );
        self.request_json(ProviderId::Kraken, Method::GET, &url, None)
            .await
    }

    pub async fn kraken_depth(&self, symbol: &str) -> Result<Value, String> {
        let pair = urlencoding::encode(symbol);
        let url = format!("https://api.kraken.com/0/public/Depth?pair={pair}");
        self.request_json(ProviderId::Kraken, Method::GET, &url, None)
            .await
    }

    pub async fn kraken_trades(&self, symbol: &str) -> Result<Value, String> {
        let pair = urlencoding::encode(symbol);
        let url = format!("https://api.kraken.com/0/public/Trades?pair={pair}");
        self.request_json(ProviderId::Kraken, Method::GET, &url, None)
            .await
    }

    pub async fn coinbase_ticker(&self, product_id: &str) -> Result<PublicQuote, String> {
        let product = urlencoding::encode(&product_id.to_ascii_uppercase());
        let url = format!("https://api.exchange.coinbase.com/products/{product}/ticker");
        let value = self
            .request_json(ProviderId::Coinbase, Method::GET, &url, None)
            .await?;

        let price = value
            .get("price")
            .and_then(as_f64)
            .ok_or_else(|| "Coinbase ticker missing price".to_string())?;
        let volume = value.get("volume").and_then(as_f64);

        Ok(PublicQuote {
            symbol: product_id.to_ascii_uppercase(),
            asset_class: AssetClass::Crypto,
            issue_type: "other",
            venue: "Coinbase",
            price,
            previous_close: None,
            change_pct: None,
            volume,
            market_cap: None,
            shares_float: None,
            shares_outstanding: None,
            ts_ms: now_ms(),
            session: "regular".to_string(),
            source: ProviderId::Coinbase,
        })
    }

    pub async fn coinbase_candles(
        &self,
        product_id: &str,
        granularity: u32,
    ) -> Result<Value, String> {
        let product = urlencoding::encode(&product_id.to_ascii_uppercase());
        let url = format!(
            "https://api.exchange.coinbase.com/products/{product}/candles?granularity={}",
            granularity
        );
        self.request_json(ProviderId::Coinbase, Method::GET, &url, None)
            .await
    }

    pub async fn coinbase_book(&self, product_id: &str) -> Result<Value, String> {
        let product = urlencoding::encode(&product_id.to_ascii_uppercase());
        let url = format!(
            "https://api.exchange.coinbase.com/products/{product}/book?level=2"
        );
        self.request_json(ProviderId::Coinbase, Method::GET, &url, None)
            .await
    }

    pub async fn coinbase_trades(&self, product_id: &str) -> Result<Value, String> {
        let product = urlencoding::encode(&product_id.to_ascii_uppercase());
        let url = format!(
            "https://api.exchange.coinbase.com/products/{product}/trades?limit=1000"
        );
        self.request_json(ProviderId::Coinbase, Method::GET, &url, None)
            .await
    }

    pub async fn bank_of_canada_series(
        &self,
        series: &str,
    ) -> Result<Value, String> {
        let encoded = urlencoding::encode(series);
        let url = format!(
            "https://www.bankofcanada.ca/valet/observations/{encoded}/json"
        );
        self.request_json(ProviderId::BankOfCanada, Method::GET, &url, None)
            .await
    }

    pub async fn bank_of_canada_fx(
        &self,
        base: &str,
        quote: &str,
    ) -> Result<PublicQuote, String> {
        let base = base.to_ascii_uppercase();
        let quote_ccy = quote.to_ascii_uppercase();
        let value_for = |code: &str| async move {
            let series = format!("FX{code}CAD");
            let value = self.bank_of_canada_series(&series).await?;
            latest_observation_value(&value, &series)
        };

        let rate = if base == "CAD" {
            let foreign_to_cad = value_for(&quote_ccy).await?;
            1.0 / foreign_to_cad
        } else if quote_ccy == "CAD" {
            value_for(&base).await?
        } else {
            let base_to_cad = value_for(&base).await?;
            let quote_to_cad = value_for(&quote_ccy).await?;
            base_to_cad / quote_to_cad
        };

        Ok(PublicQuote {
            symbol: format!("{base}{quote_ccy}"),
            asset_class: AssetClass::Fx,
            issue_type: "other",
            venue: "Bank of Canada",
            price: rate,
            previous_close: None,
            change_pct: None,
            volume: None,
            market_cap: None,
            shares_float: None,
            shares_outstanding: None,
            ts_ms: now_ms(),
            session: "regular".to_string(),
            source: ProviderId::BankOfCanada,
        })
    }

    pub async fn frankfurter_rate(
        &self,
        base: &str,
        quote: &str,
    ) -> Result<PublicQuote, String> {
        let base = base.to_ascii_uppercase();
        let quote_ccy = quote.to_ascii_uppercase();
        let url = format!(
            "https://api.frankfurter.dev/v2/rate/{}/{}",
            base, quote_ccy
        );
        let value = self
            .request_json(ProviderId::Frankfurter, Method::GET, &url, None)
            .await?;

        let rate = value
            .get("rate")
            .and_then(as_f64)
            .ok_or_else(|| "Frankfurter rate missing rate".to_string())?;

        Ok(PublicQuote {
            symbol: format!("{base}{quote_ccy}"),
            asset_class: AssetClass::Fx,
            issue_type: "other",
            venue: "Frankfurter",
            price: rate,
            previous_close: None,
            change_pct: None,
            volume: None,
            market_cap: None,
            shares_float: None,
            shares_outstanding: None,
            ts_ms: now_ms(),
            session: "regular".to_string(),
            source: ProviderId::Frankfurter,
        })
    }

    pub async fn frankfurter_rates(
        &self,
        base: &str,
        quotes: &[String],
    ) -> Result<Value, String> {
        let base = base.to_ascii_uppercase();
        let encoded_quotes = urlencoding::encode(&quotes.join(","));
        let url = format!(
            "https://api.frankfurter.dev/v2/rates?base={base}&quotes={encoded_quotes}"
        );
        self.request_json(ProviderId::Frankfurter, Method::GET, &url, None)
            .await
    }
}

fn latest_observation_value(value: &Value, series: &str) -> Result<f64, String> {
    let observations = value
        .get("observations")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("Bank of Canada series {series} missing observations"))?;

    for observation in observations.iter().rev() {
        if let Some(value) = observation
            .get(series.to_ascii_uppercase())
            .or_else(|| observation.get(series))
            .and_then(as_f64)
        {
            return Ok(value);
        }
    }

    Err(format!("Bank of Canada series {series} contains no numeric latest observation"))
}

fn health_for(id: ProviderId) -> ProviderHealth {
    let (name, auth_required) = match id {
        ProviderId::TradingView => ("TradingView public scanner", false),
        ProviderId::Yahoo => ("Yahoo Finance chart/spark", false),
        ProviderId::Nasdaq => ("Nasdaq public market API", false),
        ProviderId::Binance => ("Binance market-data-only host", false),
        ProviderId::Kraken => ("Kraken public market-data API", false),
        ProviderId::Coinbase => ("Coinbase Exchange public market-data API", false),
        ProviderId::Frankfurter => ("Frankfurter reference FX API", false),
        ProviderId::BankOfCanada => ("Bank of Canada Valet API", false),
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

fn canonical_symbol(market: &str, symbol: &str) -> String {
    if market == "america" {
        return symbol.rsplit(':').next().unwrap_or(symbol).to_string();
    }
    symbol.to_string()
}

fn yahoo_venue(asset_class: AssetClass) -> &'static str {
    match asset_class {
        AssetClass::Equity => "Yahoo Finance",
        AssetClass::Crypto => "Yahoo Crypto",
        AssetClass::Fx => "Yahoo FX",
    }
}

fn session_for(asset_class: AssetClass) -> String {
    match asset_class {
        AssetClass::Fx | AssetClass::Crypto => "regular".to_string(),
        AssetClass::Equity => "regular".to_string(),
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(string) => string
            .trim()
            .trim_start_matches('$')
            .replace(',', "")
            .replace('%', "")
            .parse::<f64>()
            .ok(),
        _ => None,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as i64
}
