// src/news.rs
use chrono::{DateTime, Duration as ChronoDuration};
use futures_util::stream::{self, StreamExt};
use quick_xml::{events::Event, reader::Reader};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewsArticle {
    pub id: String,
    pub provider: String,
    pub query: String,
    pub ticker: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub url: String,
    pub source: Option<String>,
    pub author: Option<String>,
    pub subreddit: Option<String>,
    pub published_at: Option<String>,
    pub published_at_ms: Option<i64>,
    #[serde(default)]
    pub event_type: String,
    #[serde(default)]
    pub cluster_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoryCluster {
    pub id: String,
    pub key: String,
    pub canonical_title: String,
    pub first_seen_ms: Option<i64>,
    pub last_seen_ms: Option<i64>,
    pub article_count: usize,
    pub sources: Vec<String>,
    pub tickers: Vec<String>,
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct NewsProviderHealth {
    pub provider: String,
    pub status: String,
    pub requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub last_success_ms: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Clone)]
struct NewsApiControl {
    last_request_by_query: HashMap<String, i64>,
    window_day: String,
    day_count: u32,
}

#[derive(Clone)]
pub struct NewsRouter {
    client: Client,
    health: Arc<RwLock<HashMap<String, NewsProviderHealth>>>,
    cache: Arc<RwLock<Vec<NewsArticle>>>,
    clusters: Arc<RwLock<HashMap<String, StoryCluster>>>,
    newsapi_control: Arc<RwLock<NewsApiControl>>,
    processed_news: Arc<RwLock<HashSet<String>>>,
}

impl NewsRouter {
    pub fn new() -> Result<Self, String> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .pool_idle_timeout(Duration::from_secs(30))
            .user_agent("pine-foundry/0.3 public-news-client")
            .build()
            .map_err(|error| error.to_string())?;

        let providers = [
            "reddit_rss",
            "reddit_json",
            "google_news_rss",
            "newsapi",
        ];
        let mut health = HashMap::new();
        for provider in providers {
            health.insert(
                provider.to_string(),
                NewsProviderHealth {
                    provider: provider.to_string(),
                    status: if provider == "newsapi" && newsapi_key().is_none() {
                        "unconfigured".to_string()
                    } else {
                        "unprobed".to_string()
                    },
                    requests: 0,
                    successes: 0,
                    failures: 0,
                    last_success_ms: None,
                    last_error: None,
                },
            );
        }

        Ok(Self {
            client,
            health: Arc::new(RwLock::new(health)),
            cache: Arc::new(RwLock::new(Vec::new())),
            clusters: Arc::new(RwLock::new(HashMap::new())),
            newsapi_control: Arc::new(RwLock::new(NewsApiControl {
                last_request_by_query: HashMap::new(),
                window_day: String::new(),
                day_count: 0,
            })),
            processed_news: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    pub async fn health(&self) -> Vec<NewsProviderHealth> {
        let mut items = self.health.read().await.values().cloned().collect::<Vec<_>>();
        items.sort_by(|a, b| a.provider.cmp(&b.provider));
        items
    }

    pub async fn cache(&self) -> Vec<NewsArticle> {
        self.cache.read().await.clone()
    }

    pub async fn clusters(&self) -> Vec<StoryCluster> {
        let mut items = self.clusters.read().await.values().cloned().collect::<Vec<_>>();
        items.sort_by(|a, b| b.last_seen_ms.cmp(&a.last_seen_ms));
        items
    }

    pub async fn search_all(
        &self,
        query: &str,
        ticker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NewsArticle>, String> {
        let limit = limit.clamp(1, 100);
        let (reddit_rss, reddit_json, google, newsapi) = tokio::join!(
            self.reddit_rss_search(query, ticker, limit),
            self.reddit_json_search(query, ticker, limit),
            self.google_news_search(query, ticker, limit),
            self.newsapi_search(query, ticker, limit),
        );

        let mut articles = Vec::new();
        let mut errors = Vec::new();

        for result in [reddit_rss, reddit_json, google, newsapi] {
            match result {
                Ok(mut batch) => articles.append(&mut batch),
                Err(error) => errors.push(error),
            }
        }

        let articles = dedupe_and_sort(articles);
        if articles.is_empty() && !errors.is_empty() {
            return Err(errors.join("; "));
        }

        self.cache_articles(&articles).await;
        Ok(articles)
    }

    pub async fn search_ticker(
        &self,
        ticker: &str,
        limit: usize,
    ) -> Result<Vec<NewsArticle>, String> {
        let normalized = normalize_ticker(ticker);
        let query = broad_ticker_query(&normalized);
        self.search_all(&query, Some(&normalized), limit).await
    }

    pub async fn reddit_rss_search(
        &self,
        query: &str,
        ticker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NewsArticle>, String> {
        let url = format!(
            "https://www.reddit.com/search.rss?q={}&sort=new&limit={}",
            urlencoding::encode(query),
            limit.clamp(1, 100)
        );
        let xml = self
            .get_text(
                "reddit_rss",
                self.client
                    .get(url)
                    .header("Accept", "application/rss+xml, application/xml;q=0.9, */*;q=0.8"),
            )
            .await?;
        Ok(parse_rss(&xml, "reddit_rss", query, ticker))
    }

    pub async fn reddit_json_search(
        &self,
        query: &str,
        ticker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NewsArticle>, String> {
        let url = format!(
            "https://www.reddit.com/search.json?q={}&sort=new&limit={}",
            urlencoding::encode(query),
            limit.clamp(1, 100)
        );
        let value = self
            .get_json(
                "reddit_json",
                self.client
                    .get(url)
                    .header("Accept", "application/json"),
            )
            .await?;
        Ok(parse_reddit_json(&value, query, ticker))
    }

    pub async fn google_news_search(
        &self,
        query: &str,
        ticker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NewsArticle>, String> {
        let url = format!(
            "https://news.google.com/rss/search?q={}&hl=en-CA&gl=CA&ceid=CA:en",
            urlencoding::encode(query)
        );
        let xml = self
            .get_text(
                "google_news_rss",
                self.client
                    .get(url)
                    .header("Accept", "application/rss+xml, application/xml;q=0.9, */*;q=0.8"),
            )
            .await?;
        let mut articles = parse_rss(&xml, "google_news_rss", query, ticker);
        articles.truncate(limit.clamp(1, 100));
        Ok(articles)
    }

    pub async fn newsapi_search(
        &self,
        query: &str,
        ticker: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NewsArticle>, String> {
        let api_key = newsapi_key()
            .ok_or_else(|| "NEWSAPI or NEWSAPI_KEY is not configured".to_string())?;
        let lookback_hours = env::var("PINE_FOUNDRY_NEWS_LOOKBACK_HOURS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(24)
            .clamp(1, 24 * 365);
        let from = DateTime::from_timestamp_millis(now_ms())
            .map(|value| value - ChronoDuration::hours(lookback_hours))
            .map(|value| value.to_rfc3339())
            .unwrap_or_default();
        {
            let min_interval_secs = env::var("PINE_FOUNDRY_NEWSAPI_MIN_INTERVAL_SECS")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(1800)
                .clamp(60, 86_400);
            let daily_limit = env::var("PINE_FOUNDRY_NEWSAPI_DAILY_LIMIT")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(90)
                .clamp(1, 100);
            let today = DateTime::from_timestamp_millis(now_ms())
                .map(|value| value.format("%Y-%m-%d").to_string())
                .unwrap_or_default();
            let mut control = self.newsapi_control.write().await;
            if control.window_day != today {
                control.window_day = today;
                control.day_count = 0;
                control.last_request_by_query.clear();
            }
            if control.day_count >= daily_limit {
                return Err("newsapi daily request guard reached".to_string());
            }
            let query_key = query.trim().to_ascii_lowercase();
            if let Some(last) = control.last_request_by_query.get(&query_key) {
                if now_ms().saturating_sub(*last) < min_interval_secs * 1000 {
                    return Err("newsapi request interval guard active".to_string());
                }
            }
            control.last_request_by_query.insert(query_key, now_ms());
            control.day_count += 1;
        }

        let url = format!(
            "https://newsapi.org/v2/everything?q={}&from={}&language=en&sortBy=publishedAt&pageSize={}",
            urlencoding::encode(query),
            urlencoding::encode(&from),
            limit.clamp(1, 100)
        );
        let value = self
            .get_json(
                "newsapi",
                self.client
                    .get(url)
                    .header("X-Api-Key", api_key),
            )
            .await?;

        if value.get("status").and_then(Value::as_str) == Some("error") {
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("NewsAPI returned an error");
            record_failure(&self.health, "newsapi", message).await;
            return Err(format!("newsapi: {message}"));
        }

        let mut articles = parse_newsapi_json(&value, query, ticker);
        articles.truncate(limit.clamp(1, 100));
        Ok(articles)
    }

    pub async fn refresh_configured(&self) -> Vec<NewsArticle> {
        let tickers = csv_env("PINE_FOUNDRY_NEWS_TICKERS", "BTC,ETH,SOL");
        let queries = csv_env("PINE_FOUNDRY_NEWS_QUERIES", "");

        let mut jobs = Vec::new();
        for ticker in tickers {
            jobs.push(self.search_ticker(&ticker, configured_limit()));
        }
        for query in queries {
            if !query.is_empty() {
                jobs.push(self.search_all(&query, None, configured_limit()));
            }
        }

        let mut collected = Vec::new();
        let mut worker = stream::iter(jobs).buffer_unordered(configured_concurrency());
        while let Some(result) = worker.next().await {
            match result {
                Ok(mut articles) => collected.append(&mut articles),
                Err(error) => eprintln!("news feed: {error}"),
            }
        }
        let articles = dedupe_and_sort(collected);
        let mut seen = self.processed_news.write().await;
        let mut fresh = Vec::new();
        for article in articles {
            let key = canonical_article_key(&article);
            if seen.insert(key) {
                fresh.push(article);
            }
        }
        if seen.len() > 10_000 {
            seen.clear();
        }
        fresh
    }

    async fn cache_articles(&self, articles: &[NewsArticle]) {
        if articles.is_empty() {
            return;
        }

        let new_articles = {
            let mut cache = self.cache.write().await;
            let mut seen = cache
                .iter()
                .map(canonical_article_key)
                .collect::<HashSet<_>>();
            let mut new_articles = Vec::new();
            for article in articles {
                if seen.insert(canonical_article_key(article)) {
                    cache.push(article.clone());
                    new_articles.push(article.clone());
                }
            }

            cache.sort_by(|a, b| b.published_at_ms.cmp(&a.published_at_ms));
            let max_items = env::var("PINE_FOUNDRY_NEWS_CACHE_SIZE")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(500)
                .clamp(50, 10_000);
            if cache.len() > max_items {
                cache.truncate(max_items);
            }
            new_articles
        };

        update_clusters_locked(&self.clusters, &new_articles).await;
    }

    async fn get_text(
        &self,
        provider: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<String, String> {
        self.record_request(provider).await;
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                let message = error.to_string();
                record_failure(&self.health, provider, &message).await;
                return Err(format!("{provider}: {message}"));
            }
        };

        if !response.status().is_success() {
            let message = format!("HTTP {}", response.status());
            record_failure(&self.health, provider, &message).await;
            return Err(format!("{provider}: {message}"));
        }

        let text = match response.text().await {
            Ok(text) => text,
            Err(error) => {
                let message = error.to_string();
                record_failure(&self.health, provider, &message).await;
                return Err(format!("{provider}: {message}"));
            }
        };
        record_success(&self.health, provider).await;
        Ok(text)
    }

    async fn get_json(
        &self,
        provider: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<Value, String> {
        let text = self.get_text(provider, request).await?;
        serde_json::from_str(&text)
            .map_err(|error| format!("{provider}: invalid JSON response: {error}"))
    }

    async fn record_request(&self, provider: &str) {
        let mut health = self.health.write().await;
        let entry = health.entry(provider.to_string()).or_insert_with(|| NewsProviderHealth {
            provider: provider.to_string(),
            status: "unprobed".to_string(),
            requests: 0,
            successes: 0,
            failures: 0,
            last_success_ms: None,
            last_error: None,
        });
        entry.requests += 1;
    }
}

pub async fn run_news_feed(router: Arc<NewsRouter>, state: crate::AppState) {
    let poll_secs = env::var("PINE_FOUNDRY_NEWS_POLL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60)
        .clamp(15, 3600);

    loop {
        let articles = router.refresh_configured().await;
        crate::ingest_news_articles(&state, &articles).await;
        sleep(Duration::from_secs(poll_secs)).await;
    }
}

async fn record_success(
    health: &Arc<RwLock<HashMap<String, NewsProviderHealth>>>,
    provider: &str,
) {
    let mut items = health.write().await;
    if let Some(entry) = items.get_mut(provider) {
        entry.status = "ok".to_string();
        entry.successes += 1;
        entry.last_success_ms = Some(now_ms());
        entry.last_error = None;
    }
}

async fn record_failure(
    health: &Arc<RwLock<HashMap<String, NewsProviderHealth>>>,
    provider: &str,
    message: &str,
) {
    let mut items = health.write().await;
    if let Some(entry) = items.get_mut(provider) {
        entry.status = "degraded".to_string();
        entry.failures += 1;
        entry.last_error = Some(message.chars().take(300).collect());
    }
}

fn newsapi_key() -> Option<String> {
    env::var("NEWSAPI")
        .or_else(|_| env::var("NEWSAPI_KEY"))
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn configured_limit() -> usize {
    env::var("PINE_FOUNDRY_NEWS_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(25)
        .clamp(1, 100)
}

fn configured_concurrency() -> usize {
    env::var("PINE_FOUNDRY_NEWS_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4)
        .clamp(1, 16)
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

fn normalize_ticker(ticker: &str) -> String {
    let mut value = ticker
        .trim()
        .trim_start_matches('$')
        .to_ascii_uppercase();

    for suffix in ["=X", "USDT", "USDC"] {
        if let Some(stripped) = value.strip_suffix(suffix) {
            value = stripped.to_string();
            break;
        }
    }

    value
}

fn configured_aliases(ticker: &str) -> Vec<String> {
    let mut aliases = Vec::new();
    let raw = env::var("PINE_FOUNDRY_NEWS_ALIASES").unwrap_or_default();
    for entry in raw.split(';') {
        let Some((key, values)) = entry.split_once('=') else { continue; };
        if key.trim().eq_ignore_ascii_case(ticker) {
            aliases.extend(values.split('|').map(str::trim).filter(|v| !v.is_empty()).map(str::to_owned));
        }
    }
    aliases
}

fn broad_ticker_query(ticker: &str) -> String {
    let mut terms = vec![format!("\"${}\"", ticker), format!("\"{}\"", ticker)];
    terms.extend(match ticker {
        "BTC" => vec!["Bitcoin".to_string()],
        "ETH" => vec!["Ethereum".to_string()],
        "SOL" => vec!["Solana".to_string()],
        "XRP" => vec!["XRP Ripple".to_string()],
        "DOGE" => vec!["Dogecoin".to_string()],
        "ADA" => vec!["Cardano".to_string()],
        "AVAX" => vec!["Avalanche".to_string()],
        "BNB" => vec!["BNB Binance".to_string()],
        "DOT" => vec!["Polkadot".to_string()],
        "LINK" => vec!["Chainlink".to_string()],
        _ => Vec::new(),
    });
    terms.extend(configured_aliases(ticker));
    let body = terms.into_iter().collect::<HashSet<_>>().into_iter().collect::<Vec<_>>().join(" OR ");
    format!("({body})")
}

fn parse_rss(
    xml: &str,
    provider: &str,
    query: &str,
    ticker: Option<&str>,
) -> Vec<NewsArticle> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut articles = Vec::new();
    let mut current_field = None::<String>;
    let mut in_item = false;
    let mut item = RssItem::default();

    loop {
        match reader.read_event() {
            Ok(Event::Start(event)) => {
                let name = event.name();
                match name.as_ref() {
                    b"item" | b"entry" => {
                        in_item = true;
                        item = RssItem::default();
                        current_field = None;
                    }
                    b"title" | b"description" | b"link" | b"guid" | b"pubDate"
                    | b"published" | b"updated" | b"author" | b"source" | b"dc:creator" => {
                        if in_item {
                            current_field = Some(String::from_utf8_lossy(name.as_ref()).to_string());
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Empty(event)) => {
                if in_item && event.name().as_ref() == b"link" {
                    if let Some(href) = event
                        .attributes()
                        .flatten()
                        .find(|attribute| attribute.key.as_ref() == b"href")
                    {
                        item.link = String::from_utf8_lossy(&href.value).to_string();
                    }
                }
            }
            Ok(Event::Text(event)) => {
                if in_item {
                    let value = String::from_utf8_lossy(event.as_ref()).trim().to_string();
                    if !value.is_empty() {
                        assign_rss_field(&mut item, current_field.as_deref(), &value);
                    }
                }
            }
            Ok(Event::CData(event)) => {
                if in_item {
                    let value = String::from_utf8_lossy(event.as_ref()).trim().to_string();
                    if !value.is_empty() {
                        assign_rss_field(&mut item, current_field.as_deref(), &value);
                    }
                }
            }
            Ok(Event::End(event)) => {
                let name = event.name();
                if in_item && (name.as_ref() == b"item" || name.as_ref() == b"entry") {
                    if !item.title.is_empty() && !item.link.is_empty() {
                        articles.push(item.to_article(provider, query, ticker));
                    }
                    in_item = false;
                    current_field = None;
                } else if in_item {
                    current_field = None;
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
    }

    articles
}

#[derive(Default)]
struct RssItem {
    title: String,
    description: Option<String>,
    link: String,
    guid: Option<String>,
    published_at: Option<String>,
    author: Option<String>,
    source: Option<String>,
}

fn assign_rss_field(item: &mut RssItem, field: Option<&str>, value: &str) {
    match field {
        Some("title") => item.title = value.to_string(),
        Some("description") => item.description = Some(value.to_string()),
        Some("link") => item.link = value.to_string(),
        Some("guid") => item.guid = Some(value.to_string()),
        Some("pubDate") | Some("published") | Some("updated") => {
            item.published_at = Some(value.to_string())
        }
        Some("author") | Some("dc:creator") => item.author = Some(value.to_string()),
        Some("source") => item.source = Some(value.to_string()),
        _ => {}
    }
}

impl RssItem {
    fn to_article(&self, provider: &str, query: &str, ticker: Option<&str>) -> NewsArticle {
        let id_source = self
            .guid
            .as_deref()
            .filter(|value| !value.is_empty())
            .unwrap_or(&self.link);

        NewsArticle {
            id: format!("{provider}:{id_source}"),
            provider: provider.to_string(),
            query: query.to_string(),
            ticker: ticker.map(str::to_owned),
            title: self.title.clone(),
            description: self.description.clone(),
            url: self.link.clone(),
            source: self.source.clone(),
            author: self.author.clone(),
            subreddit: reddit_subreddit_from_url(&self.link),
            published_at: self.published_at.clone(),
            published_at_ms: self.published_at.as_deref().and_then(parse_date_ms),
            event_type: String::new(),
            cluster_id: String::new(),
        }
    }
}

fn parse_reddit_json(
    value: &Value,
    query: &str,
    ticker: Option<&str>,
) -> Vec<NewsArticle> {
    let Some(children) = value
        .get("data")
        .and_then(|data| data.get("children"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };

    children
        .iter()
        .filter_map(|child| {
            let data = child.get("data")?;
            let title = data.get("title").and_then(Value::as_str)?.to_string();
            let permalink = data
                .get("permalink")
                .and_then(Value::as_str)
                .map(|value| format!("https://www.reddit.com{value}"))
                .or_else(|| data.get("url").and_then(Value::as_str).map(str::to_owned))?;
            let created_ms = data
                .get("created_utc")
                .and_then(Value::as_f64)
                .map(|seconds| (seconds * 1000.0) as i64);
            let id = data
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or(&permalink);

            Some(NewsArticle {
                id: format!("reddit_json:{}", id),
                provider: "reddit_json".to_string(),
                query: query.to_string(),
                ticker: ticker.map(str::to_owned),
                title,
                description: data
                    .get("selftext")
                    .and_then(Value::as_str)
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned),
                url: permalink.clone(),
                source: data
                    .get("subreddit_name_prefixed")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                author: data
                    .get("author")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                subreddit: data
                    .get("subreddit")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                published_at: created_ms.map(|ms| {
                    DateTime::from_timestamp_millis(ms)
                        .map(|timestamp| timestamp.to_rfc3339())
                        .unwrap_or_default()
                }),
                published_at_ms: created_ms,
                event_type: String::new(),
                cluster_id: String::new(),
            })
        })
        .collect()
}

#[derive(Deserialize)]
struct NewsApiResponse {
    articles: Vec<NewsApiArticle>,
}

#[derive(Deserialize)]
struct NewsApiArticle {
    source: Option<NewsApiSource>,
    author: Option<String>,
    title: Option<String>,
    description: Option<String>,
    url: Option<String>,
    #[serde(rename = "publishedAt")]
    published_at: Option<String>,
}

#[derive(Deserialize)]
struct NewsApiSource {
    name: Option<String>,
}

fn parse_newsapi_json(
    value: &Value,
    query: &str,
    ticker: Option<&str>,
) -> Vec<NewsArticle> {
    let Ok(response) = serde_json::from_value::<NewsApiResponse>(value.clone()) else {
        return Vec::new();
    };

    response
        .articles
        .into_iter()
        .filter_map(|article| {
            let url = article.url?;
            let title = article.title.unwrap_or_else(|| "(untitled)".to_string());
            Some(NewsArticle {
                id: format!("newsapi:{url}"),
                provider: "newsapi".to_string(),
                query: query.to_string(),
                ticker: ticker.map(str::to_owned),
                title,
                description: article.description,
                url,
                source: article.source.and_then(|source| source.name),
                author: article.author,
                subreddit: None,
                published_at_ms: article.published_at.as_deref().and_then(parse_date_ms),
                published_at: article.published_at,
                event_type: String::new(),
                cluster_id: String::new(),
            })
        })
        .collect()
}

fn reddit_subreddit_from_url(url: &str) -> Option<String> {
    let marker = "/r/";
    let start = url.find(marker)? + marker.len();
    let rest = &url[start..];
    let subreddit = rest.split('/').next()?.trim();
    (!subreddit.is_empty()).then(|| format!("r/{subreddit}"))
}

pub(crate) fn parse_date_for_state(value: &str) -> Option<i64> { parse_date_ms(value) }

fn parse_date_ms(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.timestamp_millis())
        .or_else(|_| {
            DateTime::parse_from_rfc2822(value)
                .map(|timestamp| timestamp.timestamp_millis())
        })
        .ok()
}

fn canonical_article_key(article: &NewsArticle) -> String {
    let url = article.url.trim().to_ascii_lowercase();
    if !url.is_empty() { return url; }
    article.title.trim().to_ascii_lowercase()
}

fn dedupe_and_sort(mut articles: Vec<NewsArticle>) -> Vec<NewsArticle> {
    for article in &mut articles {
        article.event_type = classify_catalyst(&article.title, article.description.as_deref());
        article.cluster_id = cluster_id_for(&article.title);
    }
    let mut seen = HashSet::new();
    articles.retain(|article| {
        let canonical = article.url.trim().to_ascii_lowercase();
        let title = article.title.trim().to_ascii_lowercase();
        let key = if canonical.is_empty() { title } else { canonical };
        seen.insert(key)
    });
    articles.sort_by(|a, b| b.published_at_ms.cmp(&a.published_at_ms));
    articles
}



fn classify_catalyst(title: &str, description: Option<&str>) -> String {
    let text = format!("{} {}", title, description.unwrap_or_default()).to_ascii_lowercase();
    for (needles, kind) in [
        (&["acquire", "acquisition", "merger", "takeover"], "m_and_a"),
        (&["earnings", "eps", "revenue", "quarter"], "earnings"),
        (&["guidance", "outlook", "forecast"], "guidance"),
        (&["offering", "dilution", "atm", "shares"], "offering"),
        (&["buyback", "repurchase"], "buyback"),
        (&["dividend", "distribution"], "dividend"),
        (&["fda", "approval", "clinical trial"], "fda_or_clinical"),
        (&["lawsuit", "litigation", "investigation", "sec probe"], "legal_or_regulatory"),
        (&["bankruptcy", "chapter 11"], "bankruptcy"),
        (&["ceo", "chief executive", "management change"], "management"),
        (&["contract", "award", "partnership"], "contract_or_partnership"),
        (&["hack", "exploit", "breach"], "security_incident"),
        (&["etf", "exchange-traded fund"], "etf"),
        (&["tariff", "rate decision", "interest rate"], "macro"),
    ] {
        if needles.iter().any(|needle| text.contains(needle)) {
            return kind.to_string();
        }
    }
    "general".to_string()
}

fn cluster_key(title: &str) -> String {
    const STOP: &[&str] = &[
        "the", "a", "an", "of", "to", "for", "and", "or", "on", "in",
        "with", "by", "from", "inc", "corp", "ltd", "company", "shares",
    ];
    let mut tokens = title
        .split_whitespace()
        .map(|token| {
            token
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric())
                .collect::<String>()
                .to_ascii_lowercase()
        })
        .filter(|token| token.len() >= 3 && !STOP.contains(&token.as_str()))
        .collect::<Vec<_>>();
    tokens.sort();
    tokens.dedup();
    tokens.truncate(16);
    tokens.join("|")
}

fn cluster_id_for(title: &str) -> String {
    let key = cluster_key(title);
    let mut hash = 0xcbf29ce484222325u64;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("cluster-{hash:016x}")
}

async fn update_clusters_locked(
    clusters: &Arc<RwLock<HashMap<String, StoryCluster>>>,
    articles: &[NewsArticle],
) {
    if articles.is_empty() {
        return;
    }
    let mut map = clusters.write().await;
    for article in articles {
        let id = if article.cluster_id.is_empty() {
            cluster_id_for(&article.title)
        } else {
            article.cluster_id.clone()
        };
        let entry = map.entry(id.clone()).or_insert_with(|| StoryCluster {
            id: id.clone(),
            key: cluster_key(&article.title),
            canonical_title: article.title.clone(),
            first_seen_ms: article.published_at_ms,
            last_seen_ms: article.published_at_ms,
            article_count: 0,
            sources: Vec::new(),
            tickers: Vec::new(),
            event_type: article.event_type.clone(),
        });
        entry.article_count += 1;
        entry.first_seen_ms = match (entry.first_seen_ms, article.published_at_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (None, value) => value,
            (value, None) => value,
        };
        entry.last_seen_ms = match (entry.last_seen_ms, article.published_at_ms) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, value) => value,
            (value, None) => value,
        };
        if let Some(source) = article.source.as_ref() {
            if !entry.sources.contains(source) {
                entry.sources.push(source.clone());
            }
        }
        if let Some(ticker) = article.ticker.as_ref() {
            if !entry.tickers.contains(ticker) {
                entry.tickers.push(ticker.clone());
            }
        }
    }
    if map.len() > 500 {
        let mut ids = map
            .values()
            .filter_map(|item| item.last_seen_ms.map(|ts| (item.id.clone(), ts)))
            .collect::<Vec<_>>();
        ids.sort_by_key(|(_, ts)| *ts);
        for (id, _) in ids.into_iter().take(map.len().saturating_sub(500)) {
            map.remove(&id);
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as i64
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticker_query_expands_known_crypto_aliases() {
        let query = broad_ticker_query("BTC");
        assert!(query.contains("Bitcoin"));
        assert!(query.contains("$"));
    }

    #[test]
    fn rss_parser_handles_atom_search_results() {
        let xml = r#"<feed xmlns="http://www.w3.org/2005/Atom">
            <entry>
                <title>Bitcoin headline</title>
                <link href="https://www.reddit.com/r/Bitcoin/comments/abc/headline/" />
                <updated>2026-09-19T12:34:56Z</updated>
                <author><name>tester</name></author>
            </entry>
        </feed>"#;
        let items = parse_rss(xml, "reddit_rss", "bitcoin", Some("BTC"));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].ticker.as_deref(), Some("BTC"));
        assert_eq!(items[0].subreddit.as_deref(), Some("r/Bitcoin"));
    }

    #[test]
    fn dedupe_is_url_based() {
        let first = NewsArticle {
            id: "a".into(),
            provider: "one".into(),
            query: "x".into(),
            ticker: None,
            title: "Headline".into(),
            description: None,
            url: "https://example.com/story".into(),
            source: None,
            author: None,
            subreddit: None,
            published_at: None,
            published_at_ms: Some(2),
            event_type: String::new(),
            cluster_id: String::new(),
        };
        let second = NewsArticle { id: "b".into(), provider: "two".into(), ..first.clone() };
        assert_eq!(dedupe_and_sort(vec![first, second]).len(), 1);
    }
}
