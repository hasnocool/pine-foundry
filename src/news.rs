// src/news.rs
use chrono::DateTime;
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
pub struct NewsRouter {
    client: Client,
    health: Arc<RwLock<HashMap<String, NewsProviderHealth>>>,
    cache: Arc<RwLock<Vec<NewsArticle>>>,
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
        let url = format!(
            "https://newsapi.org/v2/everything?q={}&language=en&sortBy=publishedAt&pageSize={}",
            urlencoding::encode(query),
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

        let mut articles = parse_newsapi_json(&value, query, ticker);
        articles.truncate(limit.clamp(1, 100));
        Ok(articles)
    }

    pub async fn refresh_configured(&self) {
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

        let mut worker = stream::iter(jobs).buffer_unordered(configured_concurrency());
        while let Some(result) = worker.next().await {
            if let Err(error) = result {
                eprintln!("news feed: {error}");
            }
        }
    }

    async fn cache_articles(&self, articles: &[NewsArticle]) {
        if articles.is_empty() {
            return;
        }

        let mut cache = self.cache.write().await;
        let mut seen = cache.iter().map(|item| item.id.clone()).collect::<HashSet<_>>();
        for article in articles {
            if seen.insert(article.id.clone()) {
                cache.push(article.clone());
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
    }

    async fn get_text(
        &self,
        provider: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<String, String> {
        self.record_request(provider).await;
        let response = request
            .send()
            .await
            .map_err(|error| {
                error.to_string()
            })?;

        if !response.status().is_success() {
            let message = format!("HTTP {}", response.status());
            record_failure(&self.health, provider, &message).await;
            return Err(format!("{provider}: {message}"));
        }

        let text = response.text().await.map_err(|error| error.to_string())?;
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

pub async fn run_news_feed(router: Arc<NewsRouter>) {
    let poll_secs = env::var("PINE_FOUNDRY_NEWS_POLL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60)
        .clamp(15, 3600);

    loop {
        router.refresh_configured().await;
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

fn broad_ticker_query(ticker: &str) -> String {
    let aliases = match ticker {
        "BTC" => Some("Bitcoin"),
        "ETH" => Some("Ethereum"),
        "SOL" => Some("Solana"),
        "XRP" => Some("XRP Ripple"),
        "DOGE" => Some("Dogecoin"),
        "ADA" => Some("Cardano"),
        "AVAX" => Some("Avalanche"),
        "BNB" => Some("BNB Binance"),
        "DOT" => Some("Polkadot"),
        "LINK" => Some("Chainlink"),
        _ => None,
    };

    match aliases {
        Some(alias) => format!(
            "(\"{}{}\" OR \"{}\" OR \"{}\")",
            "$", ticker, ticker, alias
        ),
        None => format!("(\"{}{}\" OR \"{}\")", "$", ticker, ticker),
    }
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

fn parse_date_ms(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.timestamp_millis())
        .or_else(|_| {
            DateTime::parse_from_rfc2822(value)
                .map(|timestamp| timestamp.timestamp_millis())
        })
        .ok()
}

fn dedupe_and_sort(mut articles: Vec<NewsArticle>) -> Vec<NewsArticle> {
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as i64
}
