// src/canada.rs
use crate::{news::{NewsArticle, NewsRouter}, now_ms};
use futures_util::stream::{self, StreamExt};
use serde::Serialize;
use std::{collections::HashSet, env, sync::Arc};
use tokio::{sync::RwLock, time::{sleep, Duration}};

#[derive(Debug, Clone, Serialize)]
pub struct CanadianDisclosureHealth {
    pub status: String, pub requests: u64, pub successes: u64, pub failures: u64,
    pub last_success_ms: Option<i64>, pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct CanadianDisclosureRouter {
    news: Arc<NewsRouter>,
    health: Arc<RwLock<CanadianDisclosureHealth>>,
    seen: Arc<RwLock<HashSet<String>>>,
}

impl CanadianDisclosureRouter {
    pub fn new(news: Arc<NewsRouter>) -> Self {
        Self { news, health: Arc::new(RwLock::new(CanadianDisclosureHealth {
            status: "unprobed".to_string(), requests: 0, successes: 0, failures: 0,
            last_success_ms: None, last_error: None,
        })), seen: Arc::new(RwLock::new(HashSet::new())) }
    }

    pub async fn health(&self) -> CanadianDisclosureHealth { self.health.read().await.clone() }

    pub async fn search_ticker(&self, ticker: &str, limit: usize) -> Result<Vec<NewsArticle>, String> {
        let normalized = ticker.trim().trim_start_matches('$').trim_end_matches(".TO").trim_end_matches(".V").to_ascii_uppercase();
        let query = format!("(\"${}\" OR \"{}\") (site:sedarplus.ca OR site:tsx.com/en/news OR site:sedarplus.ca/csa-party)", normalized, normalized);
        { self.health.write().await.requests += 1; }
        match self.news.google_news_search(&query, Some(&normalized), limit).await {
            Ok(mut articles) => {
                for article in &mut articles { article.provider = "canada_disclosure".to_string();
                    if article.event_type.is_empty() { article.event_type = "canadian_disclosure".to_string(); } }
                let mut h = self.health.write().await; h.status="ok".to_string(); h.successes += 1; h.last_success_ms=Some(now_ms()); h.last_error=None;
                Ok(articles)
            }
            Err(error) => { let mut h=self.health.write().await; h.status="degraded".to_string(); h.failures += 1; h.last_error=Some(error.clone()); Err(error) }
        }
    }

    pub async fn refresh_configured(&self) -> Vec<NewsArticle> {
        let tickers = env::var("PINE_FOUNDRY_CANADA_TICKERS").unwrap_or_default().split(',').map(str::trim).filter(|v| !v.is_empty()).map(str::to_owned).collect::<Vec<_>>();
        let mut worker = stream::iter(tickers)
            .map(|ticker| {
                let router = self.clone();
                async move { router.search_ticker(&ticker, 25).await }
            })
            .buffer_unordered(2);

        let mut collected = Vec::new();
        while let Some(result) = worker.next().await {
            match result {
                Ok(mut articles) => collected.append(&mut articles),
                Err(error) => eprintln!("Canadian disclosure feed: {error}"),
            }
        }

        let mut seen = self.seen.write().await;
        let mut fresh = Vec::new();
        for article in collected {
            let key = article.url.trim().to_ascii_lowercase();
            if seen.insert(key) {
                fresh.push(article);
            }
        }
        if seen.len() > 10_000 { seen.clear(); }

        fresh
    }
}

pub async fn run_canadian_disclosure_feed(router: Arc<CanadianDisclosureRouter>, state: crate::AppState) {
    let poll_secs = env::var("PINE_FOUNDRY_CANADA_POLL_SECS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(300).clamp(60, 3600);
    loop {
        let articles = router.refresh_configured().await;
        crate::ingest_news_articles(&state, &articles).await;
        sleep(Duration::from_secs(poll_secs)).await;
    }
}