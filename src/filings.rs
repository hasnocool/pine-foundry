// src/filings.rs
use crate::{journal::{EventJournal, JournalRecord}, now_ms};
use futures_util::stream::{self, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
};
use tokio::{
    sync::RwLock,
    time::{sleep, Duration},
};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilingEvent {
    pub id: String,
    pub ticker: String,
    pub cik: String,
    pub form: String,
    pub filing_date: String,
    pub acceptance_datetime: Option<String>,
    pub accession: String,
    pub primary_document: Option<String>,
    pub url: String,
    pub provider: String,
    pub event_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilingHealth {
    pub status: String,
    pub requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub last_success_ms: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct SecFilingRouter {
    client: Client,
    journal: Arc<EventJournal>,
    health: Arc<RwLock<FilingHealth>>,
    seen: Arc<RwLock<HashSet<String>>>,
}

impl SecFilingRouter {
    pub fn new(journal: Arc<EventJournal>) -> Result<Self, String> {
        let user_agent = env::var("PINE_FOUNDRY_SEC_USER_AGENT")
            .unwrap_or_else(|_| "PineFoundry/0.4 research".to_string());

        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .pool_idle_timeout(Duration::from_secs(30))
            .user_agent(user_agent)
            .build()
            .map_err(|error| error.to_string())?;

        Ok(Self {
            client,
            journal,
            health: Arc::new(RwLock::new(FilingHealth {
                status: "unprobed".to_string(),
                requests: 0,
                successes: 0,
                failures: 0,
                last_success_ms: None,
                last_error: None,
            })),
            seen: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    pub async fn health(&self) -> FilingHealth {
        self.health.read().await.clone()
    }

    pub async fn search_ticker(&self, ticker: &str) -> Result<Vec<FilingEvent>, String> {
        let map = self.ticker_map().await?;
        let Some(cik) = map.get(&ticker.to_ascii_uppercase()) else {
            return Err(format!("SEC ticker not found: {ticker}"));
        };
        self.submissions(ticker, cik).await
    }

    pub async fn refresh_configured(&self) -> Vec<FilingEvent> {
        let tickers = env::var("PINE_FOUNDRY_SEC_TICKERS")
            .unwrap_or_else(|_| "".to_string())
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();

        if tickers.is_empty() {
            return Vec::new();
        }

        let worker = stream::iter(tickers)
            .map(|ticker| {
                let router = self.clone();
                async move { router.search_ticker(&ticker).await }
            })
            .buffer_unordered(2);

        let mut worker = worker;
        let mut collected = Vec::new();
        while let Some(result) = worker.next().await {
            match result {
                Ok(events) => {
                    for event in events {
                        self.emit(event.clone()).await;
                        collected.push(event);
                    }
                }
                Err(error) => eprintln!("SEC filing feed: {error}"),
            }
        }
        collected
    }

    async fn ticker_map(&self) -> Result<HashMap<String, String>, String> {
        let url = "https://www.sec.gov/files/company_tickers.json";
        let value = self.get_json(url).await?;
        let object = value
            .as_object()
            .ok_or_else(|| "SEC ticker map was not an object".to_string())?;

        let mut map = HashMap::new();
        for row in object.values() {
            let Some(ticker) = row.get("ticker").and_then(|value| value.as_str()) else {
                continue;
            };
            let Some(cik) = row.get("cik_str") else {
                continue;
            };
            let cik = match cik {
                serde_json::Value::Number(value) => value.to_string(),
                serde_json::Value::String(value) => value.clone(),
                _ => continue,
            };
            map.insert(ticker.to_ascii_uppercase(), format!("{cik:0>10}"));
        }
        Ok(map)
    }

    async fn submissions(&self, ticker: &str, cik: &str) -> Result<Vec<FilingEvent>, String> {
        let url = format!("https://data.sec.gov/submissions/CIK{cik}.json");
        let value = self.get_json(&url).await?;
        let recent = value
            .get("filings")
            .and_then(|value| value.get("recent"))
            .ok_or_else(|| "SEC recent filings were missing".to_string())?;

        let forms = recent.get("form").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let dates = recent.get("filingDate").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let accessions = recent.get("accessionNumber").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let docs = recent.get("primaryDocument").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let accepted = recent.get("acceptanceDateTime").and_then(|v| v.as_array()).cloned().unwrap_or_default();

        let len = forms.len().min(dates.len()).min(accessions.len());
        let mut events = Vec::new();

        for index in 0..len.min(50) {
            let form = forms[index].as_str().unwrap_or_default().to_string();
            let filing_date = dates[index].as_str().unwrap_or_default().to_string();
            let accession = accessions[index].as_str().unwrap_or_default().to_string();
            let primary_document = docs.get(index).and_then(|v| v.as_str()).map(str::to_owned);
            let acceptance_datetime = accepted.get(index).and_then(|v| v.as_str()).map(str::to_owned);

            if accession.is_empty() || form.is_empty() {
                continue;
            }

            let id = format!("sec:{accession}");
            if self.seen.read().await.contains(&id) {
                continue;
            }

            let accession_path = accession.replace('-', "");
            let url = match &primary_document {
                Some(document) => format!(
                    "https://www.sec.gov/Archives/edgar/data/{}/{}/{}",
                    cik.trim_start_matches('0'),
                    accession_path,
                    document
                ),
                None => format!(
                    "https://www.sec.gov/Archives/edgar/data/{}/{}/",
                    cik.trim_start_matches('0'),
                    accession_path
                ),
            };

            events.push(FilingEvent {
                id,
                ticker: ticker.to_ascii_uppercase(),
                cik: cik.to_string(),
                form: form.clone(),
                filing_date,
                acceptance_datetime,
                accession,
                primary_document,
                url,
                provider: "SEC EDGAR".to_string(),
                event_type: classify_form(&form),
            });
        }

        Ok(events)
    }

    async fn emit(&self, event: FilingEvent) {
        self.seen.write().await.insert(event.id.clone());
        let payload = match serde_json::to_value(&event) {
            Ok(value) => value,
            Err(_) => return,
        };
        self.journal.append(JournalRecord {
            event_id: Uuid::new_v4().to_string(),
            received_at_ms: now_ms(),
            kind: "filing".to_string(),
            symbol: Some(event.ticker.clone()),
            provider: Some("sec".to_string()),
            venue: Some("SEC EDGAR".to_string()),
            sequence: None,
            payload,
        });
    }

    async fn get_json(&self, url: &str) -> Result<serde_json::Value, String> {
        {
            let mut health = self.health.write().await;
            health.requests += 1;
        }

        let response = self.client.get(url).send().await.map_err(|error| {
            error.to_string()
        })?;

        if !response.status().is_success() {
            let message = format!("HTTP {}", response.status());
            let mut health = self.health.write().await;
            health.status = "degraded".to_string();
            health.failures += 1;
            health.last_error = Some(message.clone());
            return Err(message);
        }

        let value = response.json::<serde_json::Value>().await.map_err(|error| error.to_string())?;
        let mut health = self.health.write().await;
        health.status = "ok".to_string();
        health.successes += 1;
        health.last_success_ms = Some(now_ms());
        health.last_error = None;
        Ok(value)
    }
}

pub async fn run_sec_feed(router: Arc<SecFilingRouter>, state: crate::AppState) {
    let poll_secs = env::var("PINE_FOUNDRY_SEC_POLL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30)
        .clamp(15, 3600);

    loop {
        let filings = router.refresh_configured().await;
        crate::ingest_filing_events(&state, &filings).await;
        sleep(Duration::from_secs(poll_secs)).await;
    }
}

fn classify_form(form: &str) -> String {
    match form {
        "8-K" | "8-K/A" => "material_event",
        "10-K" | "10-K/A" => "annual_report",
        "10-Q" | "10-Q/A" => "quarterly_report",
        "6-K" | "6-K/A" => "foreign_material_event",
        "20-F" | "20-F/A" => "foreign_annual_report",
        "S-1" | "S-1/A" | "S-3" | "S-3/A" => "registration",
        "424B2" | "424B3" | "424B4" | "424B5" => "prospectus",
        "4" | "4/A" | "3" | "5" => "insider_transaction",
        "SC 13D" | "SC 13D/A" | "SC 13G" | "SC 13G/A" => "ownership_change",
        _ => "filing",
    }
}
