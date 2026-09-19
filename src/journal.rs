// src/journal.rs
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::{atomic::{AtomicU64, Ordering}, Arc},
};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    sync::mpsc,
    time::{timeout, Duration, Instant},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalRecord {
    pub event_id: String,
    pub received_at_ms: i64,
    pub kind: String,
    pub symbol: Option<String>,
    pub provider: Option<String>,
    pub venue: Option<String>,
    pub sequence: Option<u64>,
    pub payload: Value,
}

#[derive(Clone)]
pub struct EventJournal {
    tx: mpsc::Sender<JournalRecord>,
    root: Arc<PathBuf>,
    dropped: Arc<AtomicU64>,
}

impl EventJournal {
    pub fn spawn(root: PathBuf) -> Self {
        let (tx, mut rx) = mpsc::channel::<JournalRecord>(8192);
        let dropped = Arc::new(AtomicU64::new(0));
        let root_arc = Arc::new(root.clone());

        tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut batch = vec![first];
                let deadline = Instant::now() + Duration::from_millis(50);

                while batch.len() < 256 {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match timeout(remaining, rx.recv()).await {
                        Ok(Some(record)) => batch.push(record),
                        Ok(None) | Err(_) => break,
                    }
                }

                if let Err(error) = append_batch(&root, &batch).await {
                    eprintln!("journal: {error}");
                }
            }
        });

        Self {
            tx,
            root: root_arc,
            dropped,
        }
    }

    pub fn append(&self, record: JournalRecord) {
        if self.tx.try_send(record).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    pub async fn read_day(root: &Path, date: &str) -> Result<Vec<JournalRecord>, String> {
        let path = root.join(format!("{date}.jsonl"));
        let text = fs::read_to_string(path)
            .await
            .map_err(|error| error.to_string())?;

        let mut records = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let record = serde_json::from_str::<JournalRecord>(line)
                .map_err(|error| format!("invalid journal record: {error}"))?;
            records.push(record);
        }
        Ok(records)
    }
}

async fn append_batch(root: &Path, records: &[JournalRecord]) -> Result<(), String> {
    if records.is_empty() {
        return Ok(());
    }

    let mut grouped = std::collections::HashMap::<String, String>::new();
    for record in records {
        let path = root.join(day_from_ms(record.received_at_ms));
        let key = path.to_string_lossy().to_string();
        let line = serde_json::to_string(record).map_err(|error| error.to_string())?;
        grouped.entry(key).or_default().push_str(&line);
        grouped.entry(path.to_string_lossy().to_string()).or_default().push('\n');
    }

    fs::create_dir_all(root)
        .await
        .map_err(|error| error.to_string())?;

    for (path, content) in grouped {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|error| error.to_string())?;
        file.write_all(content.as_bytes())
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn day_from_ms(ms: i64) -> String {
    use chrono::{DateTime, Utc};
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|value| value.format("%Y-%m-%d.jsonl").to_string())
        .unwrap_or_else(|| "unknown-date.jsonl".to_string())
}
