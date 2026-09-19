// src/journal.rs
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    sync::mpsc,
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
    tx: mpsc::UnboundedSender<JournalRecord>,
    root: Arc<PathBuf>,
}

impl EventJournal {
    pub fn spawn(root: PathBuf) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<JournalRecord>();
        let root_arc = Arc::new(root.clone());

        tokio::spawn(async move {
            while let Some(record) = rx.recv().await {
                if let Err(error) = append_record(&root, &record).await {
                    eprintln!("journal: {error}");
                }
            }
        });

        Self {
            tx,
            root: root_arc,
        }
    }

    pub fn append(&self, record: JournalRecord) {
        let _ = self.tx.send(record);
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

async fn append_record(root: &Path, record: &JournalRecord) -> Result<(), String> {
    fs::create_dir_all(root)
        .await
        .map_err(|error| error.to_string())?;
    let path = root.join(day_from_ms(record.received_at_ms));
    let line = serde_json::to_string(record).map_err(|error| error.to_string())? + "\n";
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .map_err(|error| error.to_string())?;
    file.write_all(line.as_bytes())
        .await
        .map_err(|error| error.to_string())
}

fn day_from_ms(ms: i64) -> String {
    use chrono::{DateTime, Utc};
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|value| value.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "unknown-date".to_string())
}
