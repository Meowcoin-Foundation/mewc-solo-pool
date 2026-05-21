use anyhow::{anyhow, Result};
use chrono::Utc;
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::Mutex;

pub struct Db {
    client: Client,
    base_url: String,
    service_key: String,
    server_id: String,
    last_block_at: Mutex<Option<SystemTime>>,
}

pub type SharedDb = Arc<Db>;

impl Db {
    pub fn new(base_url: String, service_key: String, server_id: String) -> SharedDb {
        Arc::new(Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
            base_url: base_url.trim_end_matches('/').to_string(),
            service_key,
            server_id,
            last_block_at: Mutex::new(None),
        })
    }

    fn url(&self, table: &str) -> String {
        format!("{}/rest/v1/{}", self.base_url, table)
    }

    async fn post(&self, table: &str, body: Value, prefer: &str) -> Result<Value> {
        let resp = self
            .client
            .post(self.url(table))
            .header("apikey", &self.service_key)
            .header("Authorization", format!("Bearer {}", self.service_key))
            .header("Content-Type", "application/json")
            .header("Prefer", prefer)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("POST {table} failed ({status}): {text}"));
        }
        if prefer.contains("representation") {
            Ok(resp.json().await?)
        } else {
            Ok(Value::Null)
        }
    }

    async fn patch(&self, table: &str, filter: &str, body: Value) -> Result<()> {
        let url = format!("{}?{}", self.url(table), filter);
        let resp = self
            .client
            .patch(url)
            .header("apikey", &self.service_key)
            .header("Authorization", format!("Bearer {}", self.service_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("PATCH {table} failed ({status}): {text}"));
        }
        Ok(())
    }

    /// On startup: close any sessions this server left open from a previous run.
    /// Uses server_id so we never touch another server's live sessions.
    pub async fn close_stale_sessions(&self) -> Result<()> {
        self.patch(
            "meowpow_miner_sessions",
            &format!("disconnected_at=is.null&server_id=eq.{}", self.server_id),
            json!({ "disconnected_at": Utc::now().to_rfc3339() }),
        )
        .await
    }

    pub async fn log_block(
        &self,
        height: u32,
        hash: &str,
        finder_address: &str,
        worker: &str,
        miner_payout_sats: u64,
        dev_fee_sats: u64,
        community_payout_sats: u64,
    ) -> Result<()> {
        let now = SystemTime::now();
        let gap_seconds = {
            let mut last = self.last_block_at.lock().await;
            let gap = last.map(|t| now.duration_since(t).unwrap_or_default().as_secs() as i64);
            *last = Some(now);
            gap
        };

        self.post(
            "meowpow_blocks",
            json!({
                "height":            height,
                "hash":              hash,
                "finder_address":    finder_address,
                "worker":            worker,
                "miner_payout":      miner_payout_sats as f64 / 1e8,
                "dev_fee":           dev_fee_sats as f64 / 1e8,
                "community_payout":  community_payout_sats as f64 / 1e8,
                "tx_fees":           0,
                "gap_seconds":       gap_seconds,
            }),
            "return=minimal",
        )
        .await?;
        Ok(())
    }

    pub async fn log_session_start(
        &self,
        address: &str,
        worker: &str,
        ip: &str,
    ) -> Result<Option<i64>> {
        let rows = self
            .post(
                "meowpow_miner_sessions",
                json!({
                    "address":   address,
                    "worker":    worker,
                    "ip":        ip,
                    "server_id": self.server_id,
                }),
                "return=representation",
            )
            .await?;
        Ok(rows[0]["id"].as_i64())
    }

    pub async fn log_session_end(&self, session_id: i64) -> Result<()> {
        self.patch(
            "meowpow_miner_sessions",
            &format!("id=eq.{session_id}"),
            json!({ "disconnected_at": Utc::now().to_rfc3339() }),
        )
        .await
    }

    pub async fn flush_share_window(
        &self,
        address: &str,
        worker: &str,
        window_start: SystemTime,
        window_end: SystemTime,
        shares_valid: u32,
        shares_invalid: u32,
        shares_stale: u32,
        difficulty_avg: f64,
        hashrate_mhs: f64,
    ) -> Result<()> {
        let to_rfc = |t: SystemTime| {
            let dt = chrono::DateTime::<Utc>::from(t);
            dt.to_rfc3339()
        };

        self.post(
            "meowpow_share_windows",
            json!({
                "address":        address,
                "worker":         worker,
                "window_start":   to_rfc(window_start),
                "window_end":     to_rfc(window_end),
                "shares_valid":   shares_valid,
                "shares_invalid": shares_invalid,
                "shares_stale":   shares_stale,
                "difficulty_avg": difficulty_avg,
                "hashrate_mhs":   hashrate_mhs,
            }),
            "return=minimal",
        )
        .await?;
        Ok(())
    }
}
