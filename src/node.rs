use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::NodeConfig;

pub struct NodeClient {
    client: Client,
    url: String,
    user: String,
    pass: String,
}

impl NodeClient {
    pub fn new(cfg: &NodeConfig) -> Self {
        Self {
            client: Client::new(),
            url: cfg.rpc_url.clone(),
            user: cfg.rpc_user.clone(),
            pass: cfg.rpc_pass.clone(),
        }
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({
            "jsonrpc": "1.0",
            "id": 1,
            "method": method,
            "params": params
        });
        let resp = self
            .client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&body)
            .send()
            .await?
            .json::<Value>()
            .await?;

        if let Some(err) = resp.get("error") {
            if !err.is_null() {
                return Err(anyhow!("RPC error: {err}"));
            }
        }
        Ok(resp["result"].clone())
    }

    pub async fn get_block_template(&self) -> Result<BlockTemplate> {
        let result = self
            .rpc(
                "getblocktemplate",
                json!([{"rules": ["segwit", "taproot"]}]),
            )
            .await?;
        let tmpl: BlockTemplate = serde_json::from_value(result)?;
        Ok(tmpl)
    }

    pub async fn submit_block(&self, block_hex: &str) -> Result<()> {
        let result = self.rpc("submitblock", json!([block_hex])).await?;
        // submitblock returns null on success, or a rejection reason string.
        if result.is_null() {
            Ok(())
        } else {
            Err(anyhow!("submitblock rejected: {result}"))
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct GbtTransaction {
    pub data: String,
    pub txid: String,
    pub hash: String, // wtxid
    pub fee: i64,
    pub weight: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BlockTemplate {
    pub version: u32,
    pub previousblockhash: String,
    pub transactions: Vec<GbtTransaction>,
    pub coinbasevalue: u64,
    pub bits: String,
    pub height: u32,
    pub curtime: u32,
    /// Pre-built segwit commitment script (OP_RETURN aa21a9ed…).
    #[serde(rename = "default_witness_commitment")]
    pub witness_commitment: Option<String>,
    #[serde(rename = "CommunityAutonomousAddress")]
    pub community_address: Option<String>,
    #[serde(rename = "CommunityAutonomousValue")]
    pub community_value: Option<u64>,
}
