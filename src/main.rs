mod db;
mod job;
mod node;
mod stratum;
mod verify;

use anyhow::Result;
use config::Config;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::info;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    pub rpc_url: String,
    pub rpc_user: String,
    pub rpc_pass: String,
    pub zmq_url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StratumConfig {
    pub port: u16,
    pub initial_difficulty: u64,
    /// Fallback MEWC address used when a miner provides an invalid address (e.g. MRR test probes).
    pub fallback_address: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PoolConfig {
    pub db_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub node: NodeConfig,
    pub stratum: StratumConfig,
    pub pool: PoolConfig,
}

pub type SharedJob = Arc<RwLock<Option<job::Job>>>;

fn parse_config_arg() -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    let mut it = args.iter().skip(1);
    while let Some(arg) = it.next() {
        if arg == "--config" || arg == "-c" {
            return it.next().cloned();
        }
        if let Some(val) = arg.strip_prefix("--config=") {
            return Some(val.to_string());
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config_path = parse_config_arg().unwrap_or_else(|| "config".to_string());

    let cfg: AppConfig = Config::builder()
        .add_source(config::File::with_name(&config_path))
        // Env vars override file: MEWC_NODE__RPC_PASS=x, MEWC_STRATUM__PORT=3333, etc.
        .add_source(config::Environment::with_prefix("MEWC").separator("__"))
        .build()?
        .try_deserialize()?;

    info!("Starting MEWC solo pool on stratum port {}", cfg.stratum.port);

    let pool = db::init(&cfg.pool.db_path).await?;
    let node = Arc::new(node::NodeClient::new(&cfg.node));
    let current_job: SharedJob = Arc::new(RwLock::new(None));

    let (job_tx, _) = broadcast::channel::<job::Job>(16);
    let job_tx = Arc::new(job_tx);

    match node.get_block_template().await {
        Ok(tmpl) => {
            let j = job::Job::from_template(tmpl);
            *current_job.write().await = Some(j.clone());
            let _ = job_tx.send(j);
            info!("Initial job ready");
        }
        Err(e) => tracing::warn!("Initial GBT failed: {e}"),
    }

    // ZMQ: blocking recv on its own thread, signals an async task via channel.
    {
        let (zmq_tx, mut zmq_rx) = tokio::sync::mpsc::channel::<()>(8);
        let zmq_url = cfg.node.zmq_url.clone();
        tokio::task::spawn_blocking(move || {
            let ctx = zmq::Context::new();
            let sock = ctx.socket(zmq::SUB).expect("zmq socket");
            sock.connect(&zmq_url).expect("zmq connect");
            sock.set_subscribe(b"hashblock").expect("zmq subscribe");
            loop {
                match sock.recv_multipart(0) {
                    Ok(_) => {
                        info!("ZMQ: new block");
                        let _ = zmq_tx.blocking_send(());
                    }
                    Err(e) => tracing::error!("ZMQ recv error: {e}"),
                }
            }
        });

        let node2 = Arc::clone(&node);
        let job_tx2 = Arc::clone(&job_tx);
        let current_job2 = Arc::clone(&current_job);
        tokio::spawn(async move {
            while zmq_rx.recv().await.is_some() {
                match node2.get_block_template().await {
                    Ok(tmpl) => {
                        let j = job::Job::from_template(tmpl);
                        *current_job2.write().await = Some(j.clone());
                        let _ = job_tx2.send(j);
                        info!("ZMQ: job refreshed");
                    }
                    Err(e) => tracing::error!("GBT after hashblock failed: {e}"),
                }
            }
        });
    }

    // Polling fallback: catch any ZMQ misses within 5 s.
    {
        let node3 = Arc::clone(&node);
        let job_tx3 = Arc::clone(&job_tx);
        let current_job3 = Arc::clone(&current_job);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.tick().await;
            loop {
                interval.tick().await;
                match node3.get_block_template().await {
                    Ok(tmpl) => {
                        let new_height = tmpl.height;
                        let cur_height = current_job3.read().await.as_ref().map(|j| j.height);
                        if cur_height != Some(new_height) {
                            let j = job::Job::from_template(tmpl);
                            *current_job3.write().await = Some(j.clone());
                            let _ = job_tx3.send(j);
                            info!("Poll: new height {new_height}, job refreshed");
                        }
                    }
                    Err(e) => tracing::warn!("GBT poll failed: {e}"),
                }
            }
        });
    }

    stratum::serve(cfg.stratum, node, current_job, job_tx, pool).await
}

