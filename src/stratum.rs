use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::db::Db;
use crate::job::{diff_to_target, Job};
use crate::node::NodeClient;
use crate::verify;
use crate::{SharedJob, StratumConfig};

pub async fn serve(
    cfg: StratumConfig,
    node: Arc<NodeClient>,
    current_job: SharedJob,
    job_tx: Arc<broadcast::Sender<Job>>,
    db: Arc<Db>,
) -> Result<()> {
    let addr = format!("0.0.0.0:{}", cfg.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Stratum listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        info!("Miner connected: {peer}");

        let node2 = Arc::clone(&node);
        let cj = Arc::clone(&current_job);
        let rx = job_tx.subscribe();
        let db2 = Arc::clone(&db);
        let initial_diff = cfg.initial_difficulty;
        let fee_address = cfg.fee_address.clone();
        let peer_ip = peer.ip().to_string();

        tokio::spawn(async move {
            if let Err(e) = handle_miner(stream, node2, cj, rx, db2, initial_diff, fee_address, peer_ip).await {
                warn!("Miner {peer} disconnected: {e}");
            }
        });
    }
}

#[derive(Clone)]
struct ShareAggregate {
    window_start: SystemTime,
    shares_valid: u32,
    shares_invalid: u32,
    shares_stale: u32,
    difficulty_sum: f64,
    peak_share_diff: f64,
}

impl ShareAggregate {
    fn new() -> Self {
        Self {
            window_start: SystemTime::now(),
            shares_valid: 0,
            shares_invalid: 0,
            shares_stale: 0,
            difficulty_sum: 0.0,
            peak_share_diff: 0.0,
        }
    }

    fn hashrate_mhs(&self, window_end: SystemTime) -> f64 {
        let elapsed = window_end
            .duration_since(self.window_start)
            .unwrap_or_default()
            .as_secs_f64();
        if elapsed <= 0.0 || self.shares_valid == 0 {
            return 0.0;
        }
        let avg_diff = self.difficulty_sum / self.shares_valid as f64;
        // hashrate = shares * difficulty * 2^32 / elapsed / 1e6 (MH/s)
        (self.shares_valid as f64 * avg_diff * 4_294_967_296.0) / elapsed / 1_000_000.0
    }

    fn difficulty_avg(&self) -> f64 {
        if self.shares_valid == 0 { 1.0 } else { self.difficulty_sum / self.shares_valid as f64 }
    }
}

struct MinerState {
    address: Option<String>,
    worker: String,
    difficulty: u64,
    #[allow(dead_code)]
    extranonce1: String,
    /// job_id → (Job, header_hash_hex, merkle_root)
    active_jobs: HashMap<String, (Job, String, [u8; 32])>,
    retarget_start: Instant,
    connected_at: Instant,
    shares_since_retarget: u32,
    share_agg: ShareAggregate,
    db_session_id: Option<i64>,
    total_valid: u32,
    total_invalid: u32,
}

impl MinerState {
    fn new(initial_diff: u64, extranonce1: String) -> Self {
        Self {
            address: None,
            worker: String::new(),
            difficulty: initial_diff,
            extranonce1,
            active_jobs: HashMap::new(),
            retarget_start: Instant::now(),
            connected_at: Instant::now(),
            shares_since_retarget: 0,
            share_agg: ShareAggregate::new(),
            db_session_id: None,
            total_valid: 0,
            total_invalid: 0,
        }
    }
}

async fn handle_miner(
    stream: TcpStream,
    node: Arc<NodeClient>,
    current_job: SharedJob,
    mut job_rx: broadcast::Receiver<Job>,
    db: Arc<Db>,
    initial_diff: u64,
    fee_address: Option<String>,
    peer_ip: String,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(Mutex::new(writer));

    let extranonce1 = hex::encode(&uuid::Uuid::new_v4().as_bytes()[..4]);
    let state = Arc::new(Mutex::new(MinerState::new(initial_diff, extranonce1.clone())));

    // Spawn task to push new jobs as they arrive.
    {
        let writer2 = Arc::clone(&writer);
        let state2 = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                match job_rx.recv().await {
                    Ok(job) => {
                        let (addr_opt, diff) = {
                            let st = state2.lock().await;
                            (st.address.clone(), st.difficulty)
                        };
                        if let Some(addr_clone) = addr_opt {
                            let (hh_bytes, mr) = job.header_hash(&addr_clone);
                            let hh_hex_str = hex::encode(hh_bytes);
                            let pool_target = diff_to_target(diff);
                            let notify = build_notify(&job, &hh_hex_str, &pool_target, true);
                            let _ = send_msg(&writer2, notify).await;
                            let mut st2 = state2.lock().await;
                            if st2.active_jobs.len() >= 4 {
                                st2.active_jobs.clear();
                            }
                            st2.active_jobs.insert(job.id.clone(), (job, hh_hex_str, mr));
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => warn!("Job broadcast lagged by {n}"),
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    let mut line = String::new();
    let mut flush_interval = tokio::time::interval(Duration::from_secs(60));
    flush_interval.tick().await; // skip immediate first tick

    loop {
        line.clear();
        tokio::select! {
            result = reader.read_line(&mut line) => {
                let n = result?;
                if n == 0 { break; }
                let trimmed = line.trim();
                if trimmed.is_empty() { continue; }
                debug!("← {trimmed}");

                let msg: Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(e) => { warn!("JSON parse error: {e}"); continue; }
                };

                let id = msg.get("id").cloned().unwrap_or(Value::Null);
                let method = msg["method"].as_str().unwrap_or("").to_string();
                let params = msg["params"].clone();

                let response = match method.as_str() {
                    "mining.subscribe" => handle_subscribe(id, &extranonce1),

                    "mining.authorize" => {
                        handle_authorize(id, &params, &state, &current_job, &writer, initial_diff, &fee_address, &db, &peer_ip).await
                    }

                    "login" => {
                        handle_login(id, &params, &state, &current_job, initial_diff, &fee_address, &db, &peer_ip).await
                    }

                    "mining.submit" => {
                        handle_submit(id, &params, &state, &node, &db, &writer).await
                    }

                    "mining.extranonce.subscribe" => {
                        json!({"id": id, "result": true, "error": null})
                    }

                    "mining.suggest_difficulty" => {
                        if let Some(suggested) = params[0].as_f64().map(|f| f as u64).filter(|&d| d > 0) {
                            state.lock().await.difficulty = suggested;
                            let _ = send_msg(&writer, json!({"id": null, "method": "mining.set_difficulty", "params": [suggested]})).await;
                            info!("Difficulty set to {suggested} by miner suggestion");
                        }
                        json!({"id": id, "result": true, "error": null})
                    }

                    "eth_submitHashrate" => json!({"id": id, "result": true, "error": null}),

                    other => {
                        warn!("Unknown method: {other}");
                        json!({"id": id, "result": null, "error": [20, "Unknown method", null]})
                    }
                };

                if !response.is_null() {
                    send_msg(&writer, response).await?;
                }
            }

            _ = flush_interval.tick() => {
                do_flush_shares(&state, &db).await;
            }
        }
    }

    // Flush remaining shares and close the session on disconnect.
    do_flush_shares(&state, &db).await;
    {
        let st = state.lock().await;
        let secs = st.connected_at.elapsed().as_secs();
        let label = match &st.address {
            Some(a) => format!("{}.{}", a, st.worker),
            None => "(unauthorized)".to_string(),
        };
        info!(
            "Miner disconnected: {} — {}s session, {} valid {} invalid shares",
            label, secs, st.total_valid, st.total_invalid
        );
        if let Some(sid) = st.db_session_id {
            drop(st);
            if let Err(e) = db.log_session_end(sid).await {
                error!("log_session_end failed: {e}");
            }
        }
    }

    Ok(())
}

async fn do_flush_shares(state: &Arc<Mutex<MinerState>>, db: &Arc<Db>) {
    let (agg, address, worker) = {
        let mut st = state.lock().await;
        if st.address.is_none() || (st.share_agg.shares_valid == 0 && st.share_agg.shares_invalid == 0) {
            return;
        }
        let agg = st.share_agg.clone();
        st.share_agg = ShareAggregate::new();
        (agg, st.address.clone().unwrap(), st.worker.clone())
    };

    let window_end = SystemTime::now();
    if let Err(e) = db.flush_share_window(
        &address,
        &worker,
        agg.window_start,
        window_end,
        agg.shares_valid,
        agg.shares_invalid,
        agg.shares_stale,
        agg.difficulty_avg(),
        agg.hashrate_mhs(window_end),
        agg.peak_share_diff,
    ).await {
        error!("flush_share_window failed: {e}");
    }
}

async fn handle_login(
    id: Value,
    params: &Value,
    state: &Arc<Mutex<MinerState>>,
    current_job: &SharedJob,
    _initial_diff: u64,
    fee_address: &Option<String>,
    db: &Arc<Db>,
    peer_ip: &str,
) -> Value {
    let login = params["login"].as_str().unwrap_or("");
    let (raw_address, worker_suffix) = login.split_once('.').unwrap_or((login, ""));
    let worker = if worker_suffix.is_empty() {
        params["rigid"].as_str().unwrap_or("default").to_string()
    } else {
        worker_suffix.to_string()
    };

    let address = if bs58::decode(raw_address)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_vec()
        .map(|b| b.len() >= 21)
        .unwrap_or(false)
    {
        raw_address.to_string()
    } else if let Some(fb) = fee_address {
        warn!("Login: invalid address '{raw_address}', using fallback");
        fb.clone()
    } else {
        warn!("Login: invalid address '{raw_address}', no fallback — rejecting");
        return json!({"id": id, "result": null, "error": [24, "invalid address", null]});
    };

    let diff = {
        let mut st = state.lock().await;
        st.address = Some(address.clone());
        st.worker = worker.clone();
        st.difficulty
    };

    info!("Miner login: {address}.{worker}");

    // Log session start (fire-and-forget, don't block the response).
    {
        let db2 = Arc::clone(db);
        let addr2 = address.clone();
        let work2 = worker.clone();
        let ip2 = peer_ip.to_string();
        let state2 = Arc::clone(state);
        tokio::spawn(async move {
            match db2.log_session_start(&addr2, &work2, &ip2).await {
                Ok(id) => state2.lock().await.db_session_id = id,
                Err(e) => error!("log_session_start failed: {e}"),
            }
        });
    }

    let pool_target = diff_to_target(diff);
    let target_hex = hex::encode(pool_target);
    let session_id = hex::encode(&uuid::Uuid::new_v4().as_bytes()[..8]);

    if let Some(job) = current_job.read().await.clone() {
        let (hh, mr) = job.header_hash(&address);
        let hh_hex = hex::encode(hh);
        {
            let mut st = state.lock().await;
            if st.active_jobs.len() >= 4 {
                st.active_jobs.clear();
            }
            st.active_jobs.insert(job.id.clone(), (job.clone(), hh_hex.clone(), mr));
        }
        json!({
            "id": id,
            "error": null,
            "result": {
                "id": session_id,
                "status": "OK",
                "job": {
                    "job_id": job.id,
                    "seed_hash": job.seed_hash,
                    "blob": hh_hex,
                    "target": target_hex
                }
            }
        })
    } else {
        json!({
            "id": id,
            "error": null,
            "result": {
                "id": session_id,
                "status": "OK",
                "job": null
            }
        })
    }
}

fn handle_subscribe(id: Value, extranonce1: &str) -> Value {
    json!({
        "id": id,
        "result": [[["mining.set_difficulty",""],["mining.notify",""]], extranonce1, 4],
        "error": null
    })
}

async fn handle_authorize(
    id: Value,
    params: &Value,
    state: &Arc<Mutex<MinerState>>,
    current_job: &SharedJob,
    writer: &Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    _initial_diff: u64,
    fee_address: &Option<String>,
    db: &Arc<Db>,
    peer_ip: &str,
) -> Value {
    let login = params[0].as_str().unwrap_or("");
    // Miners often send "address.workername" or "address.workername.password" as one string.
    let (raw_address, worker_suffix) = login.split_once('.').unwrap_or((login, ""));
    let worker = if worker_suffix.is_empty() {
        params[1].as_str().unwrap_or("default").to_string()
    } else {
        worker_suffix.split('.').next().unwrap_or("default").to_string()
    };

    // Validate that the address is a real base58 MEWC address.
    let address = if bs58::decode(raw_address)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_vec()
        .map(|b| b.len() >= 21)
        .unwrap_or(false)
    {
        raw_address.to_string()
    } else if let Some(fb) = fee_address {
        warn!("Invalid address '{raw_address}', using fallback");
        fb.clone()
    } else {
        warn!("Invalid address '{raw_address}', no fallback configured — rejecting");
        let _ = send_msg(writer, json!({"id": id, "result": null, "error": [24, "invalid address", null]})).await;
        return Value::Null;
    };

    info!("Miner authorized: {address}.{worker}");

    let diff = {
        let mut st = state.lock().await;
        st.address = Some(address.clone());
        st.worker = worker.clone();
        st.difficulty // may already be set by a prior mining.suggest_difficulty
    };

    // Log session start (fire-and-forget).
    {
        let db2 = Arc::clone(db);
        let addr2 = address.clone();
        let work2 = worker.clone();
        let ip2 = peer_ip.to_string();
        let state2 = Arc::clone(state);
        tokio::spawn(async move {
            match db2.log_session_start(&addr2, &work2, &ip2).await {
                Ok(id) => state2.lock().await.db_session_id = id,
                Err(e) => error!("log_session_start failed: {e}"),
            }
        });
    }

    // Send authorize ack first — some miners ignore notifications received before it.
    let _ = send_msg(writer, json!({"id": id, "result": true, "error": null})).await;

    // Then push difficulty and current job.
    let _ = send_msg(
        writer,
        json!({
            "id": null,
            "method": "mining.set_difficulty",
            "params": [diff]
        }),
    )
    .await;

    if let Some(job) = current_job.read().await.clone() {
        let (hh, mr) = job.header_hash(&address);
        let hh_hex = hex::encode(hh);
        let pool_target = diff_to_target(diff);
        let notify = build_notify(&job, &hh_hex, &pool_target, true);
        let _ = send_msg(writer, notify).await;

        let mut st = state.lock().await;
        st.active_jobs.insert(job.id.clone(), (job, hh_hex, mr));
    }

    // Already sent above — return null so the outer loop skips sending.
    Value::Null
}

async fn handle_submit(
    id: Value,
    params: &Value,
    state: &Arc<Mutex<MinerState>>,
    node: &Arc<NodeClient>,
    db: &Arc<Db>,
    writer: &Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
) -> Value {
    // params: [worker_name, job_id, nonce_hex, header_hash_hex, mix_hash_hex]
    let job_id = params[1].as_str().unwrap_or("").to_string();
    let nonce_str = params[2].as_str().unwrap_or("").trim_start_matches("0x").to_string();
    let submitted_hh = params[3].as_str().unwrap_or("").trim_start_matches("0x").to_string();
    let mix_hash_hex = params[4].as_str().unwrap_or("").trim_start_matches("0x").to_string();

    let nonce = match u64::from_str_radix(&nonce_str, 16) {
        Ok(n) => n,
        Err(_) => {
            return json!({"id": id, "result": null, "error": [20, "bad nonce", null]});
        }
    };

    let st = state.lock().await;
    let address = match &st.address {
        Some(a) => a.clone(),
        None => return json!({"id": id, "result": null, "error": [24, "not authorized", null]}),
    };
    let worker = st.worker.clone();

    let entry = match st.active_jobs.get(&job_id) {
        Some(e) => e.clone(),
        None => return json!({"id": id, "result": null, "error": [21, "stale job", null]}),
    };
    let diff = st.difficulty;
    drop(st);

    let (job, hh_hex, merkle_root) = entry;

    // Sanity check header_hash echo.
    if !submitted_hh.is_empty() && submitted_hh != hh_hex {
        warn!("Header hash mismatch from miner");
    }

    // Validate against pool difficulty.
    let pool_boundary = diff_to_target(diff);
    let final_hash = match verify::check_share(&hh_hex, &mix_hash_hex, nonce, &pool_boundary) {
        Some(h) => h,
        None => {
            warn!("Invalid share from {}.{} job={job_id}", address, worker);
            let mut st = state.lock().await;
            st.share_agg.shares_invalid += 1;
            st.total_invalid += 1;
            return json!({"id": id, "result": null, "error": [23, "low difficulty share", null]});
        }
    };

    // Actual difficulty of this specific hash: diff1_hi / hash_hi (top 128 bits).
    let hash_hi = u128::from_be_bytes(final_hash[..16].try_into().unwrap());
    let actual_diff = if hash_hi > 0 {
        (0x0000_0000_FFFF_0000_0000_0000_0000_0000u128 / hash_hi) as f64
    } else {
        f64::MAX
    };

    debug!("Valid share from {}.{} job={job_id} diff={diff} actual_diff={actual_diff:.0}", address, worker);

    // Record share in aggregate (valid).
    {
        let mut st = state.lock().await;
        st.share_agg.shares_valid += 1;
        st.share_agg.difficulty_sum += diff as f64;
        st.share_agg.peak_share_diff = st.share_agg.peak_share_diff.max(actual_diff);
        st.total_valid += 1;
    }

    // Check if it also meets network difficulty.
    let net_final = verify::check_share(&hh_hex, &mix_hash_hex, nonce, &job.network_boundary);
    if net_final.is_some() {
        info!("*** BLOCK FOUND by {address} at height {} ***", job.height);

        let header_prefix = job.build_header_prefix(merkle_root);
        let worker = state.lock().await.worker.clone();
        let (miner_payout_sats, dev_fee_sats) = job.coinbase_split();

        if let Some(mix_raw) = verify::mix_hash_to_raw(&mix_hash_hex) {
            let block_hex = hex::encode(job.serialize_block(&header_prefix, nonce, &mix_raw, &address));
            let final_hash_hex = hex::encode(final_hash);

            match node.submit_block(&block_hex).await {
                Ok(()) => {
                    info!("Block submitted! hash={final_hash_hex}");
                    let db2 = Arc::clone(db);
                    let addr2 = address.clone();
                    let hash2 = final_hash_hex.clone();
                    let height = job.height;
                    let community_sats = job.community_value;
                    tokio::spawn(async move {
                        if let Err(e) = db2.log_block(height, &hash2, &addr2, &worker, miner_payout_sats, dev_fee_sats, community_sats).await {
                            error!("DB log_block error: {e}");
                        }
                    });
                }
                Err(e) => error!("submitblock failed: {e}"),
            }
        }
    }

    // Vardiff: retarget after 8 shares or 60 s, whichever comes first.
    // Target 10 s/share → ~6 shares per 60 s window, keeping hashrate estimates stable.
    let new_diff_opt = {
        let mut st = state.lock().await;
        st.shares_since_retarget += 1;
        let elapsed = st.retarget_start.elapsed().as_secs_f64();
        if st.shares_since_retarget >= 8 || elapsed >= 60.0 {
            let avg = elapsed / st.shares_since_retarget as f64;
            let new_diff = ((st.difficulty as f64 * 10.0 / avg) as u64).clamp(1, 1_000_000);
            st.retarget_start = Instant::now();
            st.shares_since_retarget = 0;
            if new_diff != st.difficulty {
                st.difficulty = new_diff;
                Some(new_diff)
            } else {
                None
            }
        } else {
            None
        }
    };
    if let Some(new_diff) = new_diff_opt {
        let _ = send_msg(
            writer,
            json!({"id": null, "method": "mining.set_difficulty", "params": [new_diff]}),
        )
        .await;
        info!("Vardiff → {new_diff}");
    }

    json!({"id": id, "result": true, "error": null})
}

fn build_notify(job: &Job, header_hash_hex: &str, pool_target: &[u8; 32], clean: bool) -> Value {
    json!({
        "id": null,
        "method": "mining.notify",
        "params": [
            job.id,
            header_hash_hex,
            job.seed_hash,
            hex::encode(pool_target),
            clean,
            job.height
        ]
    })
}

async fn send_msg(
    writer: &Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    msg: Value,
) -> Result<()> {
    let mut w = writer.lock().await;
    let mut line = serde_json::to_string(&msg)?;
    line.push('\n');
    debug!("→ {}", line.trim());
    w.write_all(line.as_bytes()).await?;
    Ok(())
}

