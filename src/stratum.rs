use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, error, info, warn};

use crate::db::{self, Db};
use crate::job::{diff_to_target, Job};
use crate::node::NodeClient;
use crate::verify;
use crate::{SharedJob, StratumConfig};

pub async fn serve(
    cfg: StratumConfig,
    node: Arc<NodeClient>,
    current_job: SharedJob,
    job_tx: Arc<broadcast::Sender<Job>>,
    db: Db,
) -> Result<()> {
    let addr = format!("0.0.0.0:{}", cfg.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Stratum listening on {addr}");

    let db = Arc::new(db);

    loop {
        let (stream, peer) = listener.accept().await?;
        info!("Miner connected: {peer}");

        let node2 = Arc::clone(&node);
        let cj = Arc::clone(&current_job);
        let rx = job_tx.subscribe();
        let db2 = Arc::clone(&db);
        let initial_diff = cfg.initial_difficulty;

        tokio::spawn(async move {
            if let Err(e) = handle_miner(stream, node2, cj, rx, db2, initial_diff).await {
                warn!("Miner {peer} disconnected: {e}");
            }
        });
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
    shares_since_retarget: u32,
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
            shares_since_retarget: 0,
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
                            // Keep only the two most recent jobs; older ones can't win.
                            if st2.active_jobs.len() >= 4 {
                                st2.active_jobs.clear();
                            }
                            st2.active_jobs.insert(
                                job.id.clone(),
                                (job, hh_hex_str, mr),
                            );
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Job broadcast lagged by {n}");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break; // EOF
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        debug!("← {trimmed}");

        let msg: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                warn!("JSON parse error: {e}");
                continue;
            }
        };

        let id = msg.get("id").cloned().unwrap_or(Value::Null);
        let method = msg["method"].as_str().unwrap_or("").to_string();
        let params = msg["params"].clone();

        let response = match method.as_str() {
            "mining.subscribe" => handle_subscribe(id, &extranonce1),

            "mining.authorize" => {
                handle_authorize(
                    id,
                    &params,
                    &state,
                    &current_job,
                    &writer,
                    initial_diff,
                )
                .await
            }

            "mining.submit" => {
                handle_submit(id, &params, &state, &node, &db, &writer).await
            }

            "mining.extranonce.subscribe" => {
                json!({"id": id, "result": true, "error": null})
            }

            "eth_submitHashrate" => {
                json!({"id": id, "result": true, "error": null})
            }

            other => {
                warn!("Unknown method: {other}");
                json!({"id": id, "result": null, "error": [20, "Unknown method", null]})
            }
        };

        if !response.is_null() {
            send_msg(&writer, response).await?;
        }
    }

    Ok(())
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
    initial_diff: u64,
) -> Value {
    let login = params[0].as_str().unwrap_or("");
    // Miners often send "address.workername" or "address.workername.password" as one string.
    let (address, worker_suffix) = login.split_once('.').unwrap_or((login, ""));
    let address = address.to_string();
    let worker = if worker_suffix.is_empty() {
        params[1].as_str().unwrap_or("default").to_string()
    } else {
        worker_suffix.split('.').next().unwrap_or("default").to_string()
    };

    info!("Miner authorized: {address}.{worker}");

    {
        let mut st = state.lock().await;
        st.address = Some(address.clone());
        st.worker = worker;
    }

    // Send authorize ack first — some miners ignore notifications received before it.
    let _ = send_msg(writer, json!({"id": id, "result": true, "error": null})).await;

    // Then push difficulty and current job.
    let _ = send_msg(
        writer,
        json!({
            "id": null,
            "method": "mining.set_difficulty",
            "params": [initial_diff]
        }),
    )
    .await;

    if let Some(job) = current_job.read().await.clone() {
        let (hh, mr) = job.header_hash(&address);
        let hh_hex = hex::encode(hh);
        let pool_target = diff_to_target(initial_diff);
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
            warn!("Invalid share for job {job_id}");
            return json!({"id": id, "result": null, "error": [23, "low difficulty share", null]});
        }
    };

    info!("Valid share: job={job_id} diff={diff}");

    // Check if it also meets network difficulty.
    let net_final = verify::check_share(&hh_hex, &mix_hash_hex, nonce, &job.network_boundary);
    if net_final.is_some() {
        info!("*** BLOCK FOUND by {address} at height {} ***", job.height);

        // Re-derive header prefix from merkle root.
        let header_prefix = job.build_header_prefix(merkle_root);

        // mix_hash raw = reverse of GetHex.
        if let Some(mix_raw) = verify::mix_hash_to_raw(&mix_hash_hex) {
            let block_hex = hex::encode(job.serialize_block(
                &header_prefix,
                nonce,
                &mix_raw,
                &address,
            ));

            let final_hash_hex = hex::encode(final_hash);

            match node.submit_block(&block_hex).await {
                Ok(()) => {
                    info!("Block submitted! hash={final_hash_hex}");
                    if let Err(e) = db::log_block(db, job.height, &final_hash_hex, &address).await {
                        error!("DB log error: {e}");
                    }
                }
                Err(e) => error!("submitblock failed: {e}"),
            }
        }
    }

    // Vardiff: retarget after 8 shares or 60 s, whichever comes first.
    let new_diff_opt = {
        let mut st = state.lock().await;
        st.shares_since_retarget += 1;
        let elapsed = st.retarget_start.elapsed().as_secs_f64();
        if st.shares_since_retarget >= 8 || elapsed >= 60.0 {
            let avg = elapsed / st.shares_since_retarget as f64;
            let new_diff = ((st.difficulty as f64 * 30.0 / avg) as u64).clamp(1, 1_000_000);
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

