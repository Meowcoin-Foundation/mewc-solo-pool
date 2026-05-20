use meowpow::epoch::{calculate_epoch_seed, EPOCH_LENGTH};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::node::BlockTemplate;

/// A mining job derived from a getblocktemplate response.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub height: u32,
    pub version: u32,
    pub prev_hash: String,      // 32 bytes hex, raw block byte order (internal LE)
    pub bits: u32,
    pub time: u32,
    pub coinbase_value: u64,
    pub community_address: Option<String>,
    pub community_value: u64,
    pub witness_commitment_script: Option<Vec<u8>>,
    pub tx_data: Vec<Vec<u8>>,  // raw serialized transactions
    pub tx_wtxids: Vec<[u8; 32]>, // wtxids for witness merkle (raw LE)
    pub network_boundary: [u8; 32], // 32-byte target derived from bits
    pub seed_hash: String,      // 32 bytes hex (epoch seed, human-readable)
    pub fee_address: Option<String>,
    pub fee_percent: f64,
}

impl Job {
    pub fn from_template(t: BlockTemplate, fee_address: Option<String>, fee_percent: f64) -> Self {
        let bits = u32::from_str_radix(&t.bits, 16).expect("bits hex");
        let network_boundary = bits_to_target(bits);

        let epoch = (t.height as u64) / (EPOCH_LENGTH as u64);
        let seed = calculate_epoch_seed(epoch as i32);
        let seed_hash = hex::encode(seed.to_bytes());

        // Decode tx data and wtxids (stored in GBT as reversed/GetHex).
        let mut tx_data = Vec::new();
        let mut tx_wtxids: Vec<[u8; 32]> = Vec::new();
        for tx in &t.transactions {
            tx_data.push(hex::decode(&tx.data).expect("tx data hex"));
            // wtxid from GBT is GetHex (reversed); reverse back to internal order.
            let mut w = hex::decode(&tx.hash).expect("wtxid hex");
            w.reverse();
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&w);
            tx_wtxids.push(arr);
        }

        let witness_commitment_script = t
            .witness_commitment
            .as_deref()
            .map(|s| hex::decode(s).expect("witness commitment hex"));

        // prev_hash: GBT gives GetHex (reversed), reverse to raw internal.
        let mut prev_raw = hex::decode(&t.previousblockhash).expect("prevhash hex");
        prev_raw.reverse();
        let prev_hash = hex::encode(&prev_raw);

        let community_value = t.community_value.unwrap_or(0);

        Self {
            id: Uuid::new_v4().to_string().replace('-', "")[..8].to_string(),
            height: t.height,
            version: t.version,
            prev_hash,
            bits,
            time: t.curtime,
            coinbase_value: t.coinbasevalue,
            community_address: t.community_address,
            community_value,
            witness_commitment_script,
            tx_data,
            tx_wtxids,
            network_boundary,
            seed_hash,
            fee_address,
            fee_percent,
        }
    }

    /// Build the coinbase transaction for a given miner address.
    /// Returns (coinbase_bytes, coinbase_txid_bytes, coinbase_wtxid = [0u8;32]).
    pub fn build_coinbase(&self, miner_address: &str) -> Vec<u8> {
        let miner_script = p2pkh_script(miner_address);
        let community_script = self
            .community_address
            .as_deref()
            .map(p2pkh_script);

        let scriptsig = build_coinbase_scriptsig(self.height);

        let fee_script = if self.fee_percent > 0.0 {
            self.fee_address.as_deref().map(p2pkh_script)
        } else {
            None
        };
        let fee_value = if fee_script.is_some() {
            (self.coinbase_value as f64 * self.fee_percent / 100.0) as u64
        } else {
            0
        };
        let miner_value = self.coinbase_value - fee_value;

        let mut cb = Vec::new();

        // version (LE i32)
        cb.extend_from_slice(&2u32.to_le_bytes());

        // segwit marker + flag
        cb.push(0x00);
        cb.push(0x01);

        // input count
        cb.push(0x01);
        // prevout: 32 zero bytes + 0xffffffff index
        cb.extend_from_slice(&[0u8; 32]);
        cb.extend_from_slice(&0xffffffffu32.to_le_bytes());
        // scriptsig
        push_var_bytes(&mut cb, &scriptsig);
        // sequence
        cb.extend_from_slice(&0xffffffffu32.to_le_bytes());

        // outputs
        let mut num_outputs: u8 = 1;
        if community_script.is_some() { num_outputs += 1; }
        if fee_script.is_some() { num_outputs += 1; }
        if self.witness_commitment_script.is_some() { num_outputs += 1; }
        cb.push(num_outputs);

        // vout[0]: miner
        cb.extend_from_slice(&miner_value.to_le_bytes());
        push_var_bytes(&mut cb, &miner_script);

        // vout[1]: community fund (must be index 1 per Meowcoin consensus)
        if let Some(script) = &community_script {
            cb.extend_from_slice(&self.community_value.to_le_bytes());
            push_var_bytes(&mut cb, script);
        }

        // vout[2]: dev fee
        if let Some(script) = &fee_script {
            cb.extend_from_slice(&fee_value.to_le_bytes());
            push_var_bytes(&mut cb, script);
        }

        // vout[last]: OP_RETURN witness commitment
        if let Some(script) = &self.witness_commitment_script {
            cb.extend_from_slice(&0u64.to_le_bytes()); // value = 0
            push_var_bytes(&mut cb, script);
        }

        // witness for the single input: one item of 32 zero bytes
        cb.push(0x01); // item count
        cb.push(0x20); // 32 bytes
        cb.extend_from_slice(&[0u8; 32]);

        // locktime
        cb.extend_from_slice(&0u32.to_le_bytes());

        cb
    }

    /// Compute the header hash (SHA256d of first 80 header bytes, then reversed).
    /// Returns bytes ready for Hash256::from_bytes and for hex encoding into mining.notify.
    pub fn header_hash(&self, miner_address: &str) -> ([u8; 32], [u8; 32]) {
        let coinbase = self.build_coinbase(miner_address);
        let coinbase_txid = sha256d_no_witness(&coinbase);
        let merkle_root = self.compute_merkle_root(coinbase_txid);
        let header_bytes = self.build_header_prefix(merkle_root);
        let raw = sha256d(&header_bytes);
        let mut reversed = raw;
        reversed.reverse();
        (reversed, merkle_root)
    }

    fn compute_merkle_root(&self, coinbase_txid: [u8; 32]) -> [u8; 32] {
        let mut hashes: Vec<[u8; 32]> = std::iter::once(coinbase_txid)
            .chain(self.tx_data.iter().map(|raw| sha256d_no_witness(raw)))
            .collect();
        merkle_root(&mut hashes)
    }

    /// First 80 bytes of the block header (no nonce64/mix_hash).
    pub fn build_header_prefix(&self, merkle_root: [u8; 32]) -> [u8; 80] {
        let mut h = [0u8; 80];
        h[0..4].copy_from_slice(&self.version.to_le_bytes());
        let prev = hex::decode(&self.prev_hash).expect("prev_hash hex");
        h[4..36].copy_from_slice(&prev);
        h[36..68].copy_from_slice(&merkle_root);
        h[68..72].copy_from_slice(&self.time.to_le_bytes());
        h[72..76].copy_from_slice(&self.bits.to_le_bytes());
        h[76..80].copy_from_slice(&self.height.to_le_bytes());
        h
    }

    /// Serialize the full block for submitblock.
    pub fn serialize_block(
        &self,
        header_prefix: &[u8; 80],
        nonce64: u64,
        mix_hash_raw: &[u8; 32], // raw block byte order (reversed from GetHex)
        miner_address: &str,
    ) -> Vec<u8> {
        let mut block = Vec::new();
        block.extend_from_slice(header_prefix);
        block.extend_from_slice(&nonce64.to_le_bytes());
        block.extend_from_slice(mix_hash_raw);

        // Transaction count (varint): coinbase + others
        let tx_count = 1 + self.tx_data.len();
        write_varint(&mut block, tx_count as u64);

        block.extend(self.build_coinbase(miner_address));
        for tx in &self.tx_data {
            block.extend_from_slice(tx);
        }
        block
    }
}

// --- helpers ---

fn sha256d(data: &[u8]) -> [u8; 32] {
    let first = Sha256::digest(data);
    let second = Sha256::digest(first);
    second.into()
}

/// SHA256d of a transaction excluding witness data (for txid).
fn sha256d_no_witness(raw_tx: &[u8]) -> [u8; 32] {
    // Strip segwit marker/flag and witness fields to get the legacy serialization.
    // If bytes[4]==0x00 && bytes[5]==0x01 it is segwit.
    if raw_tx.len() > 6 && raw_tx[4] == 0x00 && raw_tx[5] == 0x01 {
        let stripped = strip_witness(raw_tx);
        sha256d(&stripped)
    } else {
        sha256d(raw_tx)
    }
}

fn strip_witness(tx: &[u8]) -> Vec<u8> {
    // Layout: version(4) marker(1) flag(1) inputs ... outputs ... witness ... locktime(4)
    // Re-serialize without marker/flag/witness.
    let mut out = Vec::new();
    out.extend_from_slice(&tx[0..4]); // version
    let mut pos = 6usize; // skip marker+flag

    // inputs
    let (in_count, n) = read_varint(tx, pos);
    out.extend(encode_varint(in_count));
    pos += n;
    for _ in 0..in_count {
        let start = pos;
        pos += 32 + 4; // outpoint
        let (script_len, n2) = read_varint(tx, pos);
        pos += n2 + script_len as usize;
        pos += 4; // sequence
        out.extend_from_slice(&tx[start..pos]);
    }

    // outputs
    let (out_count, n) = read_varint(tx, pos);
    out.extend(encode_varint(out_count));
    pos += n;
    for _ in 0..out_count {
        let start = pos;
        pos += 8; // value
        let (script_len, n2) = read_varint(tx, pos);
        pos += n2 + script_len as usize;
        out.extend_from_slice(&tx[start..pos]);
    }

    // skip witness data: one stack per input
    for _ in 0..in_count {
        let (items, n) = read_varint(tx, pos);
        pos += n;
        for _ in 0..items {
            let (item_len, n2) = read_varint(tx, pos);
            pos += n2 + item_len as usize;
        }
    }

    out.extend_from_slice(&tx[pos..pos + 4]); // locktime
    out
}

fn read_varint(data: &[u8], pos: usize) -> (u64, usize) {
    match data[pos] {
        v @ 0..=0xfc => (v as u64, 1),
        0xfd => (u16::from_le_bytes([data[pos + 1], data[pos + 2]]) as u64, 3),
        0xfe => (
            u32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]) as u64,
            5,
        ),
        _ => (
            u64::from_le_bytes(data[pos + 1..pos + 9].try_into().unwrap()),
            9,
        ),
    }
}

fn encode_varint(v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    write_varint(&mut out, v);
    out
}

fn write_varint(out: &mut Vec<u8>, v: u64) {
    if v < 0xfd {
        out.push(v as u8);
    } else if v <= 0xffff {
        out.push(0xfd);
        out.extend_from_slice(&(v as u16).to_le_bytes());
    } else if v <= 0xffffffff {
        out.push(0xfe);
        out.extend_from_slice(&(v as u32).to_le_bytes());
    } else {
        out.push(0xff);
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn push_var_bytes(out: &mut Vec<u8>, data: &[u8]) {
    write_varint(out, data.len() as u64);
    out.extend_from_slice(data);
}

fn merkle_root(hashes: &mut Vec<[u8; 32]>) -> [u8; 32] {
    if hashes.is_empty() {
        return [0u8; 32];
    }
    while hashes.len() > 1 {
        if hashes.len() % 2 != 0 {
            let last = *hashes.last().unwrap();
            hashes.push(last);
        }
        let mut next = Vec::with_capacity(hashes.len() / 2);
        for pair in hashes.chunks_exact(2) {
            let mut data = [0u8; 64];
            data[..32].copy_from_slice(&pair[0]);
            data[32..].copy_from_slice(&pair[1]);
            next.push(sha256d(&data));
        }
        *hashes = next;
    }
    hashes[0]
}

fn bits_to_target(bits: u32) -> [u8; 32] {
    let exp = (bits >> 24) as usize;
    let coeff = bits & 0x007fffff;
    let mut target = [0u8; 32];
    if exp <= 3 {
        let shifted = coeff >> (8 * (3 - exp));
        target[31] = (shifted & 0xff) as u8;
        target[30] = ((shifted >> 8) & 0xff) as u8;
        target[29] = ((shifted >> 16) & 0xff) as u8;
    } else {
        let pos = 32 - exp;
        if pos < 32 {
            target[pos] = ((coeff >> 16) & 0xff) as u8;
        }
        if pos + 1 < 32 {
            target[pos + 1] = ((coeff >> 8) & 0xff) as u8;
        }
        if pos + 2 < 32 {
            target[pos + 2] = (coeff & 0xff) as u8;
        }
    }
    target
}

/// Convert pool difficulty to a 32-byte boundary target.
/// diff1 = 0x00000000FFFF0000...0000 (Bitcoin/Stratum standard).
/// Uses 256-bit division via two u128 halves to avoid bignum crates.
pub fn diff_to_target(diff: u64) -> [u8; 32] {
    if diff == 0 {
        return [0xff; 32];
    }
    // diff1 high 128 bits = 0x00000000FFFF00000000000000000000
    // diff1 low  128 bits = 0x00000000000000000000000000000000
    let diff1_hi: u128 = 0x0000_0000_FFFF_0000_0000_0000_0000_0000u128;
    let d = diff as u128;

    let target_hi = diff1_hi / d;
    let rem0 = diff1_hi % d; // rem0 < d <= 2^64, fits in u128

    // Compute target_lo = rem0 * 2^128 / d in two 64-bit steps.
    // rem0 < 2^64, so rem0 << 64 < 2^128 — no overflow.
    let lo_hi = (rem0 << 64) / d;
    let rem1 = (rem0 << 64) % d; // rem1 < d <= 2^64
    let lo_lo = (rem1 << 64) / d;
    let target_lo = (lo_hi << 64) | lo_lo;

    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&target_hi.to_be_bytes());
    out[16..].copy_from_slice(&target_lo.to_be_bytes());
    out
}

fn build_coinbase_scriptsig(height: u32) -> Vec<u8> {
    let mut script = Vec::new();

    // BIP34 height push
    let height_bytes = encode_height(height);
    script.push(height_bytes.len() as u8);
    script.extend_from_slice(&height_bytes);

    // Pool tag
    let tag = b"/MEWC-SOLO/";
    script.push(tag.len() as u8);
    script.extend_from_slice(tag);

    script
}

fn encode_height(h: u32) -> Vec<u8> {
    if h == 0 {
        return vec![0x00];
    }
    let mut bytes = Vec::new();
    let mut v = h;
    while v > 0 {
        bytes.push((v & 0xff) as u8);
        v >>= 8;
    }
    // If the top bit is set, add a zero byte to keep it positive.
    if bytes.last().unwrap() & 0x80 != 0 {
        bytes.push(0x00);
    }
    bytes
}

fn p2pkh_script(address: &str) -> Vec<u8> {
    let decoded = match bs58::decode(address)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_vec()
    {
        Ok(b) if b.len() >= 21 => b,
        _ => {
            tracing::error!("p2pkh_script: invalid address '{address}'");
            return vec![0u8; 25];
        }
    };
    let hash160 = &decoded[1..21];
    let mut script = Vec::with_capacity(25);
    script.push(0x76); // OP_DUP
    script.push(0xa9); // OP_HASH160
    script.push(0x14); // push 20 bytes
    script.extend_from_slice(hash160);
    script.push(0x88); // OP_EQUALVERIFY
    script.push(0xac); // OP_CHECKSIG
    script
}
