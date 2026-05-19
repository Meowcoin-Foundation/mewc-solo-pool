use meowpow::{light_verify, Hash256};

/// Validate a share using light_verify (no DAG required).
/// Returns the final hash bytes if valid against boundary, None otherwise.
pub fn check_share(
    header_hash_hex: &str, // GetHex form — what was sent in mining.notify
    mix_hash_hex: &str,    // GetHex form — what miner sent in mining.submit
    nonce: u64,
    boundary: &[u8; 32],  // 32-byte target (big-endian)
) -> Option<[u8; 32]> {
    let hh = hex::decode(header_hash_hex).ok()?;
    let mh = hex::decode(mix_hash_hex).ok()?;
    if hh.len() != 32 || mh.len() != 32 { return None; }

    let header_hash = Hash256::from_bytes(hh.as_slice().try_into().unwrap());
    let mix_hash = Hash256::from_bytes(mh.as_slice().try_into().unwrap());
    let boundary_hash = Hash256::from_bytes(boundary);

    let final_hash = light_verify(&header_hash, &mix_hash, nonce);

    if final_hash.is_less_or_equal(&boundary_hash) {
        Some(final_hash.to_bytes())
    } else {
        None
    }
}

/// Convert mix_hash from GetHex (what miner sends) to raw block bytes (reversed).
pub fn mix_hash_to_raw(mix_hash_hex: &str) -> Option<[u8; 32]> {
    let mut b = hex::decode(mix_hash_hex).ok()?;
    if b.len() != 32 { return None; }
    b.reverse();
    Some(b.try_into().unwrap())
}
