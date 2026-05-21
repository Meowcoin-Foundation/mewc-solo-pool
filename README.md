# mewc-solo-pool

Solo stratum mining pool for Meowcoin (MEWC) using the MeowPoW algorithm. No share accounting, no payouts — miners provide their own address at login and any valid block goes directly to them via the coinbase.

## Download

Grab the latest Linux x86_64 binary from the [Releases](../../releases) page (rolling pre-release updated on every commit, tagged releases for stable).

## Requirements

- A synced Meowcoin node with RPC and ZMQ enabled
- Linux x86_64 (the released binary is statically linked against libzmq)
- A [Supabase](https://supabase.com) project for stats and block logging

### Node config (`meowcoin.conf`)

```ini
server=1
rpcuser=user
rpcpassword=yourpassword
rpcport=8766
zmqpubhashblock=tcp://127.0.0.1:28332
```

## Configuration

Copy the example and edit it:

```bash
cp config.toml.example config.toml
```

```toml
[node]
rpc_url  = "http://127.0.0.1:8766"
rpc_user = "user"
rpc_pass = "yourpassword"
zmq_url  = "tcp://127.0.0.1:28332"

[stratum]
port               = 3333
initial_difficulty = 1
# Dev fee address — also used as the fallback for miners that send an invalid address.
fee_address = "YourMEWCAddress"
fee_percent = 1

[pool]
# Supabase project URL (e.g. https://xxxx.supabase.co)
supabase_url = "https://your-project.supabase.co"
# Service role key from Supabase dashboard → Settings → API
supabase_service_key = "eyJ..."
```

### Environment variable overrides

Any config value can be overridden with an env var using the prefix `MEWC_` and `__` as the nesting separator:

```bash
MEWC_NODE__RPC_PASS=secret ./mewc-solo-pool
MEWC_STRATUM__PORT=4444 ./mewc-solo-pool
```

## Supabase setup

Run the SQL in `supabase/schema.sql` against your Supabase project once to create the tables, indexes, views, and RLS policies. The schema includes:

- `meowpow_blocks` — every block found with height, hash, finder address, payout split, and time since last block
- `meowpow_share_windows` — 60-second share aggregates per miner (valid/invalid/stale counts, average difficulty, estimated hashrate)
- `meowpow_miner_sessions` — connect/disconnect events per miner
- Views for pool stats, per-miner hashrates, and pool luck

All tables allow public `SELECT` (safe for a frontend to query directly with the anon key) and restrict writes to the service role key used by the pool daemon.

## Dev fee

A configurable percentage of each coinbase reward is split to `fee_address` at the time a block is found. The community fund vout (enforced by Meowcoin consensus) is always preserved. Set `fee_percent = 0` to disable.

## Running

```bash
# Default: reads config.toml in the current directory
./mewc-solo-pool

# Custom config path
./mewc-solo-pool --config /etc/mewc/pool.toml
```

Point your GPU miner at `stratum+tcp://<host>:3333` and use your MEWC address as the username. Worker name is optional.

```
# Example (TeamRedMiner)
./teamredminer -a meowpow -o stratum+tcp://127.0.0.1:3333 -u MYourMeowcoinAddress.rig1 -p x

# Example (SRBMiner)
./SRBMiner-MULTI --algorithm meowpow --pool stratum+tcp://127.0.0.1:3333 --wallet MYourMeowcoinAddress
```

## Building from source

```bash
git clone https://github.com/zach-price/mewc-solo-pool
cd mewc-solo-pool
cargo build --release
# binary at target/release/mewc-solo-pool
```

Requires Rust stable, `pkg-config`, `libssl-dev`, and `libzmq3-dev`.
