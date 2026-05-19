# mewc-solo-pool

Solo stratum mining pool for Meowcoin (MEWC) using the MeowPoW algorithm. No share accounting, no payouts — miners provide their own address at login and any valid block goes directly to them via the coinbase.

## Download

Grab the latest Linux x86_64 binary from the [Releases](../../releases) page (rolling pre-release updated on every commit, tagged releases for stable).

## Requirements

- A synced Meowcoin node with RPC and ZMQ enabled
- Linux x86_64 (the released binary is statically linked against libzmq)

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
initial_difficulty = 1000

[pool]
db_path = "blocks.db"   # SQLite block log
```

### Environment variable overrides

Any config value can be overridden with an env var using the prefix `MEWC__` and `__` as the nesting separator:

```bash
MEWC_NODE__RPC_PASS=secret ./mewc-solo-pool
MEWC_STRATUM__PORT=4444 ./mewc-solo-pool
```

## Running

```bash
# Default: reads config.toml in the current directory
./mewc-solo-pool

# Custom config path
./mewc-solo-pool --config /etc/mewc/pool.toml
```

Point your GPU miner at `stratum+tcp://<host>:3333` and use your MEWC address as the username. The worker name is ignored.

```
# Example (TeamRedMiner)
./teamredminer -a meowpow -o stratum+tcp://127.0.0.1:3333 -u MYourMeowcoinAddress -p x
```

Found blocks are logged to `blocks.db` (SQLite) with height, hash, finder address, and timestamp.

## Building from source

```bash
git clone https://github.com/zach-price/mewc-solo-pool
cd mewc-solo-pool
cargo build --release
# binary at target/release/mewc-solo-pool
```

Requires Rust stable and `pkg-config` + `libssl-dev` (for reqwest TLS).

## Block log

```bash
sqlite3 blocks.db "SELECT height, hash, finder, datetime(timestamp, 'unixepoch') FROM blocks;"
```
