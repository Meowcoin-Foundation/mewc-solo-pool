-- MEWC Solo Pool — Supabase Schema
-- Run this in the Supabase SQL editor to set up the pool database.
-- RLS is configured so the frontend can read everything with the anon key,
-- and only the pool process (service role key) can write.

-- ============================================================
-- BLOCKS
-- Every block found by the pool. Written immediately on discovery.
-- ============================================================
CREATE TABLE IF NOT EXISTS meowpow_blocks (
    id                bigserial PRIMARY KEY,
    height            integer       NOT NULL,
    hash              text          NOT NULL UNIQUE,
    finder_address    text          NOT NULL,
    miner_payout      numeric(20,8) NOT NULL,  -- MEWC paid to miner (after dev fee)
    dev_fee           numeric(20,8) NOT NULL DEFAULT 0,
    community_payout  numeric(20,8) NOT NULL DEFAULT 0,
    tx_fees           numeric(20,8) NOT NULL DEFAULT 0,
    found_at          timestamptz   NOT NULL DEFAULT now(),
    gap_seconds       integer,                 -- seconds since pool's previous block (null for first)
    worker            text          NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS meowpow_blocks_found_at       ON meowpow_blocks (found_at DESC);
CREATE INDEX IF NOT EXISTS meowpow_blocks_finder_address ON meowpow_blocks (finder_address);
CREATE INDEX IF NOT EXISTS meowpow_blocks_height         ON meowpow_blocks (height DESC);

-- ============================================================
-- SHARE WINDOWS
-- Aggregated share stats per miner/worker, flushed every 60 seconds.
-- Keeps the hot mining path free of any DB I/O.
-- Rows older than 90 days can be pruned — meowpow_blocks are the permanent record.
-- ============================================================
CREATE TABLE IF NOT EXISTS meowpow_share_windows (
    id              bigserial PRIMARY KEY,
    address         text          NOT NULL,
    worker          text          NOT NULL DEFAULT '',
    window_start    timestamptz   NOT NULL,
    window_end      timestamptz   NOT NULL,
    shares_valid    integer       NOT NULL DEFAULT 0,
    shares_invalid  integer       NOT NULL DEFAULT 0,
    shares_stale    integer       NOT NULL DEFAULT 0,
    difficulty_avg  numeric       NOT NULL DEFAULT 1,
    -- hashrate_mhs = shares_valid * difficulty_avg * 4294967296 / window_seconds
    -- stored pre-computed for fast frontend queries
    hashrate_mhs    numeric       NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS meowpow_share_windows_address      ON meowpow_share_windows (address, window_start DESC);
CREATE INDEX IF NOT EXISTS meowpow_share_windows_window_start ON meowpow_share_windows (window_start DESC);

-- ============================================================
-- MINER SESSIONS
-- Connect/disconnect events. Gives uptime visibility and
-- lets the frontend show currently active miners.
-- ============================================================
CREATE TABLE IF NOT EXISTS meowpow_miner_sessions (
    id               bigserial PRIMARY KEY,
    address          text        NOT NULL,
    worker           text        NOT NULL DEFAULT '',
    ip               text        NOT NULL,
    server_id        text        NOT NULL DEFAULT '',
    connected_at     timestamptz NOT NULL DEFAULT now(),
    disconnected_at  timestamptz          -- null means currently connected
);

CREATE INDEX IF NOT EXISTS meowpow_miner_sessions_address      ON meowpow_miner_sessions (address);
CREATE INDEX IF NOT EXISTS meowpow_miner_sessions_connected_at ON meowpow_miner_sessions (connected_at DESC);
CREATE INDEX IF NOT EXISTS meowpow_miner_sessions_active       ON meowpow_miner_sessions (disconnected_at) WHERE disconnected_at IS NULL;

-- ============================================================
-- USEFUL VIEWS
-- Pre-built queries the frontend can hit directly.
-- ============================================================

-- Current pool hashrate and active miner count.
-- AVG per worker (instantaneous rate), then SUM across workers for pool total.
-- peak_difficulty_since_last_block: highest 60s window avg difficulty since the last block found.
-- True per-share peak isn't stored — label this honestly on the frontend.
CREATE OR REPLACE VIEW meowpow_pool_stats AS
SELECT
    COUNT(*)                        AS active_miners,
    COALESCE(SUM(hashrate_mhs), 0)  AS pool_hashrate_mhs,
    (
        SELECT COALESCE(MAX(sw.difficulty_avg), 0)
        FROM meowpow_share_windows sw
        WHERE sw.window_end >= COALESCE(
            (SELECT MAX(found_at) FROM meowpow_blocks),
            now() - INTERVAL '24 hours'
        )
    )                               AS peak_difficulty_since_last_block
FROM (
    SELECT address, worker, AVG(NULLIF(hashrate_mhs, 0)) AS hashrate_mhs
    FROM meowpow_share_windows
    WHERE window_end > now() - INTERVAL '10 minutes'
    GROUP BY address, worker
) w;

-- Per-miner/worker hashrate. Driven purely by recent share activity —
-- no session join, so multiple servers and restarts don't cause ghost entries.
-- A miner appears here only if they have produced work in the last 15 minutes.
CREATE OR REPLACE VIEW meowpow_miner_hashrates AS
SELECT
    address,
    worker,
    COALESCE(AVG(NULLIF(hashrate_mhs, 0)), 0) AS hashrate_mhs,
    COALESCE(SUM(shares_valid), 0)             AS shares_valid,
    COALESCE(SUM(shares_invalid), 0)           AS shares_invalid,
    MAX(window_end)                            AS last_share_at,
    TRUE                                       AS is_online
FROM meowpow_share_windows
WHERE window_end > now() - INTERVAL '15 minutes'
GROUP BY address, worker
ORDER BY hashrate_mhs DESC;

-- Pool luck: actual meowpow_blocks vs expected based on shares submitted
-- Expected meowpow_blocks = sum(shares * difficulty * 2^32) / network_difficulty
-- Network difficulty is not stored here — compute in the frontend or pass as param.
CREATE OR REPLACE VIEW meowpow_pool_luck AS
SELECT
    (SELECT COUNT(*)                          FROM meowpow_blocks)         AS meowpow_blocks_found,
    (SELECT SUM(shares_valid)                 FROM meowpow_share_windows)  AS total_shares,
    (SELECT SUM(shares_valid * difficulty_avg) FROM meowpow_share_windows) AS total_work,
    (SELECT MIN(found_at)                     FROM meowpow_blocks)         AS since;

-- Historical pool hashrate, time-bucketed for charting.
-- AVG per worker per bucket, then SUM across workers = pool rate for that period.
-- Usage: SELECT * FROM meowpow_pool_hashrate_history(24, 5);
CREATE OR REPLACE FUNCTION meowpow_pool_hashrate_history(hours int DEFAULT 24, bucket_minutes int DEFAULT 5)
RETURNS TABLE (bucket timestamptz, pool_hashrate_mhs numeric) AS $$
SELECT
    time_bucket,
    SUM(avg_hashrate) AS pool_hashrate_mhs
FROM (
    SELECT
        date_trunc('minute', window_end)
            - ((EXTRACT(MINUTE FROM window_end)::int % bucket_minutes) * INTERVAL '1 minute')
            AS time_bucket,
        address,
        worker,
        AVG(NULLIF(hashrate_mhs, 0)) AS avg_hashrate
    FROM meowpow_share_windows
    WHERE window_end > now() - (hours * INTERVAL '1 hour')
    GROUP BY time_bucket, address, worker
) sub
GROUP BY time_bucket
ORDER BY time_bucket ASC;
$$ LANGUAGE sql STABLE;

-- ============================================================
-- ROW LEVEL SECURITY
-- Public (anon key): read only.
-- Pool process (service role key): full write access.
-- ============================================================
ALTER TABLE meowpow_blocks         ENABLE ROW LEVEL SECURITY;
ALTER TABLE meowpow_share_windows  ENABLE ROW LEVEL SECURITY;
ALTER TABLE meowpow_miner_sessions ENABLE ROW LEVEL SECURITY;

-- Public read on all tables
CREATE POLICY "public read meowpow_blocks"
    ON meowpow_blocks FOR SELECT TO anon USING (true);

CREATE POLICY "public read meowpow_share_windows"
    ON meowpow_share_windows FOR SELECT TO anon USING (true);

CREATE POLICY "public read meowpow_miner_sessions"
    ON meowpow_miner_sessions FOR SELECT TO anon USING (true);

-- Service role has full access (implicit in Supabase — service key bypasses RLS)
-- No additional policies needed for writes.

-- ============================================================
-- DATA RETENTION (optional — run manually or via pg_cron)
-- Keeps meowpow_share_windows lean. Blocks are permanent.
-- ============================================================
-- DELETE FROM meowpow_share_windows WHERE window_end < now() - INTERVAL '90 days';
--
-- To automate with pg_cron (enable the extension in Supabase dashboard first):
-- SELECT cron.schedule('prune-share-windows', '0 3 * * *',
--   $$DELETE FROM meowpow_share_windows WHERE window_end < now() - INTERVAL '90 days'$$);
