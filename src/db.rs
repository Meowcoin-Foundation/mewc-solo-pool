use anyhow::Result;
use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
use tracing::info;

pub type Db = SqlitePool;

pub async fn init(path: &str) -> Result<Db> {
    let url = format!("sqlite:{path}?mode=rwc");
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS blocks (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            height    INTEGER NOT NULL,
            hash      TEXT NOT NULL,
            finder    TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    info!("Block log DB ready at {path}");
    Ok(pool)
}

pub async fn log_block(pool: &Db, height: u32, hash: &str, finder: &str) -> Result<()> {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;

    sqlx::query(
        "INSERT INTO blocks (height, hash, finder, timestamp) VALUES (?, ?, ?, ?)",
    )
    .bind(height as i64)
    .bind(hash)
    .bind(finder)
    .bind(ts)
    .execute(pool)
    .await?;

    Ok(())
}
