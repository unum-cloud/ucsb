//! SQLite benchmark for key-value records and JSONB documents.
//!
//! Requires a C compiler to build the bundled SQLite engine.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features sqlite-backend \
//!     --bin crud-eval-sqlite -- --records 100K --threads 4
//! ```

use clap::Parser;
use crudeval::{backend::*, run, CommonArgs};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::json;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, default_value_t = 64)]
    cache_mib: u32,
}

struct SqliteBackend {
    path: PathBuf,
    durability: Durability,
    docs: bool,
    cache_mib: u32,
    keeper: Mutex<Connection>,
}
struct SqliteSession {
    connection: Connection,
    docs: bool,
}
impl SqliteBackend {
    fn connect(&self) -> Result<Connection> {
        let connection = Connection::open(&self.path).map_err(|e| e.to_string())?;
        connection
            .busy_timeout(Duration::from_secs(60))
            .map_err(|e| e.to_string())?;
        connection
            .pragma_update(
                None,
                "synchronous",
                match self.durability {
                    Durability::None => "OFF",
                    Durability::Buffered => "NORMAL",
                    Durability::Flushed => "FULL",
                },
            )
            .map_err(|e| e.to_string())?;
        connection
            .pragma_update(None, "cache_size", -(i64::from(self.cache_mib) * 1024))
            .map_err(|e| e.to_string())?;
        Ok(connection)
    }
    fn open(path: &Path, durability: Durability, docs: bool, cache_mib: u32) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
        let path = path.join("sqlite.db");
        let keeper = Connection::open(&path).map_err(|e| e.to_string())?;
        keeper
            .busy_timeout(Duration::from_secs(60))
            .map_err(|e| e.to_string())?;
        keeper.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS records (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;").map_err(|e| e.to_string())?;
        Ok(Self {
            path,
            durability,
            docs,
            cache_mib,
            keeper: Mutex::new(keeper),
        })
    }
}
impl Backend for SqliteBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([
            ("backend".into(), json!("sqlite")),
            ("library_version".into(), json!(rusqlite::version())),
            ("durability".into(), json!(self.durability)),
            ("journal_mode".into(), json!("WAL")),
            (
                "synchronous".into(),
                json!(match self.durability {
                    Durability::None => "OFF",
                    Durability::Buffered => "NORMAL",
                    Durability::Flushed => "FULL",
                }),
            ),
            (
                "document_storage".into(),
                json!(if self.docs { "JSONB" } else { "none" }),
            ),
            ("cache_mib_per_session".into(), json!(self.cache_mib)),
        ])
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            transactions: true,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_>> {
        Ok(Box::new(SqliteSession {
            connection: self.connect()?,
            docs: self.docs,
        }))
    }
    fn flush(&self) -> Result<()> {
        let connection = self.keeper.lock().map_err(|e| e.to_string())?;
        let (busy, _, _): (i64, i64, i64) = connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .map_err(|e| e.to_string())?;
        if busy != 0 {
            return Err("SQLite checkpoint blocked by an active transaction".into());
        }
        Ok(())
    }
    fn disk_bytes(&self) -> Result<u64> {
        directory_bytes(self.path.parent().unwrap())
    }
}
impl BackendSession for SqliteSession {
    fn insert(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        let transaction = (keys.len() > 1 && self.connection.is_autocommit())
            .then(|| self.connection.unchecked_transaction())
            .transpose()
            .map_err(|e| e.to_string())?;
        let mut statement = self
            .connection
            .prepare_cached(if self.docs {
                "INSERT INTO records (key,value) VALUES (?1,jsonb(CAST(?2 AS TEXT)))"
            } else {
                "INSERT INTO records (key,value) VALUES (?1,?2)"
            })
            .map_err(|e| e.to_string())?;
        for (i, key) in keys.iter().enumerate() {
            statement
                .execute(params![key.as_bytes().as_slice(), values.get(i)])
                .map_err(|e| e.to_string())?;
        }
        drop(statement);
        if let Some(transaction) = transaction {
            transaction.commit().map_err(|e| e.to_string())?;
        }
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordBatch) -> Result<usize> {
        output.clear();
        let mut statement = self
            .connection
            .prepare_cached(if self.docs {
                "SELECT json(value) FROM records WHERE key=?1"
            } else {
                "SELECT value FROM records WHERE key=?1"
            })
            .map_err(|e| e.to_string())?;
        let mut found = 0;
        for key in keys {
            let value = statement
                .query_row([key.as_bytes().as_slice()], |row| {
                    let value = row.get_ref(0)?.as_bytes().map_err(rusqlite::Error::from)?;
                    output.push(Some(value));
                    Ok(())
                })
                .optional()
                .map_err(|e| e.to_string())?;
            found += usize::from(value.is_some());
            if value.is_none() {
                output.push(None);
            }
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        let transaction = (keys.len() > 1 && self.connection.is_autocommit())
            .then(|| self.connection.unchecked_transaction())
            .transpose()
            .map_err(|e| e.to_string())?;
        let sql = if self.docs {
            "UPDATE records SET value=jsonb_set(value,'$.score',json_extract(CAST(?2 AS TEXT),'$.score')) WHERE key=?1"
        } else {
            "UPDATE records SET value=?2 WHERE key=?1"
        };
        let mut statement = self.connection.prepare_cached(sql).map_err(|e| e.to_string())?;
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            count += statement
                .execute(params![key.as_bytes().as_slice(), values.get(i)])
                .map_err(|e| e.to_string())?;
        }
        drop(statement);
        if let Some(transaction) = transaction {
            transaction.commit().map_err(|e| e.to_string())?;
        }
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let transaction = (keys.len() > 1 && self.connection.is_autocommit())
            .then(|| self.connection.unchecked_transaction())
            .transpose()
            .map_err(|e| e.to_string())?;
        let mut statement = self
            .connection
            .prepare_cached("DELETE FROM records WHERE key=?1")
            .map_err(|e| e.to_string())?;
        let mut count = 0;
        for key in keys {
            count += statement
                .execute([key.as_bytes().as_slice()])
                .map_err(|e| e.to_string())?;
        }
        drop(statement);
        if let Some(transaction) = transaction {
            transaction.commit().map_err(|e| e.to_string())?;
        }
        Ok(count)
    }
    fn range_read(&mut self, start: Key, limit: usize, keys: &mut Vec<Key>, output: &mut RecordBatch) -> Result<usize> {
        keys.clear();
        output.clear();
        let mut statement = self
            .connection
            .prepare_cached(if self.docs {
                "SELECT key,json(value) FROM records WHERE key>=?1 ORDER BY key LIMIT ?2"
            } else {
                "SELECT key,value FROM records WHERE key>=?1 ORDER BY key LIMIT ?2"
            })
            .map_err(|e| e.to_string())?;
        let mut rows = statement
            .query(params![
                start.as_bytes().as_slice(),
                i64::try_from(limit).unwrap_or(i64::MAX)
            ])
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let key = row
                .get_ref(0)
                .map_err(|e| e.to_string())?
                .as_blob()
                .map_err(|e| e.to_string())?;
            let value = row
                .get_ref(1)
                .map_err(|e| e.to_string())?
                .as_bytes()
                .map_err(|e| e.to_string())?;
            keys.push(Key::from_slice(key).map_err(|e| e.to_string())?);
            output.push(Some(value));
        }
        Ok(keys.len())
    }
    fn begin(&mut self) -> Result<()> {
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| e.to_string())
    }
    fn commit(&mut self) -> Result<()> {
        self.connection.execute_batch("COMMIT").map_err(|e| e.to_string())
    }
    fn rollback(&mut self) -> Result<()> {
        self.connection.execute_batch("ROLLBACK").map_err(|e| e.to_string())
    }
}
fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli.common, json!({"cache_mib": cli.cache_mib}), move |args, path| {
        if args.data_model == DataModel::Graph {
            return Err("SQLite graph data_model is unsupported".into());
        }
        if args.data_model == DataModel::Documents && args.field != "/score" {
            return Err("SQLite document updates support only /score".into());
        }
        Ok(Box::new(SqliteBackend::open(
            path,
            args.durability,
            args.data_model == DataModel::Documents,
            cli.cache_mib,
        )?))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_contract() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let db = SqliteBackend::open(path, Durability::None, false, 8).unwrap();
        crudeval::assert_backend_contract!(&db);
    }

    #[test]
    fn document_update_and_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let db = SqliteBackend::open(directory.path(), Durability::Flushed, true, 8).unwrap();
        let mut session = db.session().unwrap();
        let key = Key::from_u128(1);
        let mut values = RecordBatch::default();
        values.push(Some(br#"{"score":1,"payload":"keep"}"#));
        session.insert(&[key], &values).unwrap();
        values.clear();
        values.push(Some(br#"{"score":2,"payload":"replace"}"#));
        session.begin().unwrap();
        session.update(&[key], &values).unwrap();
        session.rollback().unwrap();
        let mut output = RecordBatch::default();
        session.read(&[key], &mut output).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(output.get(0).unwrap()).unwrap(),
            json!({"score":1,"payload":"keep"})
        );
        session.begin().unwrap();
        session.update(&[key], &values).unwrap();
        session.commit().unwrap();
        session.read(&[key], &mut output).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(output.get(0).unwrap()).unwrap(),
            json!({"score":2,"payload":"keep"})
        );
    }
}
