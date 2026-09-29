//! fjall key-value benchmark with native write batches.
//!
//! The engine is implemented in Rust; no external database installation is needed.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features fjall-backend \
//!     --bin crud-eval-fjall -- --records 100K --threads 4
//! ```

use clap::Parser;
use crudeval::{backend::*, run, CommonArgs};
use fjall::{Database, Keyspace, PersistMode};
use serde_json::json;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Mutex,
};
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
}
struct FjallBackend {
    db: Database,
    records: Keyspace,
    path: PathBuf,
    durability: Durability,
    writes: Mutex<()>,
}
impl FjallBackend {
    fn open(path: &Path, durability: Durability) -> Result<Self> {
        let db = Database::builder(path).open().map_err(|e| e.to_string())?;
        let records = db.keyspace("records", Default::default).map_err(|e| e.to_string())?;
        Ok(Self {
            db,
            records,
            path: path.into(),
            durability,
            writes: Mutex::new(()),
        })
    }
    fn batch(&self) -> fjall::OwnedWriteBatch {
        self.db.batch().durability(match self.durability {
            Durability::None => None,
            Durability::Buffered => Some(PersistMode::Buffer),
            Durability::Flushed => Some(PersistMode::SyncAll),
        })
    }
}
impl Backend for FjallBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([
            ("backend".into(), json!("fjall")),
            ("library_version".into(), json!("3.1.10")),
            ("durability".into(), json!(self.durability)),
            ("conditional_writes".into(), json!("serialized")),
        ])
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            native_batch_write: true,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_>> {
        Ok(Box::new(FjallSession(self)))
    }
    fn flush(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncAll).map_err(|e| e.to_string())
    }
    fn disk_bytes(&self) -> Result<u64> {
        directory_bytes(&self.path)
    }
}
struct FjallSession<'a>(&'a FjallBackend);
impl BackendSession for FjallSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        let mut batch = self.0.batch();
        for (i, key) in keys.iter().enumerate() {
            batch.insert(
                &self.0.records,
                key.as_bytes().as_slice(),
                values.get(i).ok_or("missing input value")?,
            );
        }
        batch.commit().map_err(|e| e.to_string())?;
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordBatch) -> Result<usize> {
        output.clear();
        let mut count = 0;
        for key in keys {
            let value = self.0.records.get(key.as_bytes()).map_err(|e| e.to_string())?;
            count += usize::from(value.is_some());
            output.push(value.as_deref());
        }
        Ok(count)
    }
    fn update(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        let _guard = self.0.writes.lock().map_err(|e| e.to_string())?;
        let mut batch = self.0.batch();
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            if self.0.records.contains_key(key.as_bytes()).map_err(|e| e.to_string())? {
                batch.insert(
                    &self.0.records,
                    key.as_bytes().as_slice(),
                    values.get(i).ok_or("missing input value")?,
                );
                count += 1;
            }
        }
        batch.commit().map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let _guard = self.0.writes.lock().map_err(|e| e.to_string())?;
        let mut batch = self.0.batch();
        let mut count = 0;
        for key in keys {
            if self.0.records.contains_key(key.as_bytes()).map_err(|e| e.to_string())? {
                batch.remove(&self.0.records, key.as_bytes().as_slice());
                count += 1;
            }
        }
        batch.commit().map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn range_read(&mut self, start: Key, limit: usize, keys: &mut Vec<Key>, output: &mut RecordBatch) -> Result<usize> {
        keys.clear();
        output.clear();
        for row in self.0.records.range(start.as_bytes().as_slice()..).take(limit) {
            let (key, value) = row.into_inner().map_err(|e| e.to_string())?;
            keys.push(Key::from_slice(&key).map_err(|e| e.to_string())?);
            output.push(Some(&value));
        }
        Ok(keys.len())
    }
}
fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli.common, (), |args, path| {
        if args.data_model != DataModel::KeyValue {
            return Err("fjall supports only kv data_model".into());
        }
        Ok(Box::new(FjallBackend::open(path, args.durability)?))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_contract() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let db = FjallBackend::open(path, Durability::None).unwrap();
        crudeval::assert_backend_contract!(&db);
    }
}
