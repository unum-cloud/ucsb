//! RocksDB key-value benchmark with native batch reads and SST ingestion.
//!
//! Requires a C++ compiler, CMake, and libclang to build the bundled engine.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features rocksdb-backend \
//!     --bin crud-eval-rocksdb -- --records 100K --threads 4
//! ```

use clap::Parser;
use crudeval::{backend::*, run, CommonArgs};
use rocksdb::{
    Direction, IngestExternalFileOptions, IteratorMode, Options, SstFileWriter, WriteBatch, WriteOptions, DB,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
};
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, default_value = "128MiB", value_parser = crudeval::workload::parse_count)]
    write_buffer_size: u64,
}
struct RocksDbBackend {
    db: DB,
    path: PathBuf,
    durability: Durability,
    write_buffer_size: usize,
    writes: Mutex<()>,
    sst_id: AtomicU64,
    ingested: AtomicBool,
}
impl RocksDbBackend {
    fn open(path: &Path, durability: Durability, write_buffer_size: usize) -> Result<Self> {
        if write_buffer_size == 0 {
            return Err("write buffer size must be positive".into());
        }
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_write_buffer_size(write_buffer_size);
        let db = DB::open(&options, path).map_err(|e| e.to_string())?;
        Ok(Self {
            db,
            path: path.into(),
            durability,
            write_buffer_size,
            writes: Mutex::new(()),
            sst_id: AtomicU64::new(0),
            ingested: AtomicBool::new(false),
        })
    }
    fn write_options(&self) -> WriteOptions {
        let mut options = WriteOptions::default();
        options.disable_wal(self.durability == Durability::None);
        options.set_sync(self.durability == Durability::Flushed);
        options
    }
}
impl Backend for RocksDbBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        BTreeMap::from([
            ("backend".into(), json!("rocksdb")),
            ("client_version".into(), json!("rocksdb 0.25.0")),
            ("library_version".into(), json!("11.8.1")),
            ("compact_after_ingestion".into(), json!(true)),
            ("durability".into(), json!(self.durability)),
            ("write_buffer_size".into(), json!(self.write_buffer_size)),
            ("conditional_writes".into(), json!("serialized")),
        ])
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            native_batch_read: true,
            native_batch_write: true,
            native_bulk_load: true,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_>> {
        Ok(Box::new(RocksDbSession(self)))
    }
    fn flush(&self) -> Result<()> {
        self.db.flush().map_err(|e| e.to_string())?;
        self.db.flush_wal(true).map_err(|e| e.to_string())?;
        if self.ingested.swap(false, Ordering::Relaxed) {
            self.db.compact_range::<&[u8], &[u8]>(None, None);
        }
        Ok(())
    }
    fn disk_bytes(&self) -> Result<u64> {
        directory_bytes(&self.path)
    }
}
struct RocksDbSession<'a>(&'a RocksDbBackend);
impl BackendSession for RocksDbSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        let mut batch = WriteBatch::default();
        for (i, key) in keys.iter().enumerate() {
            batch.put(key.as_bytes(), values.get(i).ok_or("missing input value")?);
        }
        self.0
            .db
            .write_opt(batch, &self.0.write_options())
            .map_err(|e| e.to_string())?;
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordBatch) -> Result<usize> {
        output.clear();
        let mut count = 0;
        for value in self.0.db.multi_get(keys.iter().map(Key::as_bytes)) {
            let value = value.map_err(|e| e.to_string())?;
            count += usize::from(value.is_some());
            output.push(value.as_deref());
        }
        Ok(count)
    }
    fn update(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        let _guard = self.0.writes.lock().map_err(|e| e.to_string())?;
        let mut batch = WriteBatch::default();
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            if self
                .0
                .db
                .get_pinned(key.as_bytes())
                .map_err(|e| e.to_string())?
                .is_some()
            {
                batch.put(key.as_bytes(), values.get(i).ok_or("missing input value")?);
                count += 1;
            }
        }
        self.0
            .db
            .write_opt(batch, &self.0.write_options())
            .map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let _guard = self.0.writes.lock().map_err(|e| e.to_string())?;
        let mut batch = WriteBatch::default();
        let mut count = 0;
        for key in keys {
            if self
                .0
                .db
                .get_pinned(key.as_bytes())
                .map_err(|e| e.to_string())?
                .is_some()
            {
                batch.delete(key.as_bytes());
                count += 1;
            }
        }
        self.0
            .db
            .write_opt(batch, &self.0.write_options())
            .map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn range_read(&mut self, start: Key, limit: usize, keys: &mut Vec<Key>, output: &mut RecordBatch) -> Result<usize> {
        keys.clear();
        output.clear();
        for row in self
            .0
            .db
            .iterator(IteratorMode::From(start.as_bytes(), Direction::Forward))
            .take(limit)
        {
            let (key, value) = row.map_err(|e| e.to_string())?;
            keys.push(Key::from_slice(&key).map_err(|e| e.to_string())?);
            output.push(Some(&value));
        }
        Ok(keys.len())
    }
    fn bulk_load(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let id = self.0.sst_id.fetch_add(1, Ordering::Relaxed);
        let path = self.0.path.join(format!("load-{id}.sst"));
        let options = Options::default();
        let mut writer = SstFileWriter::create(&options);
        writer.open(&path).map_err(|e| e.to_string())?;
        let result = (|| {
            for (i, key) in keys.iter().enumerate() {
                writer
                    .put(key.as_bytes(), values.get(i).ok_or("missing input value")?)
                    .map_err(|e| e.to_string())?;
            }
            writer.finish().map_err(|e| e.to_string())?;
            let mut ingest = IngestExternalFileOptions::default();
            ingest.set_move_files(true);
            self.0
                .db
                .ingest_external_file_opts(&ingest, vec![&path])
                .map_err(|e| e.to_string())?;
            self.0.ingested.store(true, Ordering::Relaxed);
            Ok(keys.len())
        })();
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
        result
    }
}
fn main() -> Result<()> {
    let cli = Cli::parse();
    run(
        cli.common,
        json!({"write_buffer_size": cli.write_buffer_size}),
        move |args, path| {
            if args.data_model != DataModel::KeyValue {
                return Err("RocksDB supports only kv data_model".into());
            }
            Ok(Box::new(RocksDbBackend::open(
                path,
                args.durability,
                usize::try_from(cli.write_buffer_size).map_err(|_| "write buffer size exceeds platform limit")?,
            )?))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_contract() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let db = RocksDbBackend::open(path, Durability::None, 8 << 20).unwrap();
        crudeval::assert_backend_contract!(&db);
    }
}
