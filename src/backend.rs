//! Storage contracts, ordered UUID keys, and reusable record batches.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type Key = uuid::Uuid;
pub type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DataModel {
    #[default]
    KeyValue,
    Documents,
    Graph,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Durability {
    #[default]
    None,
    Buffered,
    Flushed,
}

/// Reusable rows, with missing entries retaining their position in the request.
#[derive(Default, Debug)]
pub struct RecordBatch {
    pub bytes: Vec<u8>,
    pub ends: Vec<usize>,
    pub found: Vec<bool>,
}

impl RecordBatch {
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.ends.clear();
        self.found.clear();
    }

    pub fn push(&mut self, value: Option<&[u8]>) {
        if let Some(value) = value {
            self.bytes.extend_from_slice(value);
        }
        self.ends.push(self.bytes.len());
        self.found.push(value.is_some());
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if !self.found[index] {
            return None;
        }
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        Some(&self.bytes[start..self.ends[index]])
    }

    pub fn len(&self) -> usize {
        self.ends.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct BackendCapabilities {
    pub ordered_ranges: bool,
    pub transactions: bool,
    pub native_batch_read: bool,
    pub native_batch_write: bool,
    pub native_bulk_load: bool,
}

pub trait Backend: Sync {
    fn metadata(&self) -> BTreeMap<String, Value>;
    fn capabilities(&self) -> BackendCapabilities;
    fn session(&self) -> Result<Box<dyn BackendSession + '_>>;
    fn flush(&self) -> Result<()>;
    fn disk_bytes(&self) -> Result<u64>;
    fn server_usage(&self) -> Result<Option<Value>> {
        Ok(None)
    }
}

/// Methods return affected entries; updates and deletes never create missing rows.
pub trait BackendSession {
    /// Insert fresh, distinct keys reserved by the runner.
    fn insert(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize>;
    fn read(&mut self, keys: &[Key], output: &mut RecordBatch) -> Result<usize>;
    fn update(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize>;
    fn delete(&mut self, keys: &[Key]) -> Result<usize>;
    /// Inclusive lower bound, ascending unique keys, at most `limit` rows.
    fn range_read(&mut self, start: Key, limit: usize, keys: &mut Vec<Key>, output: &mut RecordBatch) -> Result<usize>;
    fn bulk_load(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
        self.insert(keys, values)
    }
    fn expand_neighbors(&mut self, _start: Key, _limit: usize, _keys: &mut Vec<Key>) -> Result<usize> {
        Err("graph expansion unsupported".into())
    }
    fn begin(&mut self) -> Result<()> {
        Err("transactions unsupported".into())
    }
    fn commit(&mut self) -> Result<()> {
        Err("transactions unsupported".into())
    }
    fn rollback(&mut self) -> Result<()> {
        Err("transactions unsupported".into())
    }
}

pub fn directory_bytes(path: &std::path::Path) -> Result<u64> {
    let mut bytes = 0;
    if !path.exists() {
        return Ok(0);
    }
    for entry in std::fs::read_dir(path).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let metadata = entry.metadata().map_err(|e| e.to_string())?;
        bytes += if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(bytes)
}

#[doc(hidden)]
#[macro_export]
macro_rules! assert_backend_contract {
    ($db:expr) => {{
        use $crate::backend::{Backend, Key, RecordBatch};
        let db: &dyn Backend = $db;
        let mut session = db.session().unwrap();
        let keys = [Key::from_u128(2), Key::from_u128(4), Key::from_u128(256)];
        let mut values = RecordBatch::default();
        values.push(Some(b"first"));
        values.push(Some(b""));
        values.push(Some(b"last value"));
        assert_eq!(session.bulk_load(&keys, &values).unwrap(), 3);
        let missing = Key::from_u128(3);
        let mut output = RecordBatch::default();
        assert_eq!(
            session
                .read(&[keys[0], missing, keys[1], keys[2]], &mut output)
                .unwrap(),
            3
        );
        assert_eq!(output.get(0), Some(b"first".as_slice()));
        assert_eq!(output.get(1), None);
        assert_eq!(output.get(2), Some(b"".as_slice()));
        assert_eq!(output.get(3), Some(b"last value".as_slice()));
        let mut changes = RecordBatch::default();
        changes.push(Some(b"replacement"));
        changes.push(Some(b"absent"));
        assert_eq!(session.update(&[keys[0], missing], &changes).unwrap(), 1);
        let mut scanned = Vec::new();
        assert_eq!(session.range_read(missing, 10, &mut scanned, &mut output).unwrap(), 2);
        assert_eq!(scanned, keys[1..]);
        assert_eq!(output.get(1), Some(b"last value".as_slice()));
        assert_eq!(session.range_read(keys[0], 0, &mut scanned, &mut output).unwrap(), 0);
        assert!(scanned.is_empty() && output.is_empty());
        assert_eq!(session.delete(&[missing, keys[1]]).unwrap(), 1);
        assert_eq!(session.read(&[keys[0], keys[1], missing], &mut output).unwrap(), 1);
        assert_eq!(output.get(0), Some(b"replacement".as_slice()));
        assert_eq!(output.get(1), None);
        assert_eq!(output.get(2), None);
        if db.capabilities().transactions {
            session.begin().unwrap();
            session.insert(&[missing], &values).unwrap();
            assert_eq!(session.read(&[missing], &mut output).unwrap(), 1);
            session.rollback().unwrap();
            assert_eq!(session.read(&[missing], &mut output).unwrap(), 0);
            session.begin().unwrap();
            session.insert(&[missing], &values).unwrap();
            session.commit().unwrap();
            assert_eq!(session.read(&[missing], &mut output).unwrap(), 1);
        }
        drop(session);
        db.flush().unwrap();
        assert!(db.disk_bytes().unwrap() > 0);
    }};
}
