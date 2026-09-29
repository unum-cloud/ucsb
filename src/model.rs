//! Deterministic key-value, document, and graph records with read verification.

use crate::{
    backend::{DataModel, Key},
    data::{derive_seed, Distribution, RandomGenerator, ValuePool},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, ops::RangeInclusive};

pub struct RecordGenerator {
    data_model: DataModel,
    pool: Option<ValuePool>,
    seed: u64,
    records: u64,
    degree: usize,
}

#[derive(Serialize, Deserialize)]
struct Document {
    _id: Key,
    score: u64,
    payload: String,
}

#[derive(Serialize, Deserialize)]
struct Vertex {
    _id: Key,
    version: u64,
    neighbors: Vec<Key>,
    slots: Vec<usize>,
}

impl RecordGenerator {
    pub fn new(
        data_model: DataModel,
        seed: u64,
        sizes: RangeInclusive<usize>,
        records: u64,
        degree: usize,
    ) -> Result<Self, String> {
        if data_model == DataModel::Graph && records == 0 {
            return Err("graph workloads require at least one initial record".into());
        }
        Ok(Self {
            data_model,
            pool: if data_model == DataModel::Graph {
                None
            } else {
                Some(ValuePool::new(seed, sizes)?)
            },
            seed,
            records,
            degree: degree.min(records.saturating_sub(1) as usize),
        })
    }

    pub fn fill(&self, key: Key, version: u64, output: &mut Vec<u8>) {
        self.fill_with_floor(key, version, 0, output);
    }

    pub fn fill_with_floor(&self, key: Key, version: u64, floor: u64, output: &mut Vec<u8>) {
        match self.data_model {
            DataModel::KeyValue => self.pool.as_ref().unwrap().fill(key, version, output),
            DataModel::Documents => {
                self.pool.as_ref().unwrap().fill(key, 0, output);
                let payload = encode_hex(output);
                output.clear();
                serde_json::to_writer(
                    output,
                    &Document {
                        _id: key,
                        score: version & i64::MAX as u64,
                        payload,
                    },
                )
                .unwrap();
            }
            DataModel::Graph => {
                let version = version & i64::MAX as u64;
                let (slots, neighbors) = graph_neighbors(self.seed, key, version, self.records, self.degree)
                    .into_iter()
                    .enumerate()
                    .filter(|(_, key)| key.as_u128() >= u128::from(floor))
                    .unzip();
                output.clear();
                serde_json::to_writer(
                    output,
                    &Vertex {
                        _id: key,
                        version,
                        neighbors,
                        slots,
                    },
                )
                .unwrap();
            }
        }
    }

    pub fn modify(&self, key: Key, value: &[u8], floor: u64, output: &mut Vec<u8>) -> Result<(), String> {
        let version = match self.data_model {
            DataModel::KeyValue => {
                u64::from_be_bytes(value.get(16..24).ok_or("truncated value header")?.try_into().unwrap())
            }
            DataModel::Documents => {
                serde_json::from_slice::<Document>(value)
                    .map_err(|e| e.to_string())?
                    .score
            }
            DataModel::Graph => {
                serde_json::from_slice::<Vertex>(value)
                    .map_err(|e| e.to_string())?
                    .version
            }
        };
        self.fill_with_floor(key, version.wrapping_add(1) & i64::MAX as u64, floor, output);
        Ok(())
    }

    pub fn verify(&self, key: Key, value: &[u8]) -> Result<(), String> {
        self.verify_with_floor(key, value, 0)
    }

    pub fn verify_with_floor(&self, key: Key, value: &[u8], floor: u64) -> Result<(), String> {
        match self.data_model {
            DataModel::KeyValue => self.pool.as_ref().unwrap().verify(key, value).map(|_| ()),
            DataModel::Documents => {
                let document: Document = serde_json::from_slice(value).map_err(|e| format!("invalid document: {e}"))?;
                if document._id != key || document.score > i64::MAX as u64 {
                    return Err("document key or score mismatch".into());
                }
                let payload = decode_hex(&document.payload)?;
                if self.pool.as_ref().unwrap().verify(key, &payload)? != 0 {
                    return Err("document immutable payload version mismatch".into());
                }
                Ok(())
            }
            DataModel::Graph => {
                let vertex: Vertex = serde_json::from_slice(value).map_err(|e| format!("invalid vertex: {e}"))?;
                if vertex._id != key || vertex.version > i64::MAX as u64 {
                    return Err("vertex key or version mismatch".into());
                }
                let (slots, expected): (Vec<_>, Vec<_>) =
                    graph_neighbors(self.seed, key, vertex.version, self.records, self.degree)
                        .into_iter()
                        .enumerate()
                        .filter(|(_, key)| key.as_u128() >= u128::from(floor))
                        .unzip();
                if vertex.neighbors != expected || vertex.slots != slots {
                    return Err("vertex adjacency mismatch".into());
                }
                Ok(())
            }
        }
    }
}

pub fn graph_neighbors(seed: u64, key: Key, version: u64, records: u64, degree: usize) -> Vec<Key> {
    let degree = degree.min(records.saturating_sub(1) as usize);
    if degree == 0 {
        return Vec::new();
    }
    let source = key.as_u128();
    let source_seed = seed ^ (source as u64).rotate_left(17) ^ ((source >> 64) as u64).rotate_left(41);
    let mut selected = BTreeSet::new();
    let mut neighbors = vec![Key::nil(); degree];
    // Select stable slots first so changing slot zero cannot alter another edge.
    for slot in (1..degree).chain(std::iter::once(0)) {
        let mut rng = RandomGenerator::new(
            derive_seed(source_seed, "graph-neighbor", slot) ^ if slot == 0 { version } else { 0 },
        );
        let mut target = 0;
        for _ in 0..16 {
            target = rng.sample(Distribution::Zipf, 0..records).unwrap();
            if u128::from(target) != source && !selected.contains(&target) {
                break;
            }
        }
        while u128::from(target) == source || selected.contains(&target) {
            target = if target + 1 == records { 0 } else { target + 1 };
        }
        selected.insert(target);
        neighbors[slot] = Key::from_u128(u128::from(target));
    }
    neighbors
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        text.push(DIGITS[(byte >> 4) as usize] as char);
        text.push(DIGITS[(byte & 15) as usize] as char);
    }
    text
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err("odd document payload length".into());
    }
    fn digit(byte: u8) -> Result<u8, String> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err("invalid document payload encoding".into()),
        }
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn document_updates_preserve_payload_and_verification_detects_corruption() {
        let model = RecordGenerator::new(DataModel::Documents, 42, 32..=32, 10, 3).unwrap();
        let key = Key::from_u128(3);
        let mut bytes = Vec::new();
        model.fill(key, 0, &mut bytes);
        let first: Document = serde_json::from_slice(&bytes).unwrap();
        model.fill(key, 7, &mut bytes);
        model.verify(key, &bytes).unwrap();
        let mut updated: Document = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(first.payload, updated.payload);
        assert_eq!(updated.score, 7);
        updated.payload.replace_range(0..2, "ff");
        assert!(model.verify(key, &serde_json::to_vec(&updated).unwrap()).is_err());
    }
    #[test]
    fn graph_edges_are_unique_and_only_first_slot_can_change() {
        let key = Key::from_u128(4);
        let before = graph_neighbors(42, key, 0, 100, 8);
        let after = graph_neighbors(42, key, 123, 100, 8);
        assert_eq!(before, graph_neighbors(42, key, 0, 100, 8));
        assert_ne!(before, graph_neighbors(43, key, 0, 100, 8));
        assert_eq!(&before[1..], &after[1..]);
        assert_ne!(before[0], after[0]);
        for neighbors in [&before, &after] {
            assert_eq!(neighbors.iter().collect::<BTreeSet<_>>().len(), 8);
            assert!(!neighbors.contains(&key));
            assert!(neighbors.iter().all(|key| key.as_u128() < 100));
        }
        assert!(graph_neighbors(42, Key::nil(), 0, 1, 8).is_empty());
        assert_eq!(graph_neighbors(42, Key::nil(), 0, 4, 100).len(), 3);
    }
    #[test]
    fn graph_verification_checks_adjacency() {
        let model = RecordGenerator::new(DataModel::Graph, 42, 24..=24, 10, 3).unwrap();
        assert!(model.pool.is_none());
        let key = Key::from_u128(3);
        let mut bytes = Vec::new();
        model.fill(key, 19, &mut bytes);
        model.verify(key, &bytes).unwrap();
        model.fill_with_floor(key, 19, 3, &mut bytes);
        model.verify_with_floor(key, &bytes, 3).unwrap();
        let remaining: Vertex = serde_json::from_slice(&bytes).unwrap();
        assert!(remaining.neighbors.iter().all(|key| key.as_u128() >= 3));
        let original = graph_neighbors(42, key, 19, 10, 3);
        for (&slot, &target) in remaining.slots.iter().zip(&remaining.neighbors) {
            assert_eq!(original[slot], target);
        }
        model.fill(key, 19, &mut bytes);
        let mut vertex: Vertex = serde_json::from_slice(&bytes).unwrap();
        vertex.neighbors[0] = key;
        assert!(model.verify(key, &serde_json::to_vec(&vertex).unwrap()).is_err());
    }
}
