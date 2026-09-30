//! Seeded key sampling, commit-gated key allocation, and verifiable binary values.

use std::{
    alloc::System,
    ops::{Range, RangeInclusive},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use serde::Serialize;

use crate::backend::Key;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Distribution {
    Uniform,
    Zipf,
    Latest,
}

pub struct RandomGenerator {
    state: u64,
    latest: (u64, f64, f64),
}

impl RandomGenerator {
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed,
            latest: (0, 0.0, 0.0),
        }
    }
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0xa0761d6478bd642f);
        let product = (self.state as u128) * ((self.state ^ 0xe7037ed1a0b428db) as u128);
        product as u64 ^ (product >> 64) as u64
    }
    pub fn below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0);
        let threshold = bound.wrapping_neg() % bound;
        loop {
            let value = self.next_u64();
            if value >= threshold {
                return value % bound;
            }
        }
    }
    pub fn sample(&mut self, distribution: Distribution, live: Range<u64>) -> Option<u64> {
        let count = live.end.checked_sub(live.start)?;
        if count == 0 {
            return None;
        }
        let offset = match distribution {
            Distribution::Uniform => self.below(count),
            Distribution::Zipf => {
                let rank = self.zipf_rank(
                    10_000_000_000,
                    26.46902820178302,
                    zipf_eta(10_000_000_000, 26.46902820178302),
                );
                fnv(&rank.to_le_bytes()) % count
            }
            Distribution::Latest => {
                if count == 1 {
                    return Some(live.start);
                }
                if self.latest.0 != count {
                    let zeta = zeta(count);
                    self.latest = (count, zeta, zipf_eta(count, zeta));
                }
                count - 1 - self.zipf_rank(count, self.latest.1, self.latest.2)
            }
        };
        Some(live.start + offset)
    }
    fn zipf_rank(&mut self, count: u64, zeta: f64, eta: f64) -> u64 {
        let u = (self.next_u64() >> 11) as f64 * (1.0 / ((1u64 << 53) as f64));
        let uz = u * zeta;
        if uz < 1.0 {
            0
        } else if uz < 1.0 + 0.5_f64.powf(0.99) {
            1.min(count - 1)
        } else {
            ((count as f64 * (eta * u - eta + 1.0).powf(100.0)) as u64).min(count - 1)
        }
    }
}

fn zipf_eta(count: u64, zeta: f64) -> f64 {
    (1.0 - (2.0 / count as f64).powf(0.01)) / (1.0 - (1.0 + 0.5_f64.powf(0.99)) / zeta)
}

fn zeta(count: u64) -> f64 {
    let exact = count.min(128);
    let mut sum = (1..=exact).map(|i| (i as f64).powf(-0.99)).sum::<f64>();
    if count > exact {
        // Euler–Maclaurin avoids walking a billion-item keyspace when its size changes.
        let a = exact as f64;
        let b = count as f64;
        sum += (b.powf(0.01) - a.powf(0.01)) / 0.01
            + (b.powf(-0.99) - a.powf(-0.99)) / 2.0
            + 0.99 * (a.powf(-1.99) - b.powf(-1.99)) / 12.0;
    }
    sum
}

fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(1099511628211)
    })
}

pub fn derive_seed(seed: u64, workload: &str, thread: usize) -> u64 {
    fnv(workload.as_bytes()) ^ fnv(&seed.to_le_bytes()) ^ fnv(&(thread as u64).to_le_bytes()).rotate_left(23)
}

struct Reservations {
    next: u64,
    pending: Vec<Option<u64>, System>,
}

/// Published keys include only contiguous, successfully committed insert reservations.
pub struct KeySpace {
    floor: AtomicU64,
    acknowledged: AtomicU64,
    reservations: Mutex<Reservations>,
}

impl KeySpace {
    pub fn new(initial: u64) -> Self {
        Self {
            floor: AtomicU64::new(0),
            acknowledged: AtomicU64::new(initial),
            reservations: Mutex::new(Reservations {
                next: initial,
                pending: Vec::new_in(System),
            }),
        }
    }
    pub fn restore(live: Range<u64>) -> Result<Self, String> {
        if live.start > live.end {
            return Err("invalid persisted key range".into());
        }
        let keys = Self::new(live.end);
        keys.floor.store(live.start, Ordering::Relaxed);
        Ok(keys)
    }
    pub fn live(&self) -> Range<u64> {
        let end = self.acknowledged.load(Ordering::Acquire);
        self.floor.load(Ordering::Acquire).min(end)..end
    }
    pub fn workers(&self, count: usize) {
        let mut state = self.reservations.lock().unwrap();
        assert!(state.pending.iter().all(Option::is_none));
        state.pending.clear();
        state.pending.resize(count, None);
    }
    pub fn reserve(&self, worker: usize, count: u64) -> Result<Range<u64>, String> {
        let mut state = self.reservations.lock().unwrap();
        let start = state.next;
        let end = start.checked_add(count).ok_or("key space exhausted")?;
        if count != 0 {
            if state.pending[worker].is_none() {
                state.pending[worker] = Some(start);
            }
            state.next = end;
        }
        Ok(start..end)
    }
    pub fn acknowledge(&self, worker: usize) -> Result<(), String> {
        let mut state = self.reservations.lock().unwrap();
        state.pending[worker] = None;
        let end = state
            .pending
            .iter()
            .filter_map(|&start| start)
            .min()
            .unwrap_or(state.next);
        self.acknowledged.store(end, Ordering::Release);
        Ok(())
    }
    pub fn delete_oldest(&self, count: u64) -> Range<u64> {
        let end = self.acknowledged.load(Ordering::Acquire);
        let mut start = self.floor.load(Ordering::Acquire);
        loop {
            let next = start.saturating_add(count).min(end);
            match self
                .floor
                .compare_exchange_weak(start, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return start..next,
                Err(actual) => start = actual,
            }
        }
    }
}

pub struct ValuePool {
    bytes: Vec<u8, System>,
    sizes: RangeInclusive<usize>,
}

impl ValuePool {
    pub fn new(seed: u64, sizes: RangeInclusive<usize>) -> Result<Self, String> {
        if *sizes.start() < 24 || sizes.is_empty() {
            return Err("value sizes must be at least 24 bytes and ascending".into());
        }
        let mut rng = RandomGenerator::new(seed);
        let mut bytes = Vec::with_capacity_in(64 * 1024 * 1024, System);
        bytes.resize(64 * 1024 * 1024, 0);
        for chunk in bytes.as_chunks_mut::<8>().0 {
            chunk.copy_from_slice(&rng.next_u64().to_le_bytes());
        }
        Ok(Self { bytes, sizes })
    }
    fn layout(&self, key: Key, version: u64) -> (usize, usize) {
        let hash = fnv(key.as_bytes()) ^ fnv(&version.to_le_bytes()).rotate_left(17);
        let len = *self.sizes.start() + (hash % ((*self.sizes.end() - *self.sizes.start() + 1) as u64)) as usize;
        (len, (hash.rotate_left(31) % self.bytes.len() as u64) as usize)
    }
    pub fn len(&self, key: Key, version: u64) -> usize {
        self.layout(key, version).0
    }
    pub fn fill(&self, key: Key, version: u64, output: &mut [u8]) -> Result<(), String> {
        let (len, mut offset) = self.layout(key, version);
        if output.len() != len {
            return Err("value buffer length mismatch".into());
        }
        output[..16].copy_from_slice(key.as_bytes());
        output[16..24].copy_from_slice(&version.to_be_bytes());
        let mut position = 24;
        while position < len {
            let take = (len - position).min(self.bytes.len() - offset);
            output[position..position + take].copy_from_slice(&self.bytes[offset..offset + take]);
            position += take;
            offset = 0;
        }
        Ok(())
    }
    pub fn fill_hex(&self, key: Key, output: &mut [u8]) -> Result<(), String> {
        let (len, offset) = self.layout(key, 0);
        if output.len() != len * 2 {
            return Err("document payload length mismatch".into());
        }
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (index, pair) in output.as_chunks_mut::<2>().0.iter_mut().enumerate() {
            let byte = if index < 16 {
                key.as_bytes()[index]
            } else if index < 24 {
                0
            } else {
                self.bytes[(offset + index - 24) % self.bytes.len()]
            };
            pair[0] = HEX[(byte >> 4) as usize];
            pair[1] = HEX[(byte & 15) as usize];
        }
        Ok(())
    }
    pub fn verify_hex(&self, key: Key, value: &str) -> Result<(), String> {
        let (len, offset) = self.layout(key, 0);
        if value.len() != len * 2 {
            return Err("document payload length mismatch".into());
        }
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let byte = if index < 16 {
                key.as_bytes()[index]
            } else if index < 24 {
                0
            } else {
                self.bytes[(offset + index - 24) % self.bytes.len()]
            };
            if *pair != [HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]] {
                return Err("document payload mismatch".into());
            }
        }
        Ok(())
    }
    pub fn verify(&self, key: Key, value: &[u8]) -> Result<u64, String> {
        if value.len() < 24 || value[..16] != *key.as_bytes() {
            return Err("value key header mismatch or truncated value".into());
        }
        let version = u64::from_be_bytes(value[16..24].try_into().unwrap());
        let (len, mut offset) = self.layout(key, version);
        if len != value.len() {
            return Err("value length mismatch".into());
        }
        let mut position = 24;
        while position < len {
            let take = (len - position).min(self.bytes.len() - offset);
            if value[position..position + take] != self.bytes[offset..offset + take] {
                return Err("value payload mismatch".into());
            }
            position += take;
            offset = 0;
        }
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_publish_only_after_gaps_commit() {
        let keys = KeySpace::new(10);
        keys.workers(2);
        keys.reserve(0, 5).unwrap();
        keys.reserve(1, 7).unwrap();
        keys.acknowledge(1).unwrap();
        assert_eq!(keys.live(), 0..10);
        keys.acknowledge(0).unwrap();
        assert_eq!(keys.live(), 0..22);
        assert_eq!(keys.delete_oldest(30), 0..22);
        assert_eq!(keys.live(), 22..22);
    }
    #[test]
    fn concurrent_reservations_are_unique() {
        let keys = KeySpace::new(0);
        keys.workers(8);
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let keys = &keys;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        keys.reserve(worker, 3).unwrap();
                        keys.acknowledge(worker).unwrap();
                    }
                });
            }
        });
        assert_eq!(keys.live(), 0..24000);
    }
    #[test]
    fn verification_rejects_damage_and_wrong_keys() {
        let pool = ValuePool::new(42, 24..=1024).unwrap();
        let key = Key::from_u128(123);
        let mut value = Vec::with_capacity_in(pool.len(key, 7), System);
        value.resize(pool.len(key, 7), 0);
        pool.fill(key, 7, &mut value).unwrap();
        assert_eq!(pool.verify(key, &value).unwrap(), 7);
        assert!(pool.verify(Key::from_u128(124), &value).is_err());
        let end = value.len() - 1;
        value[end] ^= 1;
        assert!(pool.verify(key, &value).is_err());
        value.pop();
        assert!(pool.verify(key, &value).is_err());
    }
    #[test]
    fn zipf_preserves_double_precision_at_billion_key_scale() {
        let mut rng = RandomGenerator::new(42);
        let zeta = 26.46902820178302;
        let eta = zipf_eta(10_000_000_000, zeta);
        let expected = [
            8_999_346,
            1_560_521_264,
            115_271_083,
            1_751_185_404,
            3_808,
            1_918_126_200,
        ];
        for rank in expected {
            assert!(rng.zipf_rank(10_000_000_000, zeta, eta).abs_diff(rank) <= 1);
        }
    }

    #[test]
    fn seeded_distributions_cover_large_keys_and_latest_skews_recent() {
        let mut first = RandomGenerator::new(42);
        let mut second = RandomGenerator::new(42);
        let mut large = false;
        let mut recent = 0;
        for _ in 0..10000 {
            let key = first.sample(Distribution::Zipf, 0..1_000_000_000).unwrap();
            assert_eq!(Some(key), second.sample(Distribution::Zipf, 0..1_000_000_000));
            large |= key > 900_000_000;
        }
        for _ in 0..10000 {
            if first.sample(Distribution::Latest, 0..1000).unwrap() >= 900 {
                recent += 1;
            }
        }
        assert!(large);
        assert!(recent > 5000);
        assert_eq!(first.sample(Distribution::Uniform, 3..3), None);
    }
}
