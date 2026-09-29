//! Worker-local latency histograms and sampled process resource usage.

use crate::Result;
use hdrhistogram::Histogram;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use sysinfo::{ProcessesToUpdate, System};

#[derive(Default, Serialize)]
pub struct LatencySummary {
    pub count: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

#[derive(Default)]
pub struct WorkloadMeasurements {
    pub elapsed: Duration,
    pub calls: u64,
    pub entries: u64,
    pub missing: u64,
    pub failed: u64,
    pub aborted: u64,
    pub corrupted: u64,
    pub processed_bytes: u64,
    pub histograms: BTreeMap<String, Histogram<u64>>,
    pub timeline: Vec<u64>,
    pub error: Option<String>,
    pub hardware_counters: Option<crate::perf_counters::CounterSample>,
}

impl WorkloadMeasurements {
    pub fn record(&mut self, operation: &str, latency: Duration, elapsed: Duration, entries: u64) -> Result<()> {
        if !self.histograms.contains_key(operation) {
            self.histograms
                .insert(operation.to_owned(), Histogram::new(3).map_err(|e| e.to_string())?);
        }
        self.histograms
            .get_mut(operation)
            .unwrap()
            .record(latency.as_nanos().min(u64::MAX as u128) as u64)
            .map_err(|e| e.to_string())?;
        let second = elapsed.as_secs() as usize;
        self.timeline.resize(self.timeline.len().max(second + 1), 0);
        self.timeline[second] += entries;
        self.calls += 1;
        self.entries += entries;
        Ok(())
    }

    pub fn merge(&mut self, other: Self) -> Result<()> {
        self.elapsed = self.elapsed.max(other.elapsed);
        self.calls += other.calls;
        self.entries += other.entries;
        self.missing += other.missing;
        self.failed += other.failed;
        self.aborted += other.aborted;
        self.corrupted += other.corrupted;
        self.processed_bytes += other.processed_bytes;
        for (name, histogram) in other.histograms {
            if let Some(existing) = self.histograms.get_mut(&name) {
                existing.add(&histogram).map_err(|e| e.to_string())?;
            } else {
                self.histograms.insert(name, histogram);
            }
        }
        self.timeline.resize(self.timeline.len().max(other.timeline.len()), 0);
        for (second, count) in other.timeline.into_iter().enumerate() {
            self.timeline[second] += count;
        }
        if self.error.is_none() {
            self.error = other.error;
        }
        if let Some(sample) = other.hardware_counters {
            let total = self.hardware_counters.get_or_insert_with(Default::default);
            total.cycles += sample.cycles;
            total.instructions += sample.instructions;
            total.cache_misses += sample.cache_misses;
            total.branch_misses += sample.branch_misses;
        }
        Ok(())
    }

    pub fn latencies(&self) -> BTreeMap<String, LatencySummary> {
        self.histograms
            .iter()
            .map(|(name, h)| {
                (
                    name.clone(),
                    LatencySummary {
                        count: h.len(),
                        p50_ns: h.value_at_quantile(0.5),
                        p90_ns: h.value_at_quantile(0.9),
                        p99_ns: h.value_at_quantile(0.99),
                        p999_ns: h.value_at_quantile(0.999),
                        max_ns: h.max(),
                    },
                )
            })
            .collect()
    }
}

#[derive(Default, Serialize)]
pub struct ResourceUsage {
    pub cpu_seconds: f64,
    pub cpu_avg_percent: f64,
    pub cpu_max_percent: f32,
    pub rss_avg_bytes: u64,
    pub rss_max_bytes: u64,
    pub virtual_memory_avg_bytes: u64,
    pub virtual_memory_max_bytes: u64,
    pub read_bytes: u64,
    pub written_bytes: u64,
}

pub struct ResourceSampler {
    stop: mpsc::Sender<()>,
    handle: JoinHandle<ResourceUsage>,
}

impl ResourceSampler {
    pub fn start() -> Self {
        let (stop, receiver) = mpsc::channel();
        let (ready, initialized) = mpsc::sync_channel(0);
        let handle = std::thread::spawn(move || {
            let pid = sysinfo::get_current_pid().unwrap();
            let mut system = System::new();
            system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
            let initial = system.process(pid).map(|p| (p.accumulated_cpu_time(), p.disk_usage()));
            let start = Instant::now();
            let _ = ready.send(());
            let mut usage = ResourceUsage::default();
            let mut samples = 0;
            let mut rss_sum = 0u128;
            let mut virtual_sum = 0u128;
            loop {
                let stopped = !matches!(
                    receiver.recv_timeout(Duration::from_millis(100)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                );
                system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
                if let Some(process) = system.process(pid) {
                    samples += 1;
                    rss_sum += process.memory() as u128;
                    virtual_sum += process.virtual_memory() as u128;
                    usage.rss_max_bytes = usage.rss_max_bytes.max(process.memory());
                    usage.virtual_memory_max_bytes = usage.virtual_memory_max_bytes.max(process.virtual_memory());
                    usage.cpu_max_percent = usage.cpu_max_percent.max(process.cpu_usage());
                    if let Some((cpu, disk)) = &initial {
                        usage.cpu_seconds = process.accumulated_cpu_time().saturating_sub(*cpu) as f64 / 1000.0;
                        usage.read_bytes = process
                            .disk_usage()
                            .total_read_bytes
                            .saturating_sub(disk.total_read_bytes);
                        usage.written_bytes = process
                            .disk_usage()
                            .total_written_bytes
                            .saturating_sub(disk.total_written_bytes);
                    }
                }
                if stopped {
                    break;
                }
            }
            usage.cpu_avg_percent = usage.cpu_seconds / start.elapsed().as_secs_f64() * 100.0;
            usage.rss_avg_bytes = (rss_sum / samples.max(1) as u128) as u64;
            usage.virtual_memory_avg_bytes = (virtual_sum / samples.max(1) as u128) as u64;
            usage
        });
        let _ = initialized.recv();
        Self { stop, handle }
    }

    pub fn stop(self) -> Result<ResourceUsage> {
        let _ = self.stop.send(());
        self.handle.join().map_err(|_| "resource sampler panicked".into())
    }
}
