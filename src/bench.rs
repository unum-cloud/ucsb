//! Shared benchmark runner: CLI sweeps, workload execution, and resumable data directories.

pub mod backend;
pub mod data;
#[cfg(feature = "tier2")]
pub mod docker;
pub mod measure;
pub mod output;
pub mod perf_counters;
pub mod workload;

pub use backend::{Backend, BackendCapabilities, BackendSession, DataModel, Durability, Key, RecordBatch, Result};
use clap::Args;
use std::path::{Path, PathBuf};

#[derive(Args, Clone, Debug, serde::Serialize)]
pub struct CommonArgs {
    /// Initial record counts; comma-separated values form a sweep.
    #[arg(long, value_delimiter = ',', default_value = "100K", value_parser = workload::parse_count)]
    pub records: Vec<u64>,
    /// Worker counts; comma-separated values form a sweep.
    #[arg(long, value_delimiter = ',', default_value = "1")]
    pub threads: Vec<usize>,
    /// Ordered comma-separated workload names.
    #[arg(long, default_value = workload::DEFAULT_WORKLOADS)]
    pub workloads: String,
    /// Override key sampling for the selected workloads.
    #[arg(long, value_enum)]
    pub distribution: Option<data::Distribution>,
    /// Attempted entries per ordinary workload, as a count or percentage.
    #[arg(long, default_value = "10%")]
    pub entries: String,
    /// Time limit instead of an entry budget, such as 30s or 2m.
    #[arg(long)]
    pub duration: Option<String>,
    /// Binary payload size or range, such as 1KiB or 100B..1KiB.
    #[arg(long, default_value = "1KiB")]
    pub value_size: String,
    /// Parent directory for isolated, marked benchmark databases.
    #[arg(long, default_value = "data")]
    pub data_dir: PathBuf,
    /// Directory for JSON reports.
    #[arg(long, default_value = "results")]
    pub output: PathBuf,
    /// Seed for data and per-worker random generators.
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
    /// Requested write durability; adapters report their effective settings.
    #[arg(long, value_enum, default_value = "none")]
    pub durability: Durability,
    /// Storage model to exercise.
    #[arg(long, value_enum, default_value = "key-value")]
    pub data_model: DataModel,
    /// Document field to update; currently /score.
    #[arg(long, default_value = "/score")]
    pub field: String,
    /// Outgoing graph degree, capped at the initial population minus one.
    #[arg(long, default_value_t = 8)]
    pub degree: usize,
    /// Calls per transaction; zero leaves transaction boundaries to the adapter.
    #[arg(long, default_value_t = 0)]
    pub transaction_size: usize,
    /// Aggregate scheduled calls per second across workers.
    #[arg(long)]
    pub rate: Option<f64>,
    /// Disable value verification while retaining structural checks.
    #[arg(long)]
    pub no_verify: bool,
    /// Record Linux worker hardware counters; requires the perf-counters feature.
    #[arg(long)]
    pub perf_counters: bool,
    /// Reopen an embedded database between workloads.
    #[arg(long)]
    pub reopen: bool,
    /// Reopen and drop the host page cache between workloads; Linux only.
    #[arg(long)]
    pub drop_caches: bool,
}

pub fn run(
    args: CommonArgs,
    backend_options: impl serde::Serialize,
    open: impl Fn(&CommonArgs, &Path) -> Result<Box<dyn Backend>>,
) -> Result<()> {
    run_benchmarks(
        args,
        serde_json::to_value(backend_options).map_err(|e| e.to_string())?,
        open,
    )
}

use data::{derive_seed, KeySpace, RandomGenerator};
use measure::{ResourceSampler, WorkloadMeasurements};
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Barrier, OnceLock,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use workload::{Operation, Workload};
pub mod model;

fn duration(value: &str) -> Result<Duration> {
    let (number, scale) = if let Some(n) = value.strip_suffix("ms") {
        (n, 0.001)
    } else if let Some(n) = value.strip_suffix('s') {
        (n, 1.0)
    } else if let Some(n) = value.strip_suffix('m') {
        (n, 60.0)
    } else {
        (value, 1.0)
    };
    let seconds = number.parse::<f64>().map_err(|_| "invalid duration")? * scale;
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err("duration must be positive and finite".into());
    }
    Duration::try_from_secs_f64(seconds).map_err(|e| e.to_string())
}

fn entry_budget(value: &str, records: u64) -> Result<u64> {
    let count = if let Some(percent) = value.strip_suffix('%') {
        let percent = percent.parse::<f64>().map_err(|_| "invalid entry percentage")?;
        if !percent.is_finite() || percent <= 0.0 || percent > 100.0 {
            return Err("entry percentage must be in (0, 100]".into());
        }
        ((records as f64 * percent / 100.0).ceil() as u64).max(1)
    } else {
        workload::parse_count(value)?
    };
    if count == 0 {
        return Err("entries must be positive".into());
    }
    Ok(count)
}

fn drop_page_cache() -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            libc::sync();
        }
        std::fs::write("/proc/sys/vm/drop_caches", b"3\n").map_err(|e| format!("drop page cache: {e}"))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("--drop-caches requires Linux".into())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DataManifest {
    format: u32,
    identity: String,
    floor: u64,
    next: u64,
    dirty: bool,
}

impl DataManifest {
    fn read(path: &Path, identity: &str) -> Result<Self> {
        let bytes = std::fs::read(path.join(".crudeval"))
            .map_err(|e| format!("unowned benchmark directory {}: {e}", path.display()))?;
        let manifest: Self = serde_json::from_slice(&bytes)
            .map_err(|e| format!("invalid benchmark manifest {}: {e}", path.display()))?;
        if manifest.format != 1 || manifest.identity != identity || manifest.floor > manifest.next {
            return Err(format!("benchmark manifest configuration mismatch: {}", path.display()));
        }
        Ok(manifest)
    }
    fn write(&self, path: &Path) -> Result<()> {
        let temporary = path.join(".crudeval.tmp");
        let file = std::fs::File::create(&temporary).map_err(|e| e.to_string())?;
        serde_json::to_writer(&file, self).map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        std::fs::rename(temporary, path.join(".crudeval")).map_err(|e| e.to_string())
    }
}

fn run_benchmarks(
    args: CommonArgs,
    backend_options: serde_json::Value,
    open: impl Fn(&CommonArgs, &Path) -> Result<Box<dyn Backend>>,
) -> Result<()> {
    if args.records.is_empty() || args.records.contains(&0) || args.threads.is_empty() || args.threads.contains(&0) {
        return Err("records and threads must be positive".into());
    }
    if args.rate.is_some_and(|r| !r.is_finite() || r <= 0.0) {
        return Err("rate must be positive and finite".into());
    }
    if args.data_model == DataModel::Documents && args.field != "/score" {
        return Err("document updates currently support --field /score".into());
    }
    if args.perf_counters && !cfg!(all(target_os = "linux", feature = "perf-counters")) {
        return Err("--perf-counters requires Linux and the perf-counters feature".into());
    }
    let sizes = workload::parse_value_size(&args.value_size)?;
    let duration = args.duration.as_deref().map(duration).transpose()?;
    let mut workloads = args
        .workloads
        .split(',')
        .map(str::parse::<Workload>)
        .collect::<Result<Vec<_>>>()?;
    if let Some(distribution) = args.distribution {
        for workload in &mut workloads {
            if workload.name == "read-latest-95-insert-5" && distribution != data::Distribution::Latest {
                return Err(
                    "read-latest-95-insert-5 requires --distribution latest or no distribution override".into(),
                );
            }
            workload.distribution = distribution;
        }
    }
    let loading = workloads[0].operations[0].0 == Operation::BulkLoad;
    if workloads
        .iter()
        .skip(1)
        .any(|w| w.operations[0].0 == Operation::BulkLoad)
    {
        return Err("bulk-load must be the first workload".into());
    }
    if args.drop_caches && !cfg!(target_os = "linux") {
        return Err("--drop-caches requires Linux".into());
    }
    let executable = std::env::current_exe().map_err(|e| e.to_string())?;
    let engine = executable.file_stem().and_then(|s| s.to_str()).unwrap_or("crudeval");
    for &records in &args.records {
        for &threads in &args.threads {
            let budget = entry_budget(&args.entries, records)?;
            let mut config = args.clone();
            config.records = vec![records];
            config.threads = vec![threads];
            let identity = output::config_hash(&(
                records,
                threads,
                args.data_model,
                args.durability,
                args.seed,
                (*sizes.start(), *sizes.end()),
                args.degree,
                &backend_options,
            ))?;
            let path = args.data_dir.join(format!("{engine}-{identity}"));
            let previous = if path.exists() {
                Some(DataManifest::read(&path, &identity)?)
            } else {
                None
            };
            let mut manifest = if loading {
                if previous.is_some() {
                    std::fs::remove_dir_all(&path).map_err(|e| e.to_string())?;
                }
                std::fs::create_dir_all(&path).map_err(|e| e.to_string())?;
                let manifest = DataManifest {
                    format: 1,
                    identity,
                    floor: 0,
                    next: 0,
                    dirty: true,
                };
                manifest.write(&path)?;
                manifest
            } else {
                let manifest = previous.ok_or("no existing benchmark data; start with bulk-load")?;
                if manifest.dirty {
                    return Err("previous mutation was interrupted or failed; restart with bulk-load".into());
                }
                manifest
            };
            let mut backend = open(&config, &path)?;
            if args.transaction_size > 0 && !backend.capabilities().transactions {
                return Err("this backend does not support --transaction-size".into());
            }
            let mut report = output::ConfigReport {
                schema_version: 1,
                started_unix_seconds: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                machine: output::collect_machine_info(),
                workload: config.clone(),
                config: backend.metadata(),
                capabilities: backend.capabilities(),
                phases: Vec::new(),
            };
            report.config.insert("key_bytes".into(), 16.into());
            report.config.insert("backend_options".into(), backend_options.clone());
            report.config.insert("initial_key_floor".into(), manifest.floor.into());
            report.config.insert("initial_key_end".into(), manifest.next.into());
            if args.perf_counters {
                report.config.insert(
                    "hardware_counters_scope".into(),
                    "benchmark worker threads; excludes engine background threads and servers".into(),
                );
            }
            let keyspace = KeySpace::restore(manifest.floor..manifest.next)?;
            let model = model::RecordGenerator::new(args.data_model, args.seed, sizes.clone(), records, args.degree)?;
            eprintln!(
                "{} · {records} records · {threads} threads · {:?}",
                engine, args.data_model
            );
            eprintln!(
                "{:<36} {:>13} {:>12} {:>10}",
                "workload", "entries/s", "p99 (µs)", "missing"
            );
            for (index, workload) in workloads.iter().enumerate() {
                if index > 0 && (args.reopen || args.drop_caches) {
                    backend.flush()?;
                    drop(backend);
                    if args.drop_caches {
                        drop_page_cache()?;
                    }
                    backend = open(&config, &path)?;
                }
                let needs_range = workload
                    .operations
                    .iter()
                    .any(|(op, _)| matches!(op, Operation::RangeRead | Operation::FullScan));
                let skipped = needs_range && !backend.capabilities().ordered_ranges;
                let mutating = !skipped
                    && workload.operations.iter().any(|(operation, _)| {
                        matches!(
                            operation,
                            Operation::Insert
                                | Operation::Update
                                | Operation::Delete
                                | Operation::ReadModifyWrite
                                | Operation::BulkLoad
                        )
                    });
                if mutating {
                    manifest.dirty = true;
                    manifest.write(&path)?;
                }
                let mut phase = if skipped {
                    empty_phase(workload, "skipped", Some("ordered ranges unsupported".into()))
                } else {
                    execute_phase(backend.as_ref(), &config, workload, &keyspace, &model, budget, duration)?
                };
                let flush_start = Instant::now();
                if phase.status != "skipped" {
                    if let Err(error) = backend.flush() {
                        phase.error = Some(error);
                        phase.status = "failed".into();
                        phase.failed += 1;
                    }
                    phase.flush_seconds = flush_start.elapsed().as_secs_f64();
                }
                if mutating && phase.status == "completed" {
                    let live = keyspace.live();
                    manifest.floor = live.start;
                    manifest.next = live.end;
                    manifest.dirty = false;
                    manifest.write(&path)?;
                }
                phase.disk_bytes = backend.disk_bytes()?;
                phase.server_usage = backend.server_usage()?;
                let p99 = phase.latency.values().map(|l| l.p99_ns).max().unwrap_or(0) as f64 / 1000.0;
                eprintln!(
                    "{:<36} {:>13.0} {:>12.1} {:>10} {}",
                    workload.name, phase.throughput, p99, phase.missing, phase.status
                );
                let error = if phase.status == "failed" {
                    phase.error.clone()
                } else {
                    None
                };
                report.phases.push(phase);
                let report_path = output::write_report(&args.output, &report)?;
                if let Some(error) = error {
                    return Err(format!("{error}; report: {}", report_path.display()));
                }
            }
        }
    }
    Ok(())
}

fn empty_phase(workload: &Workload, status: &str, error: Option<String>) -> output::WorkloadReport {
    output::WorkloadReport {
        workload: workload.name.clone(),
        status: status.into(),
        elapsed_seconds: 0.0,
        flush_seconds: 0.0,
        calls: 0,
        entries: 0,
        throughput: 0.0,
        missing: 0,
        failed: 0,
        aborted: 0,
        corrupted: 0,
        processed_bytes: 0,
        latency: Default::default(),
        timeline_entries: Vec::new(),
        client_usage: Default::default(),
        server_usage: None,
        hardware_counters: None,
        disk_bytes: 0,
        error,
    }
}

fn execute_phase(
    backend: &dyn Backend,
    args: &CommonArgs,
    workload: &Workload,
    keyspace: &KeySpace,
    model: &model::RecordGenerator,
    budget: u64,
    duration: Option<Duration>,
) -> Result<output::WorkloadReport> {
    let threads = args.threads[0];
    let barrier = Barrier::new(threads);
    let start = OnceLock::new();
    let cancelled = AtomicBool::new(false);
    let sampler = ResourceSampler::start();
    let result = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for thread in 0..threads {
            let barrier = &barrier;
            let start = &start;
            let cancelled = &cancelled;
            handles.push(scope.spawn(move || {
                let session = backend.session();
                if barrier.wait().is_leader() {
                    let _ = start.set(Instant::now());
                }
                barrier.wait();
                let mut session = session;
                let measurements = match &mut session {
                    Ok(session) => run_worker(
                        session.as_mut(),
                        args,
                        workload,
                        keyspace,
                        model,
                        budget,
                        duration,
                        thread,
                        *start.get().unwrap(),
                        cancelled,
                    ),
                    Err(error) => {
                        cancelled.store(true, Ordering::Relaxed);
                        WorkloadMeasurements {
                            failed: 1,
                            error: Some(error.clone()),
                            ..Default::default()
                        }
                    }
                };
                measurements
            }));
        }
        let mut total = WorkloadMeasurements::default();
        for handle in handles {
            total.merge(handle.join().map_err(|_| "benchmark worker panicked")?)?;
        }
        Ok::<_, String>(total)
    });
    let usage = sampler.stop()?;
    let measurements = result?;
    let elapsed = measurements.elapsed.as_secs_f64();
    let mut phase = empty_phase(
        workload,
        if measurements.error.is_some() {
            "failed"
        } else {
            "completed"
        },
        measurements.error.clone(),
    );
    phase.elapsed_seconds = elapsed;
    phase.calls = measurements.calls;
    phase.entries = measurements.entries;
    phase.throughput = measurements.entries as f64 / elapsed.max(f64::MIN_POSITIVE);
    phase.missing = measurements.missing;
    phase.failed = measurements.failed;
    phase.aborted = measurements.aborted;
    phase.corrupted = measurements.corrupted;
    phase.processed_bytes = measurements.processed_bytes;
    phase.latency = measurements.latencies();
    phase.hardware_counters = measurements.hardware_counters;
    phase.timeline_entries = measurements.timeline;
    phase.client_usage = usage;
    Ok(phase)
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    session: &mut dyn BackendSession,
    args: &CommonArgs,
    workload: &Workload,
    keyspace: &KeySpace,
    model: &model::RecordGenerator,
    budget: u64,
    duration: Option<Duration>,
    thread: usize,
    start: Instant,
    cancelled: &AtomicBool,
) -> WorkloadMeasurements {
    let threads = args.threads[0] as u64;
    let bulk_load = workload.operations[0].0 == Operation::BulkLoad;
    let full_scan = workload.operations[0].0 == Operation::FullScan;
    let live = keyspace.live();
    let total = if bulk_load {
        args.records[0]
    } else if full_scan {
        live.end - live.start
    } else {
        budget
    };
    let target = total / threads + u64::from((thread as u64) < total % threads);
    let scan_offset = (total / threads) * thread as u64 + (thread as u64).min(total % threads);
    let mut scan_start = live.start + scan_offset;
    let scan_end = scan_start + target;
    let timed = duration.filter(|_| !bulk_load && !full_scan);
    let mut rng = RandomGenerator::new(derive_seed(args.seed, &workload.name, thread));
    let mut measurements = WorkloadMeasurements::default();
    let mut pending = WorkloadMeasurements::default();
    let mut reservations = Vec::new();
    let mut keys = Vec::new();
    let mut selected = HashSet::new();
    let mut values = RecordBatch::default();
    let mut output = RecordBatch::default();
    let mut scanned_keys = Vec::new();
    let mut value = Vec::new();
    let mut attempted = 0u64;
    let mut calls = 0u64;
    let mut transaction_calls = 0usize;
    let mut in_transaction = false;
    let transaction_size = args.transaction_size;
    let mut snapshot = live;
    let mut performance_counters = None;
    let result = (|| -> Result<()> {
        if args.perf_counters {
            performance_counters =
                Some(perf_counters::PerfCounters::start().map_err(|e| format!("worker performance counters: {e}"))?);
        }
        loop {
            if cancelled.load(Ordering::Relaxed) {
                break;
            }
            if let Some(limit) = timed {
                if start.elapsed() >= limit {
                    break;
                }
            } else if attempted >= target {
                break;
            }
            let scheduled = if let Some(rate) = args.rate {
                let seconds = (calls as f64 * threads as f64 + thread as f64) / rate;
                let seconds = timed.map_or(seconds, |limit| seconds.min(limit.as_secs_f64()));
                let delay =
                    Duration::try_from_secs_f64(seconds).map_err(|_| "rate schedule exceeds supported duration")?;
                let scheduled = start
                    .checked_add(delay)
                    .ok_or("rate schedule exceeds supported duration")?;
                while let Some(wait) = scheduled.checked_duration_since(Instant::now()) {
                    if cancelled.load(Ordering::Relaxed) {
                        break;
                    }
                    std::thread::sleep(wait.min(Duration::from_millis(10)));
                }
                if cancelled.load(Ordering::Relaxed) || timed.is_some_and(|limit| start.elapsed() >= limit) {
                    break;
                }
                scheduled
            } else {
                Instant::now()
            };
            let operation = workload.choose(&mut rng);
            if calls.is_multiple_of(256) {
                snapshot = keyspace.live();
            }
            let batch_size = if operation == Operation::RangeRead {
                workload
                    .range_size
                    .as_ref()
                    .map(|r| *r.start() + rng.below((r.end() - r.start() + 1) as u64) as usize)
                    .unwrap_or(workload.batch_size)
            } else if operation == Operation::Insert && workload.operations.len() > 1 {
                1
            } else {
                workload.batch_size
            };
            let mut count = batch_size as u64;
            if timed.is_none() {
                count = count.min(target - attempted);
            }
            keys.clear();
            values.clear();
            output.clear();
            scanned_keys.clear();
            let reservation = if matches!(operation, Operation::Insert | Operation::BulkLoad) {
                let range = keyspace.reserve(count)?;
                keys.extend(range.clone().map(|k| Key::from_u128(k as u128)));
                Some(range)
            } else if operation == Operation::Delete {
                let range = keyspace.delete_oldest(count);
                keys.extend(range.map(|k| Key::from_u128(k as u128)));
                count = keys.len() as u64;
                if count == 0 {
                    break;
                }
                None
            } else if full_scan {
                if scan_start >= scan_end {
                    break;
                }
                count = count.min(scan_end - scan_start);
                keys.push(Key::from_u128(scan_start as u128));
                None
            } else {
                let live_count = snapshot.end - snapshot.start;
                if live_count == 0 {
                    return Err("workload requires existing records; start with bulk-load".into());
                }
                if operation == Operation::Read {
                    count = count.min(live_count);
                }
                let distinct = if operation == Operation::Read { count } else { 1 };
                selected.clear();
                if distinct == live_count && distinct > 1 {
                    keys.extend(snapshot.clone().map(|number| Key::from_u128(number as u128)));
                    for index in (1..keys.len()).rev() {
                        keys.swap(index, rng.below(index as u64 + 1) as usize);
                    }
                } else {
                    while keys.len() < distinct as usize {
                        let mut number = rng.sample(workload.distribution, snapshot.clone()).unwrap();
                        for _ in 0..16 {
                            if !selected.contains(&number) {
                                break;
                            }
                            number = rng.sample(workload.distribution, snapshot.clone()).unwrap();
                        }
                        while !selected.insert(number) {
                            number = if number + 1 == snapshot.end {
                                snapshot.start
                            } else {
                                number + 1
                            };
                        }
                        keys.push(Key::from_u128(number as u128));
                    }
                }
                None
            };
            if matches!(operation, Operation::Insert | Operation::BulkLoad | Operation::Update) {
                for key in &keys {
                    let version = if bulk_load { 0 } else { rng.next_u64() & i64::MAX as u64 };
                    model.fill_with_floor(*key, version, snapshot.start, &mut value);
                    values.push(Some(&value));
                }
            }
            if transaction_size > 0 && !in_transaction {
                session.begin()?;
                in_transaction = true;
            }
            let call_start = if args.rate.is_some() { scheduled } else { Instant::now() };
            let mut read_affected = None;
            let mut read_modify_latency = None;
            let affected = match operation {
                Operation::Insert => session.insert(&keys, &values)?,
                Operation::BulkLoad => session.bulk_load(&keys, &values)?,
                Operation::Update => session.update(&keys, &values)?,
                Operation::Delete => session.delete(&keys)?,
                Operation::Read => session.read(&keys, &mut output)?,
                Operation::ReadModifyWrite => {
                    let read_start = Instant::now();
                    let found = session.read(&keys, &mut output)?;
                    let read_latency = read_start.elapsed();
                    let counters = if transaction_size > 0 {
                        &mut pending
                    } else {
                        &mut measurements
                    };
                    verify_rows(args, model, &keys, &output, found as u64, snapshot.start, counters)?;
                    read_affected = Some(found as u64);
                    if found == keys.len() {
                        for (index, &key) in keys.iter().enumerate() {
                            model.modify(key, output.get(index).unwrap(), snapshot.start, &mut value)?;
                            values.push(Some(&value));
                        }
                        let update_start = Instant::now();
                        let affected = session.update(&keys, &values)?;
                        read_modify_latency = Some(read_latency + update_start.elapsed());
                        affected
                    } else {
                        read_modify_latency = Some(read_latency);
                        0
                    }
                }
                Operation::RangeRead if args.data_model == DataModel::Graph => {
                    session.expand_neighbors(keys[0], count as usize, &mut scanned_keys)?
                }
                Operation::RangeRead | Operation::FullScan => {
                    session.range_read(keys[0], count as usize, &mut scanned_keys, &mut output)?
                }
            } as u64;
            let latency = if args.rate.is_some() {
                call_start.elapsed()
            } else {
                read_modify_latency.unwrap_or_else(|| call_start.elapsed())
            };
            let counters = if transaction_size > 0 {
                &mut pending
            } else {
                &mut measurements
            };
            if affected > count {
                return Err("backend returned more entries than requested".into());
            }
            counters.missing += count - affected;
            if matches!(operation, Operation::Insert | Operation::BulkLoad) && affected != count {
                return Err("backend did not insert the complete reserved key range".into());
            }
            if matches!(operation, Operation::Read | Operation::ReadModifyWrite) {
                if read_affected.is_none() {
                    verify_rows(args, model, &keys, &output, affected, snapshot.start, counters)?;
                }
            } else if matches!(operation, Operation::RangeRead | Operation::FullScan) {
                if scanned_keys.len() != affected as usize {
                    return Err("range key count differs from affected count".into());
                }
                if args.data_model == DataModel::Graph && operation == Operation::RangeRead {
                    let unique: HashSet<_> = scanned_keys.iter().collect();
                    if unique.len() != scanned_keys.len() || scanned_keys.contains(&keys[0]) {
                        return Err("invalid graph expansion".into());
                    }
                } else {
                    if scanned_keys.windows(2).any(|pair| pair[0] >= pair[1])
                        || scanned_keys.first().is_some_and(|k| *k < keys[0])
                    {
                        return Err("range keys must be unique, ascending, and at least the start key".into());
                    }
                    if full_scan
                        && scanned_keys.iter().enumerate().any(|(index, key)| {
                            key.as_u128() != u128::from(scan_start) + index as u128
                                || key.as_u128() >= u128::from(scan_end)
                        })
                    {
                        return Err("full scan crossed its shard or skipped an existing key".into());
                    }
                    verify_rows(args, model, &scanned_keys, &output, affected, snapshot.start, counters)?;
                }
                if full_scan {
                    if affected != count {
                        return Err("full scan ended before the expected record count".into());
                    }
                    scan_start = u64::try_from(scanned_keys.last().unwrap().as_u128())
                        .map_err(|_| "invalid scan key")?
                        .checked_add(1)
                        .ok_or("scan key overflow")?;
                }
            }
            counters.processed_bytes += if values.is_empty() {
                output.bytes.len() as u64
            } else {
                values.bytes.len() as u64
            };
            counters.record(operation.as_str(), latency, start.elapsed(), affected)?;
            if let Some(range) = reservation {
                if transaction_size > 0 {
                    reservations.push(range);
                } else {
                    keyspace.acknowledge(range)?;
                }
            }
            attempted += count;
            calls += 1;
            if transaction_size > 0 {
                transaction_calls += 1;
                if transaction_calls == transaction_size {
                    commit(
                        session,
                        keyspace,
                        &mut reservations,
                        &mut pending,
                        &mut measurements,
                        start,
                    )?;
                    in_transaction = false;
                    transaction_calls = 0;
                }
            }
        }
        if in_transaction {
            commit(
                session,
                keyspace,
                &mut reservations,
                &mut pending,
                &mut measurements,
                start,
            )?;
            in_transaction = false;
        }
        Ok(())
    })();
    if let Err(error) = result {
        cancelled.store(true, Ordering::Relaxed);
        if in_transaction {
            let _ = session.rollback();
            measurements.aborted += pending.entries;
        }
        measurements.corrupted += pending.corrupted;
        measurements.failed += 1;
        measurements.error = Some(error);
    }
    measurements.elapsed = start.elapsed();
    if let Some(counters) = performance_counters {
        match counters.finish() {
            Ok(sample) => measurements.hardware_counters = Some(sample),
            Err(error) => {
                cancelled.store(true, Ordering::Relaxed);
                measurements.failed += 1;
                if measurements.error.is_none() {
                    measurements.error = Some(format!("worker performance counters: {error}"));
                }
            }
        }
    }
    measurements
}

fn commit(
    session: &mut dyn BackendSession,
    keyspace: &KeySpace,
    reservations: &mut Vec<std::ops::Range<u64>>,
    pending: &mut WorkloadMeasurements,
    measurements: &mut WorkloadMeasurements,
    start: Instant,
) -> Result<()> {
    let before = Instant::now();
    session.commit()?;
    pending.record("commit", before.elapsed(), start.elapsed(), 0)?;
    for range in reservations.drain(..) {
        keyspace.acknowledge(range)?;
    }
    measurements.merge(std::mem::take(pending))
}

fn verify_rows(
    args: &CommonArgs,
    model: &model::RecordGenerator,
    keys: &[Key],
    batch: &RecordBatch,
    affected: u64,
    floor: u64,
    counters: &mut WorkloadMeasurements,
) -> Result<()> {
    if batch.len() != keys.len()
        || batch.found.len() != keys.len()
        || batch.ends.last().copied().unwrap_or(0) != batch.bytes.len()
        || batch.ends.windows(2).any(|ends| ends[0] > ends[1])
        || batch.found.iter().filter(|&&found| found).count() as u64 != affected
    {
        return Err("backend read results do not match the requested keys".into());
    }
    if !args.no_verify {
        for (index, &key) in keys.iter().enumerate() {
            if let Some(value) = batch.get(index) {
                if let Err(error) = model.verify_with_floor(key, value, floor) {
                    counters.corrupted += 1;
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::{
        collections::BTreeMap,
        sync::{atomic::AtomicUsize, Arc, Mutex},
    };

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: CommonArgs,
    }

    #[derive(Clone, Default)]
    struct Memory {
        rows: Arc<Mutex<BTreeMap<Key, Vec<u8>>>>,
        commits: Arc<AtomicUsize>,
        fail_commit: bool,
        corrupt: bool,
        ranges: bool,
    }
    struct MemorySession<'a> {
        backend: &'a Memory,
        pending: Option<BTreeMap<Key, Option<Vec<u8>>>>,
    }
    impl Backend for Memory {
        fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
            BTreeMap::new()
        }
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities {
                ordered_ranges: self.ranges,
                transactions: true,
                ..Default::default()
            }
        }
        fn session(&self) -> Result<Box<dyn BackendSession + '_>> {
            Ok(Box::new(MemorySession {
                backend: self,
                pending: None,
            }))
        }
        fn flush(&self) -> Result<()> {
            Ok(())
        }
        fn disk_bytes(&self) -> Result<u64> {
            Ok(0)
        }
    }
    impl MemorySession<'_> {
        fn get(&self, key: &Key) -> Option<Vec<u8>> {
            if let Some(value) = self.pending.as_ref().and_then(|pending| pending.get(key)) {
                return value.clone();
            }
            self.backend.rows.lock().unwrap().get(key).cloned()
        }
        fn put(&mut self, key: Key, value: Option<Vec<u8>>) {
            if let Some(pending) = &mut self.pending {
                pending.insert(key, value);
            } else if let Some(value) = value {
                self.backend.rows.lock().unwrap().insert(key, value);
            } else {
                self.backend.rows.lock().unwrap().remove(&key);
            }
        }
    }
    impl BackendSession for MemorySession<'_> {
        fn insert(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
            for (index, &key) in keys.iter().enumerate() {
                if self.get(&key).is_some() {
                    return Err("duplicate insert".into());
                }
                self.put(key, Some(values.get(index).unwrap().to_vec()));
            }
            Ok(keys.len())
        }
        fn read(&mut self, keys: &[Key], output: &mut RecordBatch) -> Result<usize> {
            output.clear();
            for key in keys {
                let mut value = self.get(key);
                if self.backend.corrupt {
                    if let Some(value) = &mut value {
                        value[0] ^= 1;
                    }
                }
                output.push(value.as_deref());
            }
            Ok(output.found.iter().filter(|&&found| found).count())
        }
        fn update(&mut self, keys: &[Key], values: &RecordBatch) -> Result<usize> {
            let mut found = 0;
            for (index, &key) in keys.iter().enumerate() {
                if self.get(&key).is_some() {
                    self.put(key, Some(values.get(index).unwrap().to_vec()));
                    found += 1;
                }
            }
            Ok(found)
        }
        fn delete(&mut self, keys: &[Key]) -> Result<usize> {
            let mut found = 0;
            for &key in keys {
                if self.get(&key).is_some() {
                    self.put(key, None);
                    found += 1;
                }
            }
            Ok(found)
        }
        fn range_read(
            &mut self,
            start: Key,
            limit: usize,
            keys: &mut Vec<Key>,
            output: &mut RecordBatch,
        ) -> Result<usize> {
            keys.clear();
            output.clear();
            for (&key, value) in self.backend.rows.lock().unwrap().range(start..).take(limit) {
                keys.push(key);
                output.push(Some(value));
            }
            Ok(keys.len())
        }
        fn begin(&mut self) -> Result<()> {
            self.pending = Some(BTreeMap::new());
            Ok(())
        }
        fn commit(&mut self) -> Result<()> {
            if self.backend.fail_commit {
                return Err("commit failed".into());
            }
            std::thread::sleep(Duration::from_millis(5));
            let pending = self.pending.take().unwrap();
            for (key, value) in pending {
                self.put(key, value);
            }
            self.backend.commits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn rollback(&mut self) -> Result<()> {
            self.pending = None;
            Ok(())
        }
    }
    fn args(extra: &[&str]) -> CommonArgs {
        let mut argv = vec!["test", "--records", "7", "--value-size", "32"];
        argv.extend_from_slice(extra);
        Cli::parse_from(argv).args
    }
    fn model() -> model::RecordGenerator {
        model::RecordGenerator::new(DataModel::KeyValue, 42, 32..=32, 7, 0).unwrap()
    }
    fn phase(
        backend: &Memory,
        args: &CommonArgs,
        workload: &str,
        keys: &KeySpace,
        model: &model::RecordGenerator,
        budget: u64,
    ) -> output::WorkloadReport {
        execute_phase(backend, args, &workload.parse().unwrap(), keys, model, budget, None).unwrap()
    }
    #[test]
    fn small_budgets_preserve_insert_uniqueness_and_scan_shards() {
        let backend = Memory {
            ranges: true,
            ..Default::default()
        };
        let args = args(&["--threads", "4"]);
        let keys = KeySpace::new(0);
        let model = model();
        let loaded = phase(&backend, &args, "bulk-load", &keys, &model, 1);
        assert_eq!((loaded.entries, loaded.failed), (7, 0));
        assert_eq!(keys.live(), 0..7);
        let read = phase(&backend, &args, "batch-read-256", &keys, &model, 2);
        assert_eq!((read.entries, read.calls), (2, 2));
        let scan = phase(&backend, &args, "full-scan", &keys, &model, 1);
        assert_eq!((scan.entries, scan.missing, scan.failed), (7, 0, 0));
    }
    #[test]
    fn transactions_publish_after_commit_and_failed_commits_publish_nothing() {
        let args = args(&["--transaction-size", "2"]);
        let model = model();
        for fail_commit in [false, true] {
            let backend = Memory {
                fail_commit,
                ..Default::default()
            };
            let keys = KeySpace::new(0);
            let result = phase(&backend, &args, "bulk-load-2", &keys, &model, 7);
            if fail_commit {
                assert_eq!((result.entries, result.failed, result.aborted), (0, 1, 4));
                assert_eq!(keys.live(), 0..0);
                assert!(backend.rows.lock().unwrap().is_empty());
            } else {
                assert_eq!(result.entries, 7);
                assert_eq!(backend.commits.load(Ordering::Relaxed), 2);
                assert_eq!(result.latency["commit"].count, 2);
                assert!(result.elapsed_seconds >= 0.010);
                assert_eq!(keys.live(), 0..7);
            }
        }
    }
    #[test]
    fn read_modify_write_increments_observed_version_and_detects_corruption() {
        let mut backend = Memory::default();
        let args = args(&[]);
        let keys = KeySpace::new(0);
        let model = model();
        phase(&backend, &args, "bulk-load", &keys, &model, 7);
        let mut workload: Workload = "read-50-read-modify-write-50".parse().unwrap();
        workload.operations = vec![(Operation::ReadModifyWrite, 100)];
        let result = execute_phase(&backend, &args, &workload, &keys, &model, 1, None).unwrap();
        assert_eq!(result.entries, 1);
        let versions: u64 = backend
            .rows
            .lock()
            .unwrap()
            .values()
            .map(|value| u64::from_be_bytes(value[16..24].try_into().unwrap()))
            .sum();
        assert_eq!(versions, 1);
        backend.corrupt = true;
        let bad = phase(&backend, &args, "read", &keys, &model, 1);
        assert_eq!((bad.failed, bad.corrupted), (1, 1));
    }
    #[test]
    fn resumes_live_key_range_and_refuses_incomplete_mutations_or_different_backend_options() {
        let temp = tempfile::tempdir().unwrap();
        let mut backend = Memory {
            ranges: true,
            ..Default::default()
        };
        let mut args = args(&[
            "--workloads",
            "bulk-load,batch-insert-1,delete-oldest",
            "--entries",
            "2",
        ]);
        args.data_dir = temp.path().join("data");
        args.output = temp.path().join("results");
        run(args.clone(), (), |_, _| Ok(Box::new(backend.clone()))).unwrap();
        assert_eq!(backend.rows.lock().unwrap().keys().next().unwrap().as_u128(), 2);
        args.workloads = "full-scan,batch-insert-1".into();
        run(args.clone(), (), |_, _| Ok(Box::new(backend.clone()))).unwrap();
        assert_eq!(backend.rows.lock().unwrap().len(), 9);
        assert!(
            run(args.clone(), "different-server", |_, _| Ok(Box::new(backend.clone())))
                .unwrap_err()
                .contains("no existing")
        );
        args.workloads = "batch-insert-1".into();
        args.transaction_size = 1;
        backend.fail_commit = true;
        assert!(run(args.clone(), (), |_, _| Ok(Box::new(backend.clone()))).is_err());
        args.workloads = "read".into();
        assert!(run(args, (), |_, _| Ok(Box::new(backend.clone())))
            .unwrap_err()
            .contains("interrupted"));
    }

    #[test]
    fn skipped_ranges_do_not_stop_the_chain_and_rate_respects_duration() {
        let temp = tempfile::tempdir().unwrap();
        let backend = Memory::default();
        let mut args = args(&[
            "--workloads",
            "bulk-load,range-read-256,batch-insert-1",
            "--entries",
            "1",
        ]);
        args.data_dir = temp.path().join("data");
        args.output = temp.path().join("results");
        run(args.clone(), (), |_, _| Ok(Box::new(backend.clone()))).unwrap();
        assert_eq!(backend.rows.lock().unwrap().len(), 8);
        args.rate = Some(0.01);
        let keys = KeySpace::new(8);
        let started = Instant::now();
        let result = execute_phase(
            &backend,
            &args,
            &"read".parse().unwrap(),
            &keys,
            &model(),
            100,
            Some(Duration::from_millis(20)),
        )
        .unwrap();
        assert_eq!(result.calls, 1);
        assert!(result.elapsed_seconds >= 0.020);
        assert!(result.throughput <= 50.0);
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}
