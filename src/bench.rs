//! Shared benchmark runner: CLI sweeps, workload execution, and resumable data directories.

#![feature(allocator_ext)]
#![cfg_attr(test, feature(btreemap_alloc))]

pub mod backend;
pub mod data;
#[cfg(feature = "tier2")]
pub mod docker;
pub mod measure;
pub mod output;
pub mod perf_counters;
pub mod workload;

use std::path::{Path, PathBuf};

use clap::Args;

pub use crate::backend::{
    Backend, BackendCapabilities, BackendSession, DataModel, Durability, Key, RecordBatch, Result,
};

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
    open: impl Fn(&CommonArgs, &Path) -> Result<Box<dyn Backend, System>>,
) -> Result<()> {
    run_benchmarks(
        args,
        serde_json::to_value(backend_options).map_err(|e| e.to_string())?,
        open,
    )
}

use std::{
    alloc::System,
    sync::{
        atomic::{AtomicBool, Ordering},
        Barrier, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    data::{derive_seed, KeySpace, RandomGenerator},
    measure::{ResourceSampler, WorkloadMeasurements},
    workload::{Operation, Workload},
};

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
    open: impl Fn(&CommonArgs, &Path) -> Result<Box<dyn Backend, System>>,
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
    if workloads.iter().any(|workload| {
        workload.operations.iter().any(|(op, _)| *op == Operation::Delete)
            && workload
                .operations
                .iter()
                .any(|(op, _)| matches!(op, Operation::Insert | Operation::Update | Operation::ReadModifyWrite))
    }) {
        return Err("delete workloads cannot mix insertion or updates".into());
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
                2u32,
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
                schema_version: 2,
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
            report.config.insert("adapter_revision".into(), 2.into());
            report.config.insert(
                "latency_scope".into(),
                "attempted calls, including failures and rolled-back transactions".into(),
            );
            report.config.insert(
                "throughput_scope".into(),
                "successful entries published after commit".into(),
            );
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
        timeline_entries: Vec::new_in(System),
        timeline_buffer_growths: 0,
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
    keyspace.workers(threads);
    let barrier = Barrier::new(threads);
    let start = OnceLock::new();
    let cancelled = AtomicBool::new(false);
    let sampler = ResourceSampler::start();
    let result = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity_in(threads, System);
        for thread in 0..threads {
            let barrier = &barrier;
            let start = &start;
            let cancelled = &cancelled;
            handles.push(scope.spawn(move || {
                let session = backend.session();
                let live = keyspace.live();
                let total = match workload.operations[0].0 {
                    Operation::BulkLoad => args.records[0],
                    Operation::FullScan => live.end - live.start,
                    _ => budget,
                };
                let target = total / threads as u64 + u64::from((thread as u64) < total % threads as u64);
                let buffers = WorkerBuffers::new(
                    args,
                    workload,
                    model,
                    target,
                    duration.filter(|_| !matches!(workload.operations[0].0, Operation::BulkLoad | Operation::FullScan)),
                );
                if barrier.wait().is_leader() {
                    let _ = start.set(Instant::now());
                }
                barrier.wait();
                let measurements = match (session, buffers) {
                    (Ok(mut session), Ok(buffers)) => run_worker(
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
                        buffers,
                    ),
                    (Err(error), _) | (_, Err(error)) => {
                        cancelled.store(true, Ordering::Relaxed);
                        WorkloadMeasurements {
                            failed: 1,
                            error: Some(error),
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
    phase.timeline_buffer_growths = measurements.timeline_buffer_growths;
    phase.client_usage = usage;
    Ok(phase)
}

enum WorkerSession<'a> {
    KeyValue(&'a mut dyn BackendSession),
    Documents(&'a mut dyn backend::DocumentSession),
    Graph(&'a mut dyn backend::GraphSession),
}
impl<'a> WorkerSession<'a> {
    fn select(session: &'a mut dyn BackendSession, data_model: DataModel) -> Result<Self> {
        match data_model {
            DataModel::KeyValue => Ok(Self::KeyValue(session)),
            DataModel::Documents => session
                .documents()
                .map(Self::Documents)
                .ok_or("document records unsupported".into()),
            DataModel::Graph => session
                .graph()
                .map(Self::Graph)
                .ok_or("graph records unsupported".into()),
        }
    }
    fn transaction(&mut self) -> &mut dyn backend::TransactionSession {
        match self {
            Self::KeyValue(s) => *s,
            Self::Documents(s) => *s,
            Self::Graph(s) => *s,
        }
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        match self {
            Self::KeyValue(s) => s.delete(keys),
            Self::Documents(s) => s.delete(keys),
            Self::Graph(s) => s.delete(keys),
        }
    }
}
enum WorkerRecords {
    KeyValue {
        input: RecordBatch,
        output: RecordBatch,
    },
    Documents {
        input: backend::DocumentBatch,
        output: backend::DocumentBatch,
        patches: Vec<backend::DocumentPatch, System>,
    },
    Graph {
        input: backend::GraphBatch,
        output: backend::GraphBatch,
        patches: Vec<backend::GraphPatch, System>,
        scratch: Vec<backend::GraphEdge, System>,
    },
}
impl WorkerRecords {
    fn new(data_model: DataModel, rows: usize, generator: &model::RecordGenerator) -> Result<Self> {
        Ok(match data_model {
            DataModel::KeyValue => {
                let bytes = rows
                    .checked_mul(generator.max_value_size())
                    .ok_or("record buffers exceed address space")?;
                Self::KeyValue {
                    input: RecordBatch::new(rows, bytes),
                    output: RecordBatch::new(rows, bytes),
                }
            }
            DataModel::Documents => {
                let bytes = rows
                    .checked_mul(generator.max_value_size())
                    .and_then(|n| n.checked_mul(2))
                    .ok_or("document buffers exceed address space")?;
                let mut patches = Vec::with_capacity_in(rows, System);
                patches.resize(rows, backend::DocumentPatch::default());
                Self::Documents {
                    input: backend::DocumentBatch::new(rows, bytes),
                    output: backend::DocumentBatch::new(rows, bytes),
                    patches,
                }
            }
            DataModel::Graph => {
                let edges = rows
                    .checked_mul(generator.degree())
                    .ok_or("graph buffers exceed address space")?;
                let mut patches = Vec::with_capacity_in(rows, System);
                patches.resize(rows, backend::GraphPatch::default());
                let mut scratch = Vec::with_capacity_in(generator.degree(), System);
                scratch.resize(generator.degree(), backend::GraphEdge::default());
                Self::Graph {
                    input: backend::GraphBatch::new(rows, edges),
                    output: backend::GraphBatch::new(rows, edges),
                    patches,
                    scratch,
                }
            }
        })
    }
    fn prepare(
        &mut self,
        generator: &model::RecordGenerator,
        keys: &[Key],
        operation: Operation,
        floor: u64,
        rng: &mut RandomGenerator,
    ) -> Result<()> {
        let updating = operation == Operation::Update;
        match self {
            Self::KeyValue { input, .. } => {
                let mut out = input.as_output();
                for &key in keys {
                    generator.fill_value(
                        key,
                        if operation == Operation::BulkLoad {
                            0
                        } else {
                            rng.next_u64() & i64::MAX as u64
                        },
                        &mut out,
                    )?;
                }
            }
            Self::Documents { input, patches, .. } => {
                if updating {
                    for patch in &mut patches[..keys.len()] {
                        patch.score = rng.next_u64() & i64::MAX as u64;
                    }
                } else {
                    let mut out = input.as_output();
                    for &key in keys {
                        generator.fill_document(
                            key,
                            if operation == Operation::BulkLoad {
                                0
                            } else {
                                rng.next_u64() & i64::MAX as u64
                            },
                            &mut out,
                        )?;
                    }
                }
            }
            Self::Graph {
                input,
                patches,
                scratch,
                ..
            } => {
                if updating {
                    for (index, &key) in keys.iter().enumerate() {
                        patches[index] = generator.graph_patch(key, rng.next_u64(), floor, scratch);
                    }
                } else {
                    let mut out = input.as_output();
                    for &key in keys {
                        generator.fill_vertex(
                            key,
                            if operation == Operation::BulkLoad {
                                0
                            } else {
                                rng.next_u64()
                            },
                            floor,
                            scratch,
                            &mut out,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
    fn modify(&mut self, generator: &model::RecordGenerator, keys: &[Key], floor: u64) -> Result<()> {
        match self {
            Self::KeyValue { input, output } => {
                let values = output.as_input();
                let mut out = input.as_output();
                for (index, &key) in keys.iter().enumerate() {
                    let value = values.get(index).ok_or("missing read-modify-write value")?;
                    let header = value.get(16..24).ok_or("truncated read-modify-write header")?;
                    let version = u64::from_be_bytes(header.try_into().unwrap());
                    generator.fill_value(key, model::next_version(version), &mut out)?;
                }
            }
            Self::Documents { output, patches, .. } => {
                let values = output.as_input();
                for (index, patch) in patches[..keys.len()].iter_mut().enumerate() {
                    patch.score =
                        model::next_version(values.get(index).ok_or("missing read-modify-write document")?.score);
                }
            }
            Self::Graph {
                output,
                patches,
                scratch,
                ..
            } => {
                let values = output.as_input();
                for (index, &key) in keys.iter().enumerate() {
                    let version =
                        model::next_version(values.get(index).ok_or("missing read-modify-write vertex")?.version);
                    patches[index] = generator.graph_patch(key, version, floor, scratch);
                }
            }
        }
        Ok(())
    }
    fn read(&mut self, session: &mut WorkerSession<'_>, keys: &[Key]) -> Result<usize> {
        match (self, session) {
            (Self::KeyValue { output, .. }, WorkerSession::KeyValue(s)) => s.read(keys, &mut output.as_output()),
            (Self::Documents { output, .. }, WorkerSession::Documents(s)) => s.read(keys, &mut output.as_output()),
            (Self::Graph { output, .. }, WorkerSession::Graph(s)) => s.read(keys, &mut output.as_output()),
            _ => unreachable!(),
        }
    }
    fn write(&mut self, session: &mut WorkerSession<'_>, keys: &[Key], operation: Operation) -> Result<usize> {
        match (self, session) {
            (Self::KeyValue { input, .. }, WorkerSession::KeyValue(s)) => match operation {
                Operation::BulkLoad => s.bulk_load(keys, &input.as_input()),
                Operation::Insert => s.insert(keys, &input.as_input()),
                _ => s.update(keys, &input.as_input()),
            },
            (Self::Documents { input, patches, .. }, WorkerSession::Documents(s)) => match operation {
                Operation::BulkLoad => s.bulk_load(keys, &input.as_input()),
                Operation::Insert => s.insert(keys, &input.as_input()),
                _ => s.update(keys, &patches[..keys.len()]),
            },
            (Self::Graph { input, patches, .. }, WorkerSession::Graph(s)) => match operation {
                Operation::BulkLoad => s.bulk_load(keys, &input.as_input()),
                Operation::Insert => s.insert(keys, &input.as_input()),
                _ => s.update(keys, &patches[..keys.len()]),
            },
            _ => unreachable!(),
        }
    }
    fn range_read(
        &mut self,
        session: &mut WorkerSession<'_>,
        start: Key,
        limit: usize,
        keys: &mut backend::KeysOutput<'_>,
    ) -> Result<usize> {
        match (self, session) {
            (Self::KeyValue { output, .. }, WorkerSession::KeyValue(s)) => {
                s.range_read(start, limit, keys, &mut output.as_output())
            }
            (Self::Documents { output, .. }, WorkerSession::Documents(s)) => {
                s.range_read(start, limit, keys, &mut output.as_output())
            }
            (Self::Graph { output, .. }, WorkerSession::Graph(s)) => {
                s.range_read(start, limit, keys, &mut output.as_output())
            }
            _ => unreachable!(),
        }
    }
    fn verify(
        &mut self,
        generator: &model::RecordGenerator,
        keys: &[Key],
        affected: usize,
        floor: u64,
        verify: bool,
    ) -> Result<()> {
        let (rows, found) = match self {
            Self::KeyValue { output, .. } => {
                let rows = output.as_input();
                if verify {
                    for (index, &key) in keys.iter().enumerate().take(rows.len()) {
                        if let Some(value) = rows.get(index) {
                            generator.verify_value(key, value)?;
                        }
                    }
                }
                (rows.len(), rows.found.iter().filter(|&&f| f).count())
            }
            Self::Documents { output, .. } => {
                let rows = output.as_input();
                if verify {
                    for (index, &key) in keys.iter().enumerate().take(rows.len()) {
                        if let Some(value) = rows.get(index) {
                            generator.verify_document(key, value)?;
                        }
                    }
                }
                (rows.len(), rows.payloads.found.iter().filter(|&&f| f).count())
            }
            Self::Graph { output, scratch, .. } => {
                let rows = output.as_input();
                if verify {
                    for (index, &key) in keys.iter().enumerate().take(rows.len()) {
                        if let Some(value) = rows.get(index) {
                            generator.verify_vertex(key, value, floor, scratch)?;
                        }
                    }
                }
                (rows.len(), rows.found.iter().filter(|&&f| f).count())
            }
        };
        if rows != keys.len() || found != affected {
            return Err("backend results do not match requested rows".into());
        }
        Ok(())
    }
    fn bytes(&self, operation: Operation, rows: usize) -> u64 {
        let writing = matches!(
            operation,
            Operation::Insert | Operation::BulkLoad | Operation::Update | Operation::ReadModifyWrite
        );
        let updating = matches!(operation, Operation::Update | Operation::ReadModifyWrite);
        match self {
            Self::KeyValue { input, output } => (if writing { input } else { output }).as_input().bytes.len() as u64,
            Self::Documents { input, output, .. } => {
                if updating {
                    rows as u64 * 8
                } else {
                    let batch = (if writing { input } else { output }).as_input();
                    batch.payloads.bytes.len() as u64 + batch.len() as u64 * 8
                }
            }
            Self::Graph { input, output, .. } => {
                if updating {
                    rows as u64 * 24
                } else {
                    let batch = (if writing { input } else { output }).as_input();
                    batch.edges.len() as u64 * 20 + batch.len() as u64 * 8
                }
            }
        }
    }
}
struct WorkerBuffers {
    records: WorkerRecords,
    keys: Vec<Key, System>,
    scanned: Vec<Key, System>,
    selected: Vec<u64, System>,
    measurements: WorkloadMeasurements,
}
impl WorkerBuffers {
    fn new(
        args: &CommonArgs,
        workload: &Workload,
        generator: &model::RecordGenerator,
        target: u64,
        duration: Option<Duration>,
    ) -> Result<Self> {
        let batch = workload
            .range_size
            .as_ref()
            .map_or(workload.batch_size, |range| *range.end())
            .max(workload.batch_size);
        let rows = if duration.is_some() {
            batch
        } else {
            batch.min(target.max(1) as usize)
        };
        let mut scanned = Vec::with_capacity_in(rows, System);
        scanned.resize(rows, Key::nil());
        let slots = rows
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or("batch size exceeds address space")?;
        let mut selected = Vec::with_capacity_in(slots, System);
        selected.resize(slots, u64::MAX);
        let histogram_names = workload.operations.iter().map(|(operation, _)| *operation);
        let timeline = match duration {
            Some(limit) => usize::try_from(limit.as_secs())
                .ok()
                .and_then(|seconds| seconds.checked_add(2))
                .ok_or("timeline exceeds address space")?,
            None => 3600,
        };
        Ok(Self {
            records: WorkerRecords::new(args.data_model, rows, generator)?,
            keys: Vec::with_capacity_in(rows, System),
            scanned,
            selected,
            measurements: WorkloadMeasurements::new(histogram_names, args.transaction_size > 0, timeline)?,
        })
    }
    fn select(&mut self, key: u64) -> bool {
        let mask = self.selected.len() - 1;
        let mut slot = (key.wrapping_mul(0x9e3779b97f4a7c15) >> 32) as usize & mask;
        loop {
            let value = self.selected[slot];
            if value == key {
                return false;
            }
            if value == u64::MAX {
                self.selected[slot] = key;
                return true;
            }
            slot = (slot + 1) & mask;
        }
    }
}
#[derive(Default)]
struct PendingCounts {
    entries: u64,
    missing: u64,
    bytes: u64,
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    session: &mut dyn BackendSession,
    args: &CommonArgs,
    workload: &Workload,
    keyspace: &KeySpace,
    generator: &model::RecordGenerator,
    budget: u64,
    duration: Option<Duration>,
    thread: usize,
    start: Instant,
    cancelled: &AtomicBool,
    mut buffers: WorkerBuffers,
) -> WorkloadMeasurements {
    let threads = args.threads[0] as u64;
    let bulk_load = workload.operations[0].0 == Operation::BulkLoad;
    let full_scan = workload.operations[0].0 == Operation::FullScan;
    let mut snapshot = keyspace.live();
    let total = if bulk_load {
        args.records[0]
    } else if full_scan {
        snapshot.end - snapshot.start
    } else {
        budget
    };
    let target = total / threads + u64::from((thread as u64) < total % threads);
    let mut scan_start = snapshot.start + (total / threads) * thread as u64 + (thread as u64).min(total % threads);
    let scan_end = scan_start + target;
    let timed = duration.filter(|_| !bulk_load && !full_scan);
    let mut rng = RandomGenerator::new(derive_seed(args.seed, &workload.name, thread));
    let mut pending = PendingCounts::default();
    let mut attempted = 0;
    let mut calls = 0u64;
    let mut transaction_calls = 0;
    let mut in_transaction = false;
    let mut performance_counters = None;
    let mut session = match WorkerSession::select(session, args.data_model) {
        Ok(s) => s,
        Err(error) => {
            buffers.measurements.failed = 1;
            buffers.measurements.error = Some(error);
            cancelled.store(true, Ordering::Relaxed);
            return buffers.measurements;
        }
    };
    let result = (|| -> Result<()> {
        if args.perf_counters {
            performance_counters = Some(perf_counters::PerfCounters::start().map_err(|e| e.to_string())?);
        }
        loop {
            if cancelled.load(Ordering::Relaxed)
                || timed.is_some_and(|limit| start.elapsed() >= limit)
                || timed.is_none() && attempted >= target
            {
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
            let batch = if operation == Operation::RangeRead {
                workload.range_size.as_ref().map_or(workload.batch_size, |range| {
                    *range.start() + rng.below((range.end() - range.start() + 1) as u64) as usize
                })
            } else if operation == Operation::Insert && workload.operations.len() > 1 {
                1
            } else {
                workload.batch_size
            };
            let mut count = batch as u64;
            if timed.is_none() {
                count = count.min(target - attempted);
            }
            buffers.keys.clear();
            if matches!(operation, Operation::Insert | Operation::BulkLoad) {
                let range = keyspace.reserve(thread, count)?;
                buffers.keys.extend(range.map(|key| Key::from_u128(u128::from(key))));
            } else if operation == Operation::Delete {
                buffers
                    .keys
                    .extend(keyspace.delete_oldest(count).map(|key| Key::from_u128(u128::from(key))));
                count = buffers.keys.len() as u64;
                if count == 0 {
                    break;
                }
            } else if full_scan {
                if scan_start >= scan_end {
                    break;
                }
                count = count.min(scan_end - scan_start);
                buffers.keys.push(Key::from_u128(u128::from(scan_start)));
            } else {
                let live_count = snapshot.end - snapshot.start;
                if live_count == 0 {
                    return Err("workload requires existing records; start with bulk-load".into());
                }
                if operation == Operation::Read {
                    count = count.min(live_count);
                }
                let distinct = if operation == Operation::Read { count } else { 1 };
                if distinct == 1 {
                    buffers.keys.push(Key::from_u128(u128::from(
                        rng.sample(workload.distribution, snapshot.clone()).unwrap(),
                    )));
                } else if distinct == live_count {
                    buffers
                        .keys
                        .extend(snapshot.clone().map(|key| Key::from_u128(u128::from(key))));
                    for index in (1..buffers.keys.len()).rev() {
                        buffers.keys.swap(index, rng.below(index as u64 + 1) as usize);
                    }
                } else {
                    buffers.selected.fill(u64::MAX);
                    while buffers.keys.len() < distinct as usize {
                        let mut key = rng.sample(workload.distribution, snapshot.clone()).unwrap();
                        let mut accepted = false;
                        for _ in 0..16 {
                            if buffers.select(key) {
                                accepted = true;
                                break;
                            }
                            key = rng.sample(workload.distribution, snapshot.clone()).unwrap();
                        }
                        if !accepted {
                            while !buffers.select(key) {
                                key = if key + 1 == snapshot.end {
                                    snapshot.start
                                } else {
                                    key + 1
                                };
                            }
                        }
                        buffers.keys.push(Key::from_u128(u128::from(key)));
                    }
                }
            }
            if matches!(operation, Operation::Insert | Operation::BulkLoad | Operation::Update) {
                buffers
                    .records
                    .prepare(generator, &buffers.keys, operation, snapshot.start, &mut rng)?;
            }
            if args.transaction_size > 0 && !in_transaction {
                session.transaction().begin()?;
                in_transaction = true;
            }
            let call_start = Instant::now();
            let mut storage_time = Duration::ZERO;
            let mut scan_count = 0;
            let call = (|| -> Result<usize> {
                match operation {
                    Operation::Insert | Operation::BulkLoad | Operation::Update => {
                        buffers.records.write(&mut session, &buffers.keys, operation)
                    }
                    Operation::Delete => session.delete(&buffers.keys),
                    Operation::Read => buffers.records.read(&mut session, &buffers.keys),
                    Operation::ReadModifyWrite => {
                        let before = Instant::now();
                        let result = buffers.records.read(&mut session, &buffers.keys);
                        storage_time += before.elapsed();
                        let found = result?;
                        if let Err(error) =
                            buffers
                                .records
                                .verify(generator, &buffers.keys, found, snapshot.start, !args.no_verify)
                        {
                            buffers.measurements.corrupted += 1;
                            return Err(error);
                        }
                        if found != buffers.keys.len() {
                            return Ok(0);
                        }
                        buffers.records.modify(generator, &buffers.keys, snapshot.start)?;
                        let before = Instant::now();
                        let result = buffers.records.write(&mut session, &buffers.keys, Operation::Update);
                        storage_time += before.elapsed();
                        result
                    }
                    Operation::RangeRead | Operation::FullScan => {
                        let mut keys = backend::KeysOutput::new(&mut buffers.scanned);
                        let result = if operation == Operation::RangeRead {
                            if let WorkerSession::Graph(graph) = &mut session {
                                graph.expand_neighbors(buffers.keys[0], count as usize, &mut keys)
                            } else {
                                buffers
                                    .records
                                    .range_read(&mut session, buffers.keys[0], count as usize, &mut keys)
                            }
                        } else {
                            buffers
                                .records
                                .range_read(&mut session, buffers.keys[0], count as usize, &mut keys)
                        };
                        scan_count = keys.len();
                        result
                    }
                }
            })();
            let latency = if args.rate.is_some() {
                scheduled.elapsed()
            } else if operation == Operation::ReadModifyWrite {
                storage_time
            } else {
                call_start.elapsed()
            };
            buffers.measurements.record_attempt(operation, latency)?;
            let affected = call? as u64;
            if affected > count {
                return Err("backend returned more entries than requested".into());
            }
            if matches!(operation, Operation::Insert | Operation::BulkLoad) && affected != count {
                return Err("backend did not insert the complete reserved key range".into());
            }
            if operation == Operation::Read {
                if let Err(error) = buffers.records.verify(
                    generator,
                    &buffers.keys,
                    affected as usize,
                    snapshot.start,
                    !args.no_verify,
                ) {
                    buffers.measurements.corrupted += 1;
                    return Err(error);
                }
            }
            if matches!(operation, Operation::RangeRead | Operation::FullScan) {
                let keys = &buffers.scanned[..scan_count];
                if scan_count != affected as usize {
                    return Err("range key count differs from affected count".into());
                }
                if args.data_model == DataModel::Graph && operation == Operation::RangeRead {
                    for (index, key) in keys.iter().enumerate() {
                        if *key == buffers.keys[0] || keys[..index].contains(key) {
                            return Err("invalid graph expansion".into());
                        }
                    }
                } else {
                    if keys.windows(2).any(|pair| pair[0] >= pair[1])
                        || keys.first().is_some_and(|key| *key < buffers.keys[0])
                    {
                        return Err("range keys must be unique, ascending, and at least start".into());
                    }
                    if full_scan
                        && keys.iter().enumerate().any(|(index, key)| {
                            key.as_u128() != u128::from(scan_start) + index as u128
                                || key.as_u128() >= u128::from(scan_end)
                        })
                    {
                        return Err("full scan crossed its shard or skipped an existing key".into());
                    }
                    if let Err(error) =
                        buffers
                            .records
                            .verify(generator, keys, affected as usize, snapshot.start, !args.no_verify)
                    {
                        buffers.measurements.corrupted += 1;
                        return Err(error);
                    }
                }
                if full_scan {
                    if affected != count {
                        return Err("full scan ended before expected record count".into());
                    }
                    scan_start = scan_start.checked_add(affected).ok_or("scan key overflow")?;
                }
            }
            let bytes = if affected == 0
                || operation == Operation::Delete
                || args.data_model == DataModel::Graph && operation == Operation::RangeRead
            {
                0
            } else {
                buffers.records.bytes(operation, affected as usize)
            };
            if args.transaction_size > 0 {
                pending.entries += affected;
                pending.missing += count - affected;
                pending.bytes += bytes;
            } else {
                buffers
                    .measurements
                    .publish(start.elapsed(), affected, count - affected, bytes);
                if matches!(operation, Operation::Insert | Operation::BulkLoad) {
                    keyspace.acknowledge(thread)?;
                }
            }
            attempted += count;
            calls += 1;
            if args.transaction_size > 0 {
                transaction_calls += 1;
                if transaction_calls == args.transaction_size {
                    commit(
                        &mut session,
                        keyspace,
                        thread,
                        &mut pending,
                        &mut buffers.measurements,
                        start,
                    )?;
                    in_transaction = false;
                    transaction_calls = 0;
                }
            }
        }
        if in_transaction {
            commit(
                &mut session,
                keyspace,
                thread,
                &mut pending,
                &mut buffers.measurements,
                start,
            )?;
            in_transaction = false;
        }
        Ok(())
    })();
    if let Err(error) = result {
        cancelled.store(true, Ordering::Relaxed);
        if in_transaction {
            let _ = session.transaction().rollback();
            buffers.measurements.aborted += pending.entries;
        }
        buffers.measurements.failed += 1;
        buffers.measurements.error = Some(error);
    }
    buffers.measurements.elapsed = start.elapsed();
    if let Some(counters) = performance_counters {
        match counters.finish() {
            Ok(sample) => buffers.measurements.hardware_counters = Some(sample),
            Err(error) => {
                cancelled.store(true, Ordering::Relaxed);
                buffers.measurements.failed += 1;
                if buffers.measurements.error.is_none() {
                    buffers.measurements.error = Some(error.to_string());
                }
            }
        }
    }
    buffers.measurements
}
fn commit(
    session: &mut WorkerSession<'_>,
    keyspace: &KeySpace,
    thread: usize,
    pending: &mut PendingCounts,
    measurements: &mut WorkloadMeasurements,
    start: Instant,
) -> Result<()> {
    let before = Instant::now();
    let result = session.transaction().commit();
    measurements.record_commit(before.elapsed())?;
    result?;
    keyspace.acknowledge(thread)?;
    measurements.publish(start.elapsed(), pending.entries, pending.missing, pending.bytes);
    *pending = PendingCounts::default();
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{atomic::AtomicUsize, Arc, Mutex},
    };

    use clap::Parser;

    use super::*;

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
        fn metadata(&self) -> BTreeMap<String, serde_json::Value, System> {
            BTreeMap::new_in(System)
        }
        fn capabilities(&self) -> BackendCapabilities {
            BackendCapabilities {
                ordered_ranges: self.ranges,
                transactions: true,
                ..Default::default()
            }
        }
        fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
            Ok(Box::new_in(
                MemorySession {
                    backend: self,
                    pending: None,
                },
                System,
            ))
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
        fn insert(&mut self, keys: &[Key], values: &backend::RecordInput<'_>) -> Result<usize> {
            for (index, &key) in keys.iter().enumerate() {
                if self.get(&key).is_some() {
                    return Err("duplicate insert".into());
                }
                self.put(key, Some(values.get(index).unwrap().to_vec()));
            }
            Ok(keys.len())
        }
        fn read(&mut self, keys: &[Key], output: &mut backend::RecordOutput<'_>) -> Result<usize> {
            output.clear();
            let mut found = 0;
            for key in keys {
                let mut value = self.get(key);
                if self.backend.corrupt {
                    if let Some(value) = &mut value {
                        value[0] ^= 1;
                    }
                }
                found += usize::from(value.is_some());
                output.push(value.as_deref())?;
            }
            Ok(found)
        }
        fn update(&mut self, keys: &[Key], values: &backend::RecordInput<'_>) -> Result<usize> {
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
            keys: &mut backend::KeysOutput<'_>,
            output: &mut backend::RecordOutput<'_>,
        ) -> Result<usize> {
            keys.clear();
            output.clear();
            for (&key, value) in self.backend.rows.lock().unwrap().range(start..).take(limit) {
                keys.push(key)?;
                output.push(Some(value))?;
            }
            Ok(keys.len())
        }
    }
    impl backend::TransactionSession for MemorySession<'_> {
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
                assert_eq!(result.calls, 3);
                assert_eq!(result.latency["commit"].count, 1);
                assert_eq!(result.latency["bulk-load"].count, 2);
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
        run(args.clone(), (), |_, _| Ok(Box::new_in(backend.clone(), System))).unwrap();
        assert_eq!(backend.rows.lock().unwrap().keys().next().unwrap().as_u128(), 2);
        args.workloads = "full-scan,batch-insert-1".into();
        run(args.clone(), (), |_, _| Ok(Box::new_in(backend.clone(), System))).unwrap();
        assert_eq!(backend.rows.lock().unwrap().len(), 9);
        assert!(run(args.clone(), "different-server", |_, _| Ok(Box::new_in(
            backend.clone(),
            System
        )))
        .unwrap_err()
        .contains("no existing"));
        args.workloads = "batch-insert-1".into();
        args.transaction_size = 1;
        backend.fail_commit = true;
        assert!(run(args.clone(), (), |_, _| Ok(Box::new_in(backend.clone(), System))).is_err());
        args.workloads = "read".into();
        assert!(run(args, (), |_, _| Ok(Box::new_in(backend.clone(), System)))
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
        run(args.clone(), (), |_, _| Ok(Box::new_in(backend.clone(), System))).unwrap();
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
