# Source

`bench.rs` is the library root.
Each engine has one binary source file with its own typed command-line options and storage implementation.
The shared runner owns workload execution, validation, measurement, and reports.

## Responsibilities

| Source | Responsibility |
| :-- | :-- |
| `bench.rs` | `CommonArgs`, configuration sweeps, data manifests, worker coordination, entry budgets, transaction boundaries |
| `backend.rs` | `Backend`, `BackendSession`, `BackendCapabilities`, `RecordBatch`, `Key`, `DataModel`, `Durability` |
| `workload.rs` | Explicit workload names, operation shares, batch sizes, checked count and size parsing |
| `data.rs` | `KeySpace`, `RandomGenerator`, distributions, deterministic value pool |
| `model.rs` | `RecordGenerator`: key-value payloads, documents, graph adjacency, verification |
| `measure.rs` | `WorkloadMeasurements`, `LatencySummary`, `ResourceUsage`, `ResourceSampler` |
| `output.rs` | `WorkloadReport`, `ConfigReport`, `MachineInfo`, configuration hashes, atomic report writes |
| `perf_counters.rs` | Optional Linux counters for benchmark worker threads |
| `docker.rs` | `ContainerHandle` and `NetworkHandle` lifecycle, readiness, server resource snapshots |
| `cypher.rs` | Shared graph operations for Neo4j, Memgraph, and FalkorDB |
| `rocksdb.rs`, `lmdb.rs`, `redb.rs`, `fjall.rs`, `sqlite.rs` | Embedded engine binaries |
| `redis.rs`, `mongodb.rs`, `postgres.rs`, `neo4j.rs`, `falkordb.rs` | Server binaries and their supported server variants |

## Vocabulary and contracts

`Backend` owns the database and is shared across workers.
Each worker creates and uses its own `BackendSession`; sessions need not implement `Send`.
Storage operations are `insert`, `read`, `update`, `delete`, `bulk_load`, `range_read`, and `expand_neighbors`.
Methods return affected record counts; updates and deletes never create missing records.

`RecordBatch` stores contiguous value bytes, row boundaries, and presence flags.
Point reads retain request order and a position for every missing key; an empty value is present.
`range_read` uses an inclusive lower bound and returns ascending, unique keys with aligned values.
A full scan partitions the live key range into disjoint contiguous worker shards.

`Key` is a UUID whose big-endian bytes preserve integer order.
`KeySpace` reserves globally unique insert ranges and publishes them only after successful writes or commits.
Deletes claim the oldest live keys.
The data manifest records the live floor and next key after successful mutating workloads; interrupted mutations require a fresh bulk load.
Backend options participate in directory identity, preventing different server choices or engine settings from sharing a dataset.

`DataModel` selects `KeyValue`, `Documents`, or `Graph` through `--data-model key-value|documents|graph`.
Documents update `/score` while retaining their deterministic payload.
Graph updates change edge slot zero; slot identities survive deletion of incoming edges.
Read-modify-write derives the next value from the value actually read.

## Capabilities and measurement

`BackendCapabilities` describes the implemented adapter, including ordered ranges, transactions, native batches, and native bulk loading.
Unsupported range workloads are reported as skipped and the remaining chain continues.
LMDB, redb, SQLite, and PostgreSQL expose explicit transactions; the other adapters reject `--transaction-size`.
Redis-family adapters do not expose ordered ranges.
RocksDB uses SST ingestion for bulk loading.
SQLite and MongoDB support key-value records and documents; PostgreSQL also supports graphs.
Neo4j, Memgraph, and FalkorDB support graph workloads.

Throughput counts successful records, while latency distributions group storage calls by operation.
Transaction commits are timed and uncommitted records do not count as successful throughput.
Value generation and verification are outside individual storage-call latency; open-loop latency starts at the scheduled arrival.
Workload elapsed time excludes session teardown and histogram merging.
Flush time is separate, client resource sampling is process-wide, and optional hardware counters cover only benchmark worker threads.

## Relationship to RetriEval

Both crates use `src/bench.rs` as their library root, feature-gated backend binaries, `CommonArgs`, `Backend`, `ConfigReport`, `MachineInfo`, and `ContainerHandle`.
Backend implementations use `<Engine>Backend` names, with `<Engine>Session` for storage worker state.
A configuration fixes engine settings, data generation, and worker count; a sweep runs several configurations.
A workload specifies an operation mix; a workload report describes one execution of it within a run.
CrudEval retains these storage-specific names instead of borrowing RetriEval's incremental-index `StepEntry` terminology.
Its shared backend is `Sync` and yields worker-local sessions, whereas RetriEval's engines generally manage their own query parallelism.

## Tests

Run commands from the repository root:

```sh
cargo test
cargo test --no-default-features --lib
cargo test --features lmdb-backend,redb-backend,fjall-backend
cargo test --no-default-features --features rocksdb-backend
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

The default feature runs SQLite tests alongside the shared library tests.
RocksDB requires its native build dependencies; see the root README.
Enabling server features compiles their adapters but does not start live server tests.

### Contracts

`backend.rs` defines `assert_backend_contract!`, invoked only by inline adapter tests.
The macro shares assertions across binary test targets without compiling a test helper into production binaries.
It checks missing-row alignment, present empty values, UUID ordering, inclusive range reads, zero-length ranges, update and delete counts, and flush persistence accounting.
Adapters advertising transactions also exercise read-your-writes, rollback, and commit.

Focused tests beside the shared implementation cover:

- Workload names, checked sizes, deterministic distributions, and large-key Zipf precision.
- Concurrent insert reservations, commit publication order, and payload corruption detection.
- Document payload preservation and graph adjacency, including stable edge slots after deletion.
- Small entry budgets, disjoint full scans, read-modify-write, commit accounting, and skipped workloads.
- Duration-limited arrival rates, persisted key ranges, backend configuration isolation, and interrupted mutation refusal.

For a live server check, run its binary against the pinned image with a small dataset and inspect the JSON report.
Confirm zero failed and corrupted entries, expected capability skips, and container cleanup after exit.
Container smoke checks require Docker and are separate from the unit test suite.
