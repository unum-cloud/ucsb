# UCSB

Unum Cloud Service Benchmark is the grandchild of Yahoo Cloud Service Benchmark, reimplemented in C++, with less costly abstractions and with more workloads, crafted specifically for the Big Data age!

## Benchmark Specification

### Implemented Backends

* RocksDB, version: 6.20.3
* LevelDB, version: 1.23
* LMDB, version: 0.9.29
* WiredTiger, version: 10.0.0

Our UnumDB is also evaluated with this benchmark, but it was removed from the publicly available mirror.

### Workloads

* Initialization
* Reads/Update: 50/50 (Called Workload # in YCSB)
* Read/Insert: 95/5 (Called Workload # in YCSB)
* Read (Called Workload # in YCSB)
* Range Select
* Full Scan
* Batch Read
* *Bulk Import*

### Sizes

By default we run this benchmarks with 8 byte integer keys and 1 KB values.
For a key-value store to be fast, you want keys as small as possible, yet capable of addressing long ranges.
The original YCSB used fixed size strings with a lot of unnecessary serialization and formatting methods, causing additional and benchmarking itself, rather than the DBMS.
The size of values, though was left identical to the original YCSB benchmark.

Here is how long the benchmarks took on our hardware for every size:

* 100 MB
* 1 GB
* 10 GB
* 100 GB
* 1 TB
* *10 TB*

## Known Issues

* [ ] Current benchmarks don't use custom key comparators. Both variants were tested and it didn't affect speed.
* [ ] Linker error when trying to supply custom comparators to LevelDB.
* [ ] RocksDB local builds are significantly slower than prebuilt variants.
* [ ] WiredTiger crashes on 1 TB benchmarks, specifically on the `ReadInsert` workload.

## Benchmark

Run `sudo /usr/bin/python3 run.py`. Output is located in `bench/results/` folder.
