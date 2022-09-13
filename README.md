# UCSB

Unum Cloud Serving Benchmark is the grandchild of Yahoo Cloud Serving Benchmark, reimplemented in C++, with less costly abstractions and with more workloads, crafted specifically for the Big Data age!
A full description of this benchmark can be found [here, in our corporate blog](https://unum.cloud/ucsb), together with the recent results.

![UCSB Benchmark Duration for RocksDB, LevelDB, WiredTiger and UnumDB](https://unum.cloud/assets/post/2022-09-13-ucsb-10tb/ucsb-10tb-duration.png)

To run the benchmark, call `./run.py`.
Output will be placed into the `bench/results/` folder.

## Supported Backends

* WiredTiger. Version 10.0.0.
* LevelDB. Version 1.23.
* RocksDB. Version 6.29.3.
* LMDB. Version 0.9.29.

Our UnumDB is also evaluated with this benchmark, but it wasn't included into the public mirror.

### Implemented Workloads

* [∅](https://unum.cloud/ucsb#0): imports monotonically increasing keys 🔄
* [A](https://unum.cloud/ucsb#A): 50% reads + 50% updates, all random
* [C](https://unum.cloud/ucsb#C): reads, all random
* [D](https://unum.cloud/ucsb#D): 95% reads + 5% inserts, all random
* [E](https://unum.cloud/ucsb#E): range scan 🔄
* [✗](https://unum.cloud/ucsb#X): batch read 🆕
* [Y](https://unum.cloud/ucsb#Y): batch insert 🆕
* [Z](https://unum.cloud/ucsb#Z): scans 🆕

The **∅** was previously implemented as one-by-one inserts, but some KVS support the external construction of its internal representation files.
The **E** was [previously](https://github.com/brianfrankcooper/YCSB/blob/master/workloads/workloade) mixed with 5% insertions.

## Known Issues and TODOs

* [ ] Current benchmarks don't use custom key comparators. Both variants were tested and it didn't affect the speed.
* [ ] WiredTiger sometimes crashes on 1 TB benchmarks.
* [ ] Read/Update might be replaced with a Read-Modify-Write operation.
