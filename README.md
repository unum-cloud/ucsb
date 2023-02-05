<h1 align="center">Unbranded Cloud Serving Benchmark</h1>
<h3 align="center">
Yahoo Cloud Serving Benchmark for NoSQL Databases<br/>
Refactored and Extended with Batch and Range Queries<br/>
</h3>
<br/>

<p align="center">
  <a href="https://discord.gg/AxsU9mctAn"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/discord.svg" alt="Discord"></a>
	&nbsp;&nbsp;&nbsp;
  <a href="https://www.linkedin.com/company/unum-cloud/"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/linkedin.svg" alt="LinkedIn"></a>
  &nbsp;&nbsp;&nbsp;
  <a href="https://twitter.com/unum_cloud"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/twitter.svg" alt="Twitter"></a>
  &nbsp;&nbsp;&nbsp;
	<a href="https://unum.cloud/post"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/blog.svg" alt="Blog"></a>
	&nbsp;&nbsp;&nbsp;
	<a href="https://github.com/unum-cloud/ucset"><img height="25" src="https://github.com/unum-cloud/ukv/raw/main/assets/icons/github.svg" alt="GitHub"></a>
</p>

---

Unum Cloud Serving Benchmark is the grandchild of Yahoo Cloud Serving Benchmark, reimplemented in C++, with fewer mutexes or other bottlenecks, and with additional "batch" and "range" workloads, crafted specifically for the Big Data age!

|                         | Present in YCSB | Present in UCSB |
| :---------------------- | :-------------: | :-------------: |
| Size of the dataset     |        ✅        |        ✅        |
| DB configuration files  |        ✅        |        ✅        |
| Workload specifications |        ✅        |        ✅        |
| Tracking hardware usage |        ❌        |        ✅        |
| Workload Isolation      |        ❌        |        ✅        |
| Concurrency             |        ❌        |        ✅        |
| Batch Operations        |        ❌        |        ✅        |
| Bulk Operations         |        ❌        |        ✅        |
| Support of Transactions |        ❌        |        ✅        |

As you may know, benchmarking databases is very complex.
There is too much control flow to tune, so instead of learning the names of a thousand CLI arguments, you'd use a [`run.py`](https://github.com/unum-cloud/UCSB/blob/main/run.py) script to launch the benchmarks:

```sh
git clone https://github.com/unum-cloud/ucsb.git
./run.py
```

The outputs will be placed into the `bench/results/` folder.

## Supported Databases

Key-Value Stores and NoSQL databases differ in supported operations.
It also applies to the operations queried by UCSB.


|                        | Bulk Import | Bulk Scan | Batch Read | Batch Write |
| :--------------------- | :---------: | :-------: | :--------: | :---------: |
|                        |             |           |            |             |
| 💾 Embedded Databases   |             |           |            |             |
| WiredTiger             |      ✅      |     ✅     |     ❌      |      ❌      |
| LevelDB                |      ❌      |     ✅     |     ❌      |      ✅      |
| RocksDB                |      ✅      |     ✅     |     ✅      |      ✅      |
| LMDB                   |             |           |            |             |
| UDisk                  |      ✅      |     ✅     |     ✅      |      ✅      |
|                        |             |           |            |             |
| 🖥️ Standalone Databases |             |           |            |             |
| Redis                  |             |     ❌     |     ✅      |      ✅      |
| MongoDB                |             |     ✅     |     ✅      |      ✅      |

When batches aren't natively supported, we simulate them with multiple single-entry operations.
There is also asymmetry elsewhere:

* WiredTiger supports fixed-size integer keys.
* LevelDB only supports variable length keys and values.
* RocksDB has minimal support for [`fixed_key_len`](https://cs.github.com/facebook/rocksdb?q=fixed_key_len), incompatiable with `BlockBasedTable`.
* UDisk supports both fixed-size keys and values.

Just like YCSB, we use 8-byte integer keys and 1000-byte values.
Both WiredTiger and UDisk were configured to use integer keys natively.
RocksDB wrapper reverts the order of bytes in keys to use the native comparator.
None of the DBs was set to use fixed-size values, as only UDisk supports that.

---

Recent results:

* 1 TB collections. Mar 22, 2022. [post](https://unum.cloud/post/2022-03-22-ucsb)
* 10 TB collections. Sep 13, 2022. [post](https://unum.cloud/post/2022-09-13-ucsb-10tb/)

---

- [Supported Databases](#supported-databases)
- [Yet Another Benchmark?](#yet-another-benchmark)
- [Ways to Spoil a DBMS Benchmark](#ways-to-spoil-a-dbms-benchmark)
  - [Durability vs Speed](#durability-vs-speed)
  - [Strict vs Flexible RAM Limits](#strict-vs-flexible-ram-limits)
  - [Dataset Size \& NAND Modes](#dataset-size--nand-modes)
- [Preset Workloads](#preset-workloads)

---

## Yet Another Benchmark?

Yes.
In the DBMS world there are just 2 major benchmarks:

* YCSB for NoSQL.
* TPC for SQL.

There are, of course, more niche variants:

* New York Taxi Rides benchmarks.
* ANN and BigANN benchmarks for kANN indexes.

With YCSB everything seems simple - clone the repo, pick a DBMS, run the benchmark.
TPC suite seems more enterprisey, and after a few years in the industry, I still don't understand the procedure.
Moreover, most SQL databases these days are built on top of other NoSQL solutions.
So naturally we used YCSB internally.

We were getting great numbers.
All was fine until it wasn't.
We looked under the hood and realized that the benchmark code itself was less efficient than the databases it was trying to evaluate, causing additional bottlenecks and affecting the measurements.

> It was like racing Ferraris with 30 mi/h speed limits.

So just like others, we decided to port it to C++, refactor it, and share with the world.
Everyone is welcome to contribute!

## Ways to Spoil a DBMS Benchmark

If you use Google Benchmark, you know about its [bunch of nifty tricks](/post/2022-03-04-gbench), like `DoNotOptimize` or the automatic resolution of the number of iterations at runtime.
It's widespread in micro-benchmarking, but it begs for extensions when you start profiling a DBMS.
The ones shipped with UCSB spawn a sibling process that samples usage statistics from the OS.
Like `valgrind`, we read from [`/proc/*` files](https://man7.org/linux/man-pages/man5/proc.5.html) and aggregate stats like SSD I/O and overall RAM usage.

> Unlike humans, [ACID](https://en.wikipedia.org/wiki/ACID) is one of the best things that can happen to DBMS 😁

### Durability vs Speed

Like all good things, ACID is unreachable, because of at least one property - Durability.
Absolute Durability is practically impossible and high Durability is expensive.

All high-performance DBs are designed as [Log Structured Merge Trees](https://en.wikipedia.org/wiki/Log-structured_merge-tree).
It's a design that essentially bans in-place file overwrites.
Instead, it builds layers of immutable files arranged in a Tree-like order.
The problem is that until you have enough content to populate an entire top-level file, you keep data in RAM - in structures often called `MemTable`s.

![LSM Tree](https://unum.cloud/assets/post/2022-03-22-ucsb/lsm-tree.png)

If the lights go off, volatile memory will be discarded.
So a copy of every incoming write is generally appended to a Write-Ahead-Log (WAL).
Two problems  here:

1. You can't have a full write confirmation before appending to WAL. It's still a write to disk. A system call. A context switch to kernel space. Want to avoid it with [`io_uring`](https://unixism.net/loti/what_is_io_uring.html) or [`SPDK`](https://spdk.io), then be ready to change all the above logic to work in an async manner, but fast enough not to create a new bottleneck.  Hint: [`std::async`](https://en.cppreference.com/w/cpp/thread/async) will not cut it.
2. WAL is functionally stepping on the toes of a higher-level logic. Every wrapping DBMS, generally implements such mechanisms, so they disable WAL in KVS, to avoid extra stalls and replication. Example: [Yugabyte is a port](https://blog.yugabyte.com/how-we-built-a-high-performance-document-store-on-rocksdb/) of Postgres to RocksDB and disables the embedded WAL.

We generally disable WAL and benchmark the core.
Still, you can tweak all of that in the UCSB configuration files yourself.

Furthermore, as widely discussed, [flushing the data still may not guarantee it's preservation on your SSD](https://twitter.com/xenadu02/status/1495693475584557056?s=20&t=eG2cIbzMg_rTq379EkkHMQ).
So pick you ~~poison~~ hardware wisely and tune your benchmarks cautiously.

### Strict vs Flexible RAM Limits

When users specify a RAM limit for a KVS, they expect all of the required in-memory state to fit into that many bytes.
It would be too obvious for modern software, so here is one more problem.

Fast I/O is hard.
The faster you want it, the more abstractions you will need to replace.

```mermaid
graph LR
    Application -->|libc| LIBC[Userspace Buffers]
    Application -->|mmap| PC[Page Cache]
    Application -->|mmap+O_DIRECT| BL[Block I/O Layer]
    Application -->|SPDK| DL[Device Layer]

    LIBC --> PC
    PC --> BL
    BL --> DL
```

Generally, OS keeps copies of the requested pages in RAM cache.
To avoid it, enable [`O_DIRECT`](https://man7.org/linux/man-pages/man2/open.2.html).
It will slow down the app and would require some more engineering.
For one, all the disk I/O will have to be aligned to page sizes, [generally 4KB](https://docs.pmem.io/persistent-memory/getting-started-guide/creating-development-environments/linux-environments/advanced-topics/i-o-alignment-considerations), which includes both the address in the file and the address in the userspace buffers.
Split-loads should also be managed with an extra code on your side.
So most KVS (except for UDisk, of course 😂) solutions don't bother implementing very fast I/O, like `SPDK`.
In that case, they can't even know how much RAM the underlying OS has reserved for them.
So we have to configure them carefully and, ideally, add external constraints:

```sh
systemd-run --scope -p MemoryLimit=100M /path/ucsb
```

Now a question.
Let's say you want to [`mmap`](https://man7.org/linux/man-pages/man2/mmap.2.html) files and be done.
Anyways, Linux can do a far better job at managing caches than most DBs.
In that case - the memory usage will always be very high but within the limits of that process.
As soon as we near the limit - the OS will drop the old caches.
Is it better to use the least RAM or the most RAM until the limit?

For our cloud-first offering, we will favour the second option.
It will give the users the most value for their money on single-purpose instances.

Furthermore, we allow and enable "**Workload Isolation**" in UCSB by default.
It will create a separate process and a separate address space for each workload of each DB.
Between this, we flush the whole system.
The caches filled during insertions benchmarks, will be invalidated before the reads begin.
This will make the numbers more reliable but limits concurrent benchmarks to one.

### Dataset Size & NAND Modes

Large capacity SSDs store multiple bits per cell.
If you are buying a Quad Level Cell SSD, you expect each of them to store 4 bits of relevant information.
That may be a false expectation.

![SLC MLC vs TLC](https://unum.cloud/assets/post/2022-03-22-ucsb/slc-mlc-tlc-shape.jpg)

The SSD can switch to SLC mode during intensive writes, where IO is faster, especially if a lot of space is available.
In the case of an 8 TB SSD, before we reach 2 TB used space, all [NAND](https://en.wikipedia.org/wiki/Flash_memory) arrays can, in theory, be populated with just one relevant bit.

![SLC vs eMLC vs MLC vs TLC](https://unum.cloud/assets/post/2022-03-22-ucsb/slc-mlc-tlc-specs.png)

If you are benchmarking the DBMS, not the SSD, ensure that you did all benchmarks within the same mode.
In our case for a 1 TB workload on 8 TB drives, it's either:

* starting with an empty drive,
* starting with an 80% full drive.

## Preset Workloads

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
