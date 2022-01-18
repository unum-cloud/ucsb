# UCSB
Participating database engines:

* wiredtiger
* leveldb
* rocksdb
* unumdb

Unique workloads:

* Initialization
* Reads/Update: 50/50
* Read/Insert: 95/5
* Read
* Range Select
* Full Scan
* Batch Read

Key counts in different experiments:

* 10GB
* 100GB
* 1TB

Benchmark duration by size:

* 10GB keys: 44 minutes, 15 secs, 847 millisecs
* 100GB keys: 9 hours, 49 minutes, 3 secs, 306 millisecs
* 1TB keys: 8 days, 4 hours, 32 secs, 362 millisecs

Benchmark duration by DBMS:

* wiredtiger keys: 3 days, 13 hours, 26 minutes, 41 secs, 682 millisecs
* leveldb keys: 1 day, 15 hours, 9 minutes, 8 secs, 241 millisecs
* rocksdb keys: 3 days, 1 hour, 17 minutes, 504 millisecs
* unumdb keys: 8 hours, 41 minutes, 1 sec, 89 millisecs

Total: 8 days, 14 hours, 33 minutes, 51 secs, 517 millisecs
## Initialization
![init](report/cores_1//init.svg)

|Brand|CPU usage|RAM Usage|Disk Usage|Speed|
|:-:|:-:|:-:|:-:|:-:|
|WiredTiger *by MongoDB*|2.65 cores|891.40 MiB|1.06 TiB|9K ops/s|
|LevelDB *by Google*|0.99 cores|277.62 MiB|984.58 GiB|28K ops/s|
|RocksDB *by Facebook*|1.04 cores|1.40 GiB|977.76 GiB|21K ops/s|
|UnumDB *by Unum*|2.96 cores|26.11 GiB|972.20 GiB|83K ops/s 🔞🔞🔞|

## Read Only
![read](report/cores_1/read.svg)

|Brand|CPU usage|RAM Usage|Disk Usage|Speed|
|:-:|:-:|:-:|:-:|:-:|
|WiredTiger *by MongoDB*|3.32 cores|1.85 GiB|1.06 TiB|26K ops/s|
|LevelDB *by Google*|1.07 cores|117.76 MiB|984.13 GiB|28K ops/s|
|RocksDB *by Facebook*|0.40 cores|3.87 GiB|977.65 GiB|10K ops/s|
|UnumDB *by Unum*|3.00 cores|39.96 GiB|972.30 GiB|196K ops/s 🔞🔞🔞|

## Read/Update: 50/50
![readupdate_5050](report/cores_1/readupdate_5050.svg)

|Brand|CPU usage|RAM Usage|Disk Usage|Speed|
|:-:|:-:|:-:|:-:|:-:|
|WiredTiger *by MongoDB*|2.97 cores|1.17 GiB|1.10 TiB|13K ops/s|
|LevelDB *by Google*|1.75 cores|1.55 GiB|988.81 GiB|102K ops/s|
|RocksDB *by Facebook*|0.64 cores|5.14 GiB|1.07 TiB|14K ops/s|
|UnumDB *by Unum*|3.00 cores|69.74 GiB|975.45 GiB|222K ops/s 🔥🔥🔥|

## Read/Insert: 95/5
![readinsert_9505](report/cores_1/readinsert_9505.svg)

|Brand|CPU usage|RAM Usage|Disk Usage|Speed|
|:-:|:-:|:-:|:-:|:-:|
|WiredTiger *by MongoDB*|3.08 cores|1.03 GiB|1.12 TiB|16K ops/s|
|LevelDB *by Google*|1.37 cores|119.24 MiB|1.01 TiB|21K ops/s|
|RocksDB *by Facebook*|0.61 cores|4.88 GiB|1.10 TiB|40K ops/s|
|UnumDB *by Unum*|2.92 cores|20.98 GiB|1020.92 GiB|156K ops/s 🔞🔞🔞|

## Range Select
![rangeselect](report/cores_1/rangeselect.svg)

|Brand|CPU usage|RAM Usage|Disk Usage|Speed|
|:-:|:-:|:-:|:-:|:-:|
|WiredTiger *by MongoDB*|0.75 cores|1.46 GiB|1.06 TiB|226K ops/s|
|LevelDB *by Google*|0.57 cores|88.82 MiB|983.99 GiB|533K ops/s|
|RocksDB *by Facebook*|0.32 cores|4.27 GiB|977.65 GiB|146K ops/s|
|UnumDB *by Unum*|2.53 cores|47.02 GiB|972.30 GiB|626K ops/s|

## Full Scan
![scan](report/cores_1/scan.svg)

|Brand|CPU usage|RAM Usage|Disk Usage|Speed|
|:-:|:-:|:-:|:-:|:-:|
|WiredTiger *by MongoDB*|0.23 cores|1.32 GiB|1.06 TiB|183K ops/s|
|LevelDB *by Google*|0.59 cores|3.81 GiB|983.99 GiB|879K ops/s|
|RocksDB *by Facebook*|0.16 cores|3.88 GiB|977.65 GiB|84K ops/s|
|UnumDB *by Unum*|2.51 cores|110.48 GiB|972.30 GiB|1M ops/s|

## Batch Read
![batchread](report/cores_1/batchread.svg)

|Brand|CPU usage|RAM Usage|Disk Usage|Speed|
|:-:|:-:|:-:|:-:|:-:|
|WiredTiger *by MongoDB*|2.99 cores|1.47 GiB|1.06 TiB|27K ops/s|
|LevelDB *by Google*|1.00 cores|90.03 MiB|983.99 GiB|30K ops/s|
|RocksDB *by Facebook*|0.37 cores|4.17 GiB|977.65 GiB|11K ops/s|
|UnumDB *by Unum*|3.00 cores|58.59 GiB|972.30 GiB|553K ops/s 🔥🔥🔥|
