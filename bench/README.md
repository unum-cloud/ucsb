# UCSB
Participating database engines:

* wiredtiger
* leveldb
* rocksdb
* unumdb

Unique workloads:

* Initialization speed
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

* 10GB keys: 37 minutes, 52 secs, 210 millisecs
* 100GB keys: 9 hours, 45 minutes, 23 secs, 345 millisecs
* 1TB keys: 7 days, 12 hours, 10 minutes, 4 secs, 833 millisecs

Benchmark duration by DBMS:

* wiredtiger keys: 2 days, 21 hours, 41 minutes, 37 secs, 815 millisecs
* leveldb keys: 1 day, 15 hours, 7 minutes, 28 secs, 853 millisecs
* rocksdb keys: 3 days, 1 hour, 16 minutes, 15 secs, 36 millisecs
* unumdb keys: 8 hours, 27 minutes, 58 secs, 685 millisecs

Total: 7 days, 22 hours, 33 minutes, 20 secs, 390 millisecs
## Initialization
![report_init](report/init.svg)

|          Brand          | CPU usage  | RAM Usage  | Disk Usage |     Speed     |
| :---------------------: | :--------: | :--------: | :--------: | :-----------: |
| WiredTiger *by MongoDB* | 2.65 cores | 891.40 MiB |  1.06 TiB  |   9K ops/s    |
|   LevelDB *by Google*   | 0.99 cores | 277.62 MiB | 984.58 GiB |   28K ops/s   |
|  RocksDB *by Facebook*  | 1.04 cores |  1.40 GiB  | 977.76 GiB |   21K ops/s   |
|    UnumDB *by Unum*     | 2.98 cores |  8.75 GiB  | 972.20 GiB | 97K ops/s 🔞🔞🔞 |

## Read Only
![report_read](report/read.svg)

|          Brand          | CPU usage  | RAM Usage  | Disk Usage |     Speed      |
| :---------------------: | :--------: | :--------: | :--------: | :------------: |
| WiredTiger *by MongoDB* | 3.32 cores |  1.85 GiB  |  1.06 TiB  |   26K ops/s    |
|   LevelDB *by Google*   | 1.07 cores | 117.76 MiB | 984.13 GiB |   28K ops/s    |
|  RocksDB *by Facebook*  | 0.40 cores |  3.87 GiB  | 977.65 GiB |   10K ops/s    |
|    UnumDB *by Unum*     | 3.00 cores | 21.05 GiB  | 972.30 GiB | 205K ops/s 🍓🍓🍓 |

## Read/Update: 50/50
![report_readupdate_5050](report/readupdate_5050.svg)

|          Brand          | CPU usage  | RAM Usage | Disk Usage |   Speed    |
| :---------------------: | :--------: | :-------: | :--------: | :--------: |
| WiredTiger *by MongoDB* | 2.97 cores | 1.17 GiB  |  1.10 TiB  | 13K ops/s  |
|   LevelDB *by Google*   | 1.75 cores | 1.55 GiB  | 988.81 GiB | 102K ops/s |
|  RocksDB *by Facebook*  | 0.64 cores | 5.14 GiB  |  1.07 TiB  | 14K ops/s  |
|    UnumDB *by Unum*     | 3.00 cores | 89.79 GiB | 980.75 GiB | 170K ops/s |

## Read/Insert: 95/5
![report_readinsert_9505](report/readinsert_9505.svg)

|          Brand          | CPU usage  | RAM Usage  | Disk Usage  |   Speed    |
| :---------------------: | :--------: | :--------: | :---------: | :--------: |
| WiredTiger *by MongoDB* | nanT cores |  nan TiB   |   nan TiB   | nanT ops/s |
|   LevelDB *by Google*   | 1.37 cores | 119.24 MiB |  1.01 TiB   | 21K ops/s  |
|  RocksDB *by Facebook*  | 0.61 cores |  4.88 GiB  |  1.10 TiB   | 40K ops/s  |
|    UnumDB *by Unum*     | 3.00 cores | 106.96 GiB | 1022.63 GiB | 153K ops/s |

## Range Select
![report_rangeselect](report/rangeselect.svg)

|          Brand          | CPU usage  | RAM Usage | Disk Usage |   Speed    |
| :---------------------: | :--------: | :-------: | :--------: | :--------: |
| WiredTiger *by MongoDB* | 0.75 cores | 1.46 GiB  |  1.06 TiB  | 226K ops/s |
|   LevelDB *by Google*   | 0.57 cores | 88.82 MiB | 983.99 GiB | 533K ops/s |
|  RocksDB *by Facebook*  | 0.32 cores | 4.27 GiB  | 977.65 GiB | 146K ops/s |
|    UnumDB *by Unum*     | 2.53 cores | 47.02 GiB | 972.30 GiB | 626K ops/s |

## Full Scan
![report_scan](report/scan.svg)

|          Brand          | CPU usage  | RAM Usage | Disk Usage |    Speed     |
| :---------------------: | :--------: | :-------: | :--------: | :----------: |
| WiredTiger *by MongoDB* | 0.23 cores | 1.32 GiB  |  1.06 TiB  |  183K ops/s  |
|   LevelDB *by Google*   | 0.59 cores | 3.81 GiB  | 983.99 GiB |  879K ops/s  |
|  RocksDB *by Facebook*  | 0.16 cores | 3.88 GiB  | 977.65 GiB |  84K ops/s   |
|    UnumDB *by Unum*     | 2.47 cores | 67.42 GiB | 972.30 GiB | 2M ops/s 🔥🔥🔥 |

## Batch Read
![report_batchread](report/batchread.svg)

|          Brand          | CPU usage  | RAM Usage | Disk Usage |     Speed      |
| :---------------------: | :--------: | :-------: | :--------: | :------------: |
| WiredTiger *by MongoDB* | 2.99 cores | 1.47 GiB  |  1.06 TiB  |   27K ops/s    |
|   LevelDB *by Google*   | 1.00 cores | 90.03 MiB | 983.99 GiB |   30K ops/s    |
|  RocksDB *by Facebook*  | 0.37 cores | 4.17 GiB  | 977.65 GiB |   11K ops/s    |
|    UnumDB *by Unum*     | 3.00 cores | 38.32 GiB | 972.30 GiB | 541K ops/s 🔥🔥🔥 |
