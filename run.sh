#!/bin/bash

#
# 100MB benchmarks
#
# ./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/100MB.json
# ./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/100MB.json
# ./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/100MB.json
./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/100MB.json

#
# 1GB benchmarks
#
# ./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/1GB.json
# ./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/1GB.json
# ./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/1GB.json
# ./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/1GB.json

#
# 10GB benchmarks
#
# ./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/10GB.json
# ./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/10GB.json
# ./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/10GB.json
# ./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/10GB.json

#
# 100GB benchmarks
#
# ./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/100GB.json
# ./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/100GB.json
# ./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/100GB.json
# ./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/10100GB0MB.json

#
# 1TB benchmarks
#
# ./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/1TB.json
# ./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/1TB.json
# ./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/1TB.json
# ./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/1TB.json
