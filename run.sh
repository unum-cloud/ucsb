#!/bin/bash

# Note: Need to delete prev db before start next benchmark with different workload
# rm -rf ./tmp/

#
# 100MB benchmarks
#
rm -rf ./tmp/
./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/100MB.json
./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/100MB.json
./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/100MB.json
./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/100MB.json

#
# 1GB benchmarks
#
# rm -rf ./tmp/
# ./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/1GB.json
# ./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/1GB.json
# ./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/1GB.json
# ./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/1GB.json

#
# 10GB benchmarks
#
# rm -rf ./tmp/
# ./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/10GB.json
# ./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/10GB.json
# ./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/10GB.json
# ./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/10GB.json

#
# 100GB benchmarks
#
# rm -rf ./tmp/
# ./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/100GB.json
# ./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/100GB.json
# ./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/100GB.json
# ./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/100GB.json

#
# 1TB benchmarks
#
# rm -rf ./tmp/
# ./build_release/bin/_ucsb_bench -db unumdb -c ./bench/configs/unumdb/config.json -w ./bench/workloads/1TB.json
# ./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/1TB.json
# ./build_release/bin/_ucsb_bench -db leveldb -c ./bench/configs/leveldb/config.json -w ./bench/workloads/1TB.json
# ./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/1TB.json
