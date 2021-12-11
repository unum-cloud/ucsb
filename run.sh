#!/bin/bash

./build_release/bin/_ucsb_bench -db rocksdb -c ./bench/configs/rocksdb/options.ini -w ./bench/workloads/100MB.json
