#!/bin/bash

./build_release/bin/_ucsb_bench -db wiredtiger -c ./bench/configs/wiredtiger/config.json -w ./bench/workloads/100MB.json
