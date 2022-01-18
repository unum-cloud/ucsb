
import os
import sys
import time
import shutil
import pexpect
import subprocess

drop_caches = False
cleanup_previous_dbs = False

threads = [
    1,
    4,
    8,
    16,
]

db_names = [
    'unumdb',
    'rocksdb',
    'leveldb',
    'wiredtiger',
    'lmdb',
]

sizes = [
    '100MB',
    '1GB',
    '10GB',
    '100GB',
    '250GB',
    '1TB',
]

workload_names = [
    'Init',
    'Read',
    'BatchRead',
    'RangeSelect',
    'Scan',
    'ReadUpdate_50_50',
    'ReadInsert_95_5',
    'BatchInsert',
    'BulkImport',
    'Remove',
]


def get_db_config_path(db_name, size):
    path = f'./bench/configs/{db_name}/{size}.cfg'
    if not os.path.exists(path):
        path = f'./bench/configs/{db_name}/default.cfg'
    return path


def get_worklods_path(size):
    return f'./bench/workloads/{size}.json'


def drop_system_caches():
    with open('/proc/sys/vm/drop_caches', 'w') as stream:
        stream.write('3\n')


def run(db_name, size, threads_count, workload_names):
    config_path = get_db_config_path(db_name, size)
    workloads_path = get_worklods_path(size)

    filter = ','.join(workload_names)
    child = pexpect.spawn(f'./build_release/bin/_ucsb_bench \
                            -db {db_name} \
                            -c {config_path} \
                            -w {workloads_path} \
                            -threads {threads_count} \
                            -filter {filter}'
                          )
    child.interact()


if os.geteuid() != 0:
    sys.exit("Run as sudo!")

if cleanup_previous_dbs:
    if os.path.exists('./tmp/'):
        shutil.rmtree('./tmp/')

for threads_count in threads:
    for size in sizes:
        for db_name in db_names:
            if drop_caches:
                for workload_name in workload_names:
                    drop_system_caches()
                    time.sleep(8)
                    run(db_name, size, threads_count, [workload_name])
            else:
                run(db_name, size, threads_count, workload_names)
