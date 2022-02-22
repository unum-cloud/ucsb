import os
import sys
import time
import shutil
import pexpect
import pathlib
import subprocess

drop_caches = False
transactional = False
cleanup_previous = False

threads = [
    1,
    # 2,
    # 4,
    # 8,
    # 16,
]

db_names = [
    'unumdb',
    'rocksdb',
    'leveldb',
    'wiredtiger',
    'lmdb',
    'mongodb'
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


def get_db_config_file_path(db_name, size):
    path = f'./bench/configs/{db_name}/{size}.cfg'
    if not os.path.exists(path):
        path = f'./bench/configs/{db_name}/default.cfg'
    return path


def get_worklods_file_path(size):
    return f'./bench/workloads/{size}.json'


def get_db_path(db_name, size):
    return f'./tmp/{db_name}/{size}/'


def get_results_dir_path():
    if drop_caches:
        if transactional:
            return './bench/results/without_caches/transactional/'
        else:
            return './bench/results/without_caches/'
    else:
        if transactional:
            return './bench/results/transactional/'
        else:
            return './bench/results/'


def drop_system_caches():
    with open('/proc/sys/vm/drop_caches', 'w') as stream:
        stream.write('3\n')


def launch_db(db_name, config_path):
    if db_name == "mongodb":
        subprocess.Popen(
            ["mongo", "--eval", "db.getSiblingDB('admin').shutdownServer()"], stdout=subprocess.DEVNULL)
        time.sleep(2)
        subprocess.Popen(["sudo", "mongod", "--config",
                         config_path], stdout=subprocess.DEVNULL)


def run(db_name, size, threads_count, workload_names):
    config_path = get_db_config_file_path(db_name, size)
    workloads_path = get_worklods_file_path(size)
    results_path = get_results_dir_path()

    transactional_flag = '-t' if transactional else ''
    filter = ','.join(workload_names)
    child = pexpect.spawn(f'./build_release/bin/_ucsb_bench \
                            -db {db_name} \
                            {transactional_flag}\
                            -c {config_path} \
                            -w {workloads_path} \
                            -r {results_path} \
                            -threads {threads_count} \
                            -filter {filter}'
                          )
    child.interact()


if os.geteuid() != 0:
    sys.exit('Run as sudo!')

if cleanup_previous:
    print('Cleanup...')
    for size in sizes:
        for db_name in db_names:
            db_path = get_db_path(db_name, size)
            if os.path.exists(db_path):
                shutil.rmtree(db_path)

pathlib.Path(get_results_dir_path()).mkdir(parents=True, exist_ok=True)

for threads_count in threads:
    for size in sizes:
        for db_name in db_names:
            db_path = get_db_path(db_name, size)
            pathlib.Path(db_path).mkdir(parents=True, exist_ok=True)
            config_path = get_db_config_file_path(db_name, size)
            launch_db(db_name, config_path)

            if drop_caches:
                for workload_name in workload_names:
                    print('Dropping caches...')
                    drop_system_caches()
                    time.sleep(8)
                    run(db_name, size, threads_count, [workload_name])
            else:
                run(db_name, size, threads_count, workload_names)
