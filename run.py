
import os
import subprocess
from time import sleep

drop_caches = False

threads = [
    1,
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
    # '1GB',
    # '10GB',
    # '100GB',
    # '1TB',
]

workload_names = [
    'Init',
    'Read',
    'BatchRead',
    'RangeSelect',
    'Scan',
    'ReadUpdate_50_50',
    'ReadInsert_95_5',
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
    subprocess.run(['sudo', 'sh', '-c', '/usr/bin/echo', '3', '>', '/proc/sys/vm/drop_caches'])


def run_all_workloads(db_name, size, threads_count):
    config_path = get_db_config_path(db_name, size)
    workloads_path = get_worklods_path(size)
    process = subprocess.Popen([
            './build_release/bin/_ucsb_bench',
            '-db', db_name,
            '-c', config_path,
            '-w', workloads_path,
            '-threads', str(threads_count)],
        stdout=subprocess.PIPE)
    output = process.communicate()[0]
    print(output)


def run_single_workload(db_name, size, threads_count, workload_name):
    config_path = get_db_config_path(db_name, size)
    workloads_path = get_worklods_path(size)
    process = subprocess.Popen([
            './build_release/bin/_ucsb_bench',
            '-db', db_name,
            '-c', config_path,
            '-w', workloads_path,
            '-threads', str(threads_count),
            '-filter', workload_name,
        ],
        stdout=subprocess.PIPE)
    output = process.communicate()[0]
    print(output)


for threads_count in threads:
    for size in sizes:
        for db_name in db_names:
            if drop_caches:
                for workload_name in workload_names:
                    drop_system_caches()
                    sleep(8)
                    run_single_workload(db_name, size, threads_count, workload_name)
            else:
                run_all_workloads(db_name, size, threads_count)

