
import os
import shutil
import subprocess
import time

drop_caches = False

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
    'Remove',
]

destory_dbs = len(threads) > 1 or len(sizes) > 1


def get_db_config_path(db_name, size):
    path = f'./bench/configs/{db_name}/{size}.cfg'
    if not os.path.exists(path):
        path = f'./bench/configs/{db_name}/default.cfg'
    return path


def get_worklods_path(size):
    return f'./bench/workloads/{size}.json'

def drop_system_caches():
    subprocess.run(['sudo', 'sh', '-c', '/usr/bin/echo', '3', '>', '/proc/sys/vm/drop_caches'])


def run(db_name, size, threads_count, workload_name):
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

    # Print output
    while True:
        line = process.stdout.readline().decode("utf-8").strip()
        print(line)
        if not line:
            break

for threads_count in threads:
    for size in sizes:
        if destory_dbs:
            shutil.rmtree('./tmp/')
        for db_name in db_names:
            for workload_name in workload_names:
                if drop_caches:
                    drop_system_caches()
                    time.sleep(8)
                run(db_name, size, threads_count, workload_name)

