#!/usr/bin/env python3

import os
import sys
import time
import shutil
import pexpect
import pathlib

drop_caches = False
transactional = False
cleanup_previous = False
run_docker_image = False

threads = [
    1,
    # 2,
    # 4,
    # 8,
    # 16,
    # 32,
    # 64,
]

db_names = [
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
    # '10TB',
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
    'Remove',
]


def get_db_config_file_path(db_name: str, size: int) -> str:
    path = f'./bench/configs/{db_name}/{size}.cfg'
    if not os.path.exists(path):
        path = f'./bench/configs/{db_name}/default.cfg'
    return path


def get_worklods_file_path(size: int) -> str:
    return f'./bench/workloads/{size}.json'


def get_db_path(db_name: str, size: int) -> str:
    return f'./tmp/{db_name}/{size}/'


def get_results_dir_path() -> str:
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


def run(db_name: str, size: int, threads_count: int, workload_names: list) -> None:
    config_path = get_db_config_file_path(db_name, size)
    workloads_path = get_worklods_file_path(size)
    db_path = get_db_path(db_name, size)
    results_path = get_results_dir_path()

    transactional_flag = '-t' if transactional else ''
    filter = ','.join(workload_names)
    runner: str
    if run_docker_image:
        runner = f'docker run -v {os.getcwd()}/bench:/ucsb/bench -v {os.getcwd()}/tmp:/ucsb/tmp -it ucsb-image-dev'
    else:
        runner = './build_release/bin/ucsb_bench'
    child = pexpect.spawn(f'{runner} \
                            -db {db_name} \
                            {transactional_flag} \
                            -c {config_path} \
                            -w {workloads_path} \
                            -wd {db_path} \
                            -r {results_path} \
                            -threads {threads_count} \
                            -filter {filter}'
                          )
    child.interact()


def main() -> None:
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
                # Cleanup old DB
                if len(threads) > 1 and cleanup_previous:
                    if os.path.exists(db_path):
                        shutil.rmtree(db_path)

                if drop_caches:
                    for workload_name in workload_names:
                        if not run_docker_image:
                            print('Dropping caches...')
                            drop_system_caches()
                            time.sleep(8)
                        run(db_name, size, threads_count, [workload_name])
                else:
                    run(db_name, size, threads_count, workload_names)


if __name__ == '__main__':
    main()
