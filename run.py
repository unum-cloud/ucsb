#!/usr/bin/env python3

import os
import sys
import time
import fire
import shutil
import signal
import pexpect
import pathlib
import termcolor

from typing import Optional

"""
Run the script by passing arguments to the script or changing the settings below.
All the settings are also treated as defaults, so passed arguments will overwrite them.
See main() function.
"""

supported_db_names = [
    'rocksdb',
    'leveldb',
    'wiredtiger',
    'lmdb',
    'mongodb',
    'redis',
    'ukv'
]

supported_sizes = [
    '100MB',
    # '1GB',
    # '10GB',
    # '100GB',
    # '1TB',
    # '10TB',
]

supported_workload_names = [
    'Init',
    'Read',
    'BatchRead',
    'RangeSelect',
    'Scan',
    'ReadUpdate_50_50',
    'ReadUpsert_95_5',
    'BatchUpsert',
    'Remove',
]
 
default_threads_count = 1
default_main_dir_path = './db_main/'
default_storage_disk_paths = [
    # '/mnt/disk1/db_storage/',
    # '/mnt/disk2/db_storage/',
    # '/mnt/disk3/db_storage/',
    # '/mnt/disk4/db_storage/',
]


def get_db_config_file_path(db_name: str, size: str) -> str:
    path = os.path.join('./bench/configs', db_name, f'{size}.cfg')
    if not pathlib.Path(path).exists():
        path = os.path.join('./bench/configs', db_name, 'default.cfg')
    return path


def get_workloads_file_path(size: str) -> str:
    return os.path.join('./bench/workloads', f'{size}.json')


def get_db_main_dir_path(db_name: str, size: str, main_dir_path: str) -> str:
    return os.path.join(main_dir_path, db_name, size, '')


def get_db_storage_dir_paths(db_name: str, size: str, storage_disk_paths: str) -> list:
    db_storage_dir_paths = []
    for storage_disk_path in storage_disk_paths:
        db_storage_dir_paths.append(os.path.join(storage_disk_path, db_name, size, ''))
    return db_storage_dir_paths


def get_results_file_path(db_name: str, size: str, drop_caches: bool, transactional: bool, storage_disk_paths: str, threads_count: int) -> str:
    root_dir_path = ''
    if drop_caches:
        if transactional:
            root_dir_path = './bench/results/without_caches/transactional/'
        else:
            root_dir_path = './bench/results/without_caches/'
    else:
        if transactional:
            root_dir_path = './bench/results/transactional/'
        else:
            root_dir_path = './bench/results/'

    disks_count = max(len(storage_disk_paths), 1)
    return os.path.join(f'{root_dir_path}cores_{threads_count}', f'disks_{disks_count}', db_name, f'{size}.json')


def drop_system_caches():
    print('Dropping caches...')
    try:
        with open('/proc/sys/vm/drop_caches', 'w') as stream:
            stream.write('3\n')
        time.sleep(8)
    except KeyboardInterrupt:
        print(termcolor.colored('Terminated by user', 'yellow'))
        exit(1)
    except:
        print(termcolor.colored('Failed to drop system caches', 'red'))
        exit(1)


def run(db_name: str, size: str, workload_names: list, main_dir_path: str, storage_disk_paths: str, transactional: bool, drop_caches: bool, run_docker_image: bool, threads_count: bool) -> None:
    db_config_file_path = get_db_config_file_path(db_name, size)
    workloads_file_path = get_workloads_file_path(size)
    db_main_dir_path = get_db_main_dir_path(db_name, size, main_dir_path)
    db_storage_dir_paths = get_db_storage_dir_paths(db_name, size, storage_disk_paths)
    results_file_path = get_results_file_path(db_name, size, drop_caches, transactional, storage_disk_paths, threads_count)

    transactional_flag = '-t' if transactional else ''
    filter = ','.join(workload_names)
    db_storage_dir_paths = ','.join(db_storage_dir_paths)

    runner: str
    if run_docker_image:
        runner = f'docker run -v {os.getcwd()}/bench:/ucsb/bench -v {os.getcwd()}/tmp:/ucsb/tmp -it ucsb-image-dev'
    else:
        runner = './build_release/bin/ucsb_bench'
    process = pexpect.spawn(f'{runner} \
                            -db {db_name} \
                            {transactional_flag} \
                            -cfg "{db_config_file_path}" \
                            -wl "{workloads_file_path}" \
                            -md "{db_main_dir_path}" \
                            -sd "{db_storage_dir_paths}"\
                            -res "{results_file_path}" \
                            -th {threads_count} \
                            -fl {filter}'
                            )
    process.interact()
    process.close()

    # Handle signal
    if process.signalstatus:
        sig = signal.Signals(process.signalstatus)
        if sig == signal.SIGINT:
            print(termcolor.colored('Benchmark terminated by user', 'yellow'))
        else:
            print(termcolor.colored(f'Benchmark terminated (signal: {sig.name})', 'red'))
        exit(process.signalstatus)


def check_args(db_names, sizes, workload_names):
    if not db_names:
        sys.exit('Database(s) not specified')
    if not sizes:
        sys.exit('Database size(s) not specified')
    if not workload_names:
        sys.exit('Workload name(s) not specified')


def main(db_names: Optional[list[str]] = supported_db_names,
         sizes: Optional[list[str]] = supported_sizes,
         workload_names: Optional[list[str]] = supported_workload_names,
         main_dir_path: Optional[str] = default_main_dir_path,
         storage_disk_paths: Optional[list[str]] = default_storage_disk_paths,
         threads_count: Optional[int] = default_threads_count,
         transactional: Optional[bool] = False,
         cleanup_previous: Optional[bool] = True,
         drop_caches: Optional[bool] = False,
         run_docker_image: Optional[bool] = False) -> None:

    if os.geteuid() != 0:
        print(termcolor.colored(f'Run as sudo!', 'red'))
        sys.exit(-1)
    check_args(db_names, sizes, workload_names)

    # Cleanup old DBs (Note: It actually cleanups if the first workload is `Init`)
    if cleanup_previous and workload_names[0] == 'Init':
        print('Cleanup...')
        for size in sizes:
            for db_name in db_names:
                # Remove DB main directory
                db_main_dir_path = get_db_main_dir_path(db_name, size, main_dir_path)
                if pathlib.Path(db_main_dir_path).exists():
                    shutil.rmtree(db_main_dir_path)
                # Remove DB storage directories
                db_storage_dir_paths = get_db_storage_dir_paths(db_name, size, storage_disk_paths)
                for db_storage_dir_path in db_storage_dir_paths:
                    if pathlib.Path(db_storage_dir_path).exists():
                        shutil.rmtree(db_storage_dir_path)

    # Run benchmarks
    for size in sizes:
        for db_name in db_names:
            # Create DB main directory
            db_main_dir_path = get_db_main_dir_path(db_name, size, main_dir_path)
            pathlib.Path(db_main_dir_path).mkdir(parents=True, exist_ok=True)
            # Create DB storage directories
            db_storage_dir_paths = get_db_storage_dir_paths(db_name, size, storage_disk_paths)
            for db_storage_dir_path in db_storage_dir_paths:
                pathlib.Path(db_storage_dir_path).mkdir(
                    parents=True, exist_ok=True)
            # Create results dir paths
            results_file_path = get_results_file_path(db_name, size, drop_caches, transactional, storage_disk_paths, threads_count)
            pathlib.Path(results_file_path).parent.mkdir(parents=True, exist_ok=True)

            # Run benchmark
            if drop_caches:
                for workload_name in workload_names:
                    if not run_docker_image:
                        drop_system_caches()
                    run(db_name, size, [workload_name], main_dir_path, storage_disk_paths, transactional, drop_caches, run_docker_image, threads_count)
            else:
                run(db_name, size, workload_names, main_dir_path, storage_disk_paths, transactional, drop_caches, run_docker_image, threads_count)


if __name__ == '__main__':
    fire.Fire(main)
