#!/usr/bin/env python3

import os
import sys
import time
import shutil
import signal
import pexpect
import pathlib
import termcolor

from typing import Optional
import fire

supported_db_names = [
    'rocksdb',
    'leveldb',
    'wiredtiger',
    'lmdb',
]

supported_sizes = [
    '100MB',
    '1GB',
    '10GB',
    '100GB',
    '1TB',
    '10TB',
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


def get_db_config_file_path(db_name: str, size: str) -> str:
    path = f'./bench/configs/{db_name}/{size}.cfg'
    if not pathlib.Path(path).exists():
        path = f'./bench/configs/{db_name}/default.cfg'
    return path


def get_workloads_file_path(size: str) -> str:
    return f'./bench/workloads/{size}.json'


def get_db_main_dir_path(db_name: str, size: str, main_dir_root_path: str) -> str:
    return f'{main_dir_root_path}{db_name}/{size}/'


def get_db_storage_dir_paths(db_name: str, size: str, storage_disk_root_paths: str) -> list:
    db_storage_dir_paths = []
    for storage_disk_path in storage_disk_root_paths:
        if (storage_disk_path.endswith('/')):
            db_storage_dir_paths.append(
                f'{storage_disk_path}{db_name}/{size}/')
        else:
            db_storage_dir_paths.append(
                f'{storage_disk_path}/{db_name}/{size}/')
    return db_storage_dir_paths


def get_results_file_path(db_name: str, size: str, drop_caches: bool, transactional: bool, storage_disk_root_paths: str, threads_count: int) -> str:
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

    if len(storage_disk_root_paths) > 1:
        return f'{root_dir_path}cores_{threads_count}/disks_{len(storage_disk_root_paths)}/{db_name}/{size}.json'
    else:
        return f'{root_dir_path}cores_{threads_count}/disks_1/{db_name}/{size}.json'


def drop_system_caches():
    print('Dropping caches...')
    try:
        with open('/proc/sys/vm/drop_caches', 'w') as stream:
            stream.write('3\n')
        time.sleep(8)
    except KeyboardInterrupt:
        print('\33[2K\r', end='')
        print('Terminated')
        exit(1)
    except:
        print('Failed to drop system caches')
        exit(1)


def run(db_name: str, size: str, workload_names: list, main_dir_root_path: str, storage_disk_root_paths: str, transactional: bool, drop_caches: bool, run_docker_image: bool, threads_count: bool) -> None:
    db_config_file_path = get_db_config_file_path(db_name, size)
    workloads_file_path = get_workloads_file_path(size)
    db_main_dir_path = get_db_main_dir_path(db_name, size, main_dir_root_path)
    db_storage_dir_paths = get_db_storage_dir_paths(
        db_name, size, storage_disk_root_paths)
    results_file_path = get_results_file_path(
        db_name, size, drop_caches, transactional, storage_disk_root_paths, threads_count)

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
                            -c "{db_config_file_path}" \
                            -w "{workloads_file_path}" \
                            -wd "{db_main_dir_path}" \
                            -sd "{db_storage_dir_paths}"\
                            -r "{results_file_path}" \
                            -threads {threads_count} \
                            -filter {filter}'
                            )
    process.interact()
    process.close()

    # Handle signal
    if process.signalstatus:
        print('\33[2K\r', end='')
        sig = signal.Signals(process.signalstatus)
        if sig == signal.SIGINT:
            print(termcolor.colored('Benchmark terminated by user', 'yellow'))
        else:
            print(termcolor.colored(
                f'Benchmark terminated (signal: {sig.name})', 'red'))
        exit(process.signalstatus)


def validate_args(args=[], supported_args: list = []) -> str:

    count = len(supported_args)
    for arg in args:
        i = 0
        for supp_arg in supported_args:
            if supp_arg == arg:
                break
            i += 1

        if i == count:
            return arg

    return 'Ok'


def check_for_path_exists(paths=[]) -> str:
    for path in paths:
        if not os.path.exists(path):
            return path

    return 'Ok'


def check_args(db_names, sizes, workload_names, main_dir_root_path, storage_disk_root_paths, threads_count):
    if db_names is None:
        sys.exit('db_name should be initialized!')

    status = validate_args(db_names, supported_db_names)
    if status != 'Ok':
        sys.exit(status + ' is not supported database!')

    status = validate_args(sizes, supported_sizes)
    if status != 'Ok':
        sys.exit(status + ' is not supported size!')

    if workload_names != supported_workload_names:
        status = validate_args(workload_names, supported_workload_names)
        if status != 'Ok':
            sys.exit(status + ' is not supported workload names!')

    status = check_for_path_exists([main_dir_root_path])
    if status != 'Ok':
        sys.exit(status + ' path is not exists!')

    status = check_for_path_exists(storage_disk_root_paths)
    if status != 'Ok':
        sys.exit(status + ' path is not exists!')

    if not threads_count >= 1:
        sys.exit('Wrong threads count!')


def main(db_names: list = None, sizes: list = ['100MB'], workload_names: Optional[list] = supported_workload_names,
         threads_count: Optional[int] = 1, transactional: Optional[bool] = False, main_dir_root_path: Optional[str] = './tmp/',
         storage_disk_root_paths: Optional[list] = [], cleanup_previous: Optional[bool] = False, drop_caches: Optional[bool] = False,
         run_docker_image: Optional[bool] = False) -> None:

    check_args(db_names, sizes, workload_names,
               main_dir_root_path, storage_disk_root_paths, threads_count)

    # Cleanup old DBs
    if cleanup_previous:
        print('Cleanup...')
        for size in sizes:
            for db_name in db_names:
                # Remove DB main directory
                db_main_dir_path = get_db_main_dir_path(
                    db_name, size, main_dir_root_path)
                if pathlib.Path(db_main_dir_path).exists():
                    shutil.rmtree(db_main_dir_path)
                # Remove DB storage directories
                db_storage_dir_paths = get_db_storage_dir_paths(
                    db_name, size, storage_disk_root_paths)
                for db_storage_dir_path in db_storage_dir_paths:
                    if pathlib.Path(db_storage_dir_path).exists():
                        shutil.rmtree(db_storage_dir_path)

    # Create directories for DB files
    for size in sizes:
        for db_name in db_names:
            # Create DB main directory
            db_main_dir_path = get_db_main_dir_path(
                db_name, size, main_dir_root_path)
            pathlib.Path(db_main_dir_path).mkdir(parents=True, exist_ok=True)
            # Create DB storage directories
            db_storage_dir_paths = get_db_storage_dir_paths(
                db_name, size, storage_disk_root_paths)
            for db_storage_dir_path in db_storage_dir_paths:
                pathlib.Path(db_storage_dir_path).mkdir(
                    parents=True, exist_ok=True)
            # Create results dir paths
            pathlib.Path(get_results_file_path(db_name, size, drop_caches, transactional, storage_disk_root_paths, threads_count)
                         ).parent.mkdir(parents=True, exist_ok=True)

    # Run benchmarks
    for size in sizes:
        for db_name in db_names:
            if drop_caches:
                for workload_name in workload_names:
                    if not run_docker_image:
                        drop_system_caches()
                    run(db_name, size, [workload_name], main_dir_root_path, storage_disk_root_paths,
                        transactional, drop_caches, run_docker_image, threads_count)
            else:
                run(db_name, size, workload_names, main_dir_root_path, storage_disk_root_paths,
                    transactional, drop_caches, run_docker_image, threads_count)


if __name__ == '__main__':
    fire.Fire(main)
