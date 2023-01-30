#!/usr/bin/env python3

import os
import sys
import time
import shutil
import signal
import pexpect
import pathlib
import termcolor


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
    'ReadUpsert_95_5',
    'BatchUpsert',
    'Remove',
]

# Parallel workers count, a workload divided into this number
threads_count = 1

# Runs a workload by transactions if the DB supports
transactional = False

# Path where a DB stores metadata (may also data if `storage_disk_root_paths` is empty)
main_dir_root_path = './tmp/'

# Disks where a DB stores data (especially for multi disk benchmarks)
storage_disk_root_paths = [
    # '/mnt/disk1/',
    # '/mnt/disk2/',
    # '/mnt/disk3/',
    # '/mnt/disk4/',
]

# Cleans a DB which was created in previous run
cleanup_previous = False

# Drops system caches and runs workloads in separated processes
drop_caches = False

# Runs a workload in docker container (note: build the docker image)
run_docker_image = False


def get_db_config_file_path(db_name: str, size: str) -> str:
    path = f'./bench/configs/{db_name}/{size}.cfg'
    if not pathlib.Path(path).exists():
        path = f'./bench/configs/{db_name}/default.cfg'
    return path


def get_workloads_file_path(size: str) -> str:
    return f'./bench/workloads/{size}.json'


def get_db_main_dir_path(db_name: str, size: str) -> str:
    return f'{main_dir_root_path}{db_name}/{size}/'


def get_db_storage_dir_paths(db_name: str, size: str) -> list:
    db_storage_dir_paths = []
    for storage_disk_path in storage_disk_root_paths:
        if (storage_disk_path.endswith('/')):
            db_storage_dir_paths.append(f'{storage_disk_path}{db_name}/{size}/')
        else:
            db_storage_dir_paths.append(f'{storage_disk_path}/{db_name}/{size}/')
    return db_storage_dir_paths


def get_results_file_path(db_name: str, size: str) -> str:
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


def run(db_name: str, size: str, workload_names: list) -> None:
    db_config_file_path = get_db_config_file_path(db_name, size)
    workloads_file_path = get_workloads_file_path(size)
    db_main_dir_path = get_db_main_dir_path(db_name, size)
    db_storage_dir_paths = get_db_storage_dir_paths(db_name, size)
    results_file_path = get_results_file_path(db_name, size)

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


def main() -> None:
    if os.geteuid() != 0:
        sys.exit('Run as sudo!')

    # Cleanup old DBs
    if cleanup_previous:
        print('Cleanup...')
        for size in sizes:
            for db_name in db_names:
                # Remove DB main directory
                db_main_dir_path = get_db_main_dir_path(db_name, size)
                if pathlib.Path(db_main_dir_path).exists():
                    shutil.rmtree(db_main_dir_path)
                # Remove DB storage directories
                db_storage_dir_paths = get_db_storage_dir_paths(db_name, size)
                for db_storage_dir_path in db_storage_dir_paths:
                    if pathlib.Path(db_storage_dir_path).exists():
                        shutil.rmtree(db_storage_dir_path)

    # Create directories for DB files
    for size in sizes:
        for db_name in db_names:
            # Create DB main directory
            db_main_dir_path = get_db_main_dir_path(db_name, size)
            pathlib.Path(db_main_dir_path).mkdir(parents=True, exist_ok=True)
            # Create DB storage directories
            db_storage_dir_paths = get_db_storage_dir_paths(db_name, size)
            for db_storage_dir_path in db_storage_dir_paths:
                pathlib.Path(db_storage_dir_path).mkdir(
                    parents=True, exist_ok=True)
            # Create results dir paths
            pathlib.Path(get_results_file_path(db_name, size)
                         ).parent.mkdir(parents=True, exist_ok=True)

    # Run benchmarks
    for size in sizes:
        for db_name in db_names:
            if drop_caches:
                for workload_name in workload_names:
                    if not run_docker_image:
                        drop_system_caches()
                    run(db_name, size, [workload_name])
            else:
                run(db_name, size, workload_names)


if __name__ == '__main__':
    main()
