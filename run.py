#!/usr/bin/env python3

import argparse
import os
import pathlib
import shutil
import signal
import sys
import time
from threading import Thread
from typing import List

import pexpect
import termcolor

"""
Below are listed the supported databases, benchmark sizes, workloads and more settings.
All of them are optional and treated as defaults, so you can select settings here or pass arguments from outside.
Usage: sudo ./run.py [arguments], type -h or --help for help.
Note there are some databases and sizes commented by default just to minimize the sample run, so select them according to your needs.
"""

db_names = [
    "ustore",
    "rocksdb",
    "leveldb",
    "wiredtiger",
    # "mongodb",
    # "redis",
    # "lmdb",
]

sizes = [
    "100MB",
    # "1GB",
    # "10GB",
    # "100GB",
    # "1TB",
    # "10TB",
]

workload_names = [
    "Init",
    "Read",
    "BatchRead",
    "RangeSelect",
    "Scan",
    "ReadUpdate_50_50",
    "ReadUpsert_95_5",
    "BatchUpsert",
    "Remove",
]

threads_count = 1
transactional = False

drop_caches = False
run_in_docker_container = False
with_ebpf = False
with_ebpf_memory = False
with_syscall_details = False

main_dir_path = "./db_main/"
storage_disk_paths = [
    # '/disk1/db_storage/',
    # '/disk2/db_storage/',
]


def get_db_config_file_path(db_name: str, size: str) -> str:
    path = os.path.join("./bench/configs", db_name, f"{size}.cfg")
    if not pathlib.Path(path).exists():
        path = os.path.join("./bench/configs", db_name, "default.cfg")
    return path


def get_workloads_file_path(size: str) -> str:
    return os.path.join("./bench/workloads", f"{size}.json")


def get_db_main_dir_path(db_name: str, size: str, main_dir_path: str) -> str:
    return os.path.join(main_dir_path, db_name, size, "")


def get_db_storage_dir_paths(db_name: str, size: str, storage_disk_paths: List[str]) -> list:
    db_storage_dir_paths = []
    for storage_disk_path in storage_disk_paths:
        db_storage_dir_paths.append(os.path.join(storage_disk_path, db_name, size, ""))
    return db_storage_dir_paths


def get_results_file_path(db_name: str,
                          size: str,
                          drop_caches: bool,
                          transactional: bool,
                          storage_disk_paths: List[str],
                          threads_count: int,
                          ) -> str:
    root_dir_path = ""
    if drop_caches:
        if transactional:
            root_dir_path = "./bench/results/without_caches/transactional/"
        else:
            root_dir_path = "./bench/results/without_caches/"
    else:
        if transactional:
            root_dir_path = "./bench/results/transactional/"
        else:
            root_dir_path = "./bench/results/"

    disks_count = max(len(storage_disk_paths), 1)
    return os.path.join(
        f"{root_dir_path}cores_{threads_count}",
        f"disks_{disks_count}",
        db_name,
        f"{size}.json",
    )


def drop_system_caches():
    print(end="\x1b[1K\r")
    print(" [✱] Dropping system caches...", end="\r")
    try:
        with open("/proc/sys/vm/drop_caches", "w") as stream:
            stream.write("3\n")
        time.sleep(8)  # Wait for other apps to reload its caches
    except KeyboardInterrupt:
        print(termcolor.colored("Terminated by user", "yellow"))
        exit(1)
    except:
        print(termcolor.colored("Failed to drop system caches", "red"))
        exit(1)


def run(
        db_name: str,
        size: str,
        workload_names: list,
        main_dir_path: str,
        storage_disk_paths: List[str],
        transactional: bool,
        drop_caches: bool,
        run_in_docker_container: bool,
        threads_count: int,
        run_index: int,
        runs_count: int,
        with_ebpf: bool,
        with_ebpf_memory: bool,
        with_syscall_details: bool) -> None:
    db_config_file_path = get_db_config_file_path(db_name, size)
    workloads_file_path = get_workloads_file_path(size)
    db_main_dir_path = get_db_main_dir_path(db_name, size, main_dir_path)
    db_storage_dir_paths = get_db_storage_dir_paths(db_name, size, storage_disk_paths)
    results_file_path = get_results_file_path(
        db_name, size, drop_caches, transactional, storage_disk_paths, threads_count
    )

    transactional_flag = "-t" if transactional else ""
    lazy_flag = "-l" if with_ebpf else ""
    filter = ",".join(workload_names)
    db_storage_dir_paths = ",".join(db_storage_dir_paths)

    runner: str
    if run_in_docker_container:
        runner = f"docker run -v {os.getcwd()}/bench:/ucsb/bench -v {os.getcwd()}/tmp:/ucsb/tmp -it ucsb-image-dev"
    else:
        runner = "./build_release/build/bin/ucsb_bench"
        if not os.path.exists(runner):
            raise Exception("First, please build the runner: `build_release.sh`")

    process = pexpect.spawn(
        f'{runner} -db {db_name} {transactional_flag} {lazy_flag} -cfg "{db_config_file_path}" -wl "{workloads_file_path}" -md "{db_main_dir_path}" -sd "{db_storage_dir_paths}" -res "{results_file_path}" -th {threads_count} -fl {filter} -ri {run_index} -rc {runs_count}'
    )
    thread = None
    if with_ebpf:
        from ebpf.ebpf import attach_probes, harvest_ebpf
        bpf, pid, process = attach_probes(
            process=process,
            syscall_details=with_syscall_details,
            with_memory=with_ebpf_memory,
            communicate_with_signals=True,
        )
        # Send SIGUSR1 to the process to notify it that the probes are attached
        os.kill(process.pid, signal.SIGUSR1)
        thread = Thread(target=harvest_ebpf, args=(bpf,), kwargs={
            process: process,
            "interval": 5,
            "with_memory": with_ebpf_memory,
            "with_syscall_details": with_syscall_details,
            "snapshot_prefix": "-".join(workload_names),
            "save_snapshots": f"./bench/ebpf/snapshots/{db_name}_{size}",
            "communicate_with_signals": True,
        })
        thread.start()
    if with_ebpf:
        thread.join()

    process.interact()
    process.close()

    # Handle signal
    if process.signalstatus:
        sig = signal.Signals(process.signalstatus)
        if sig == signal.SIGINT:
            print(termcolor.colored("Benchmark terminated by user", "yellow"))
        else:
            print(
                termcolor.colored(f"Benchmark terminated (signal: {sig.name})", "red")
            )
        exit(process.signalstatus)


def parse_args():
    global db_names
    global sizes
    global workload_names
    global main_dir_path
    global storage_disk_paths
    global threads_count
    global transactional
    global drop_caches
    global run_in_docker_container
    global with_ebpf
    global with_ebpf_memory
    global with_syscall_details

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-db",
        "--db-names",
        help="Database name(s)",
        nargs="+",
        required=False,
        default=db_names,
    )
    parser.add_argument(
        "-sz",
        "--sizes",
        help="Benchmark size(s)",
        nargs="+",
        required=False,
        default=sizes,
    )
    parser.add_argument(
        "-wl",
        "--workload-names",
        help="Workload name(s)",
        nargs="+",
        required=False,
        default=workload_names,
    )
    parser.add_argument(
        "-md",
        "--main-dir",
        help="Main directory",
        type=str,
        required=False,
        default=main_dir_path,
    )
    parser.add_argument(
        "-sd",
        "--storage-dirs",
        help="Storage directories",
        nargs="+",
        required=False,
        default=storage_disk_paths,
    )
    parser.add_argument(
        "-th",
        "--threads",
        help="Threads count",
        type=int,
        required=False,
        default=threads_count,
    )
    parser.add_argument(
        "-tx",
        "--transactional",
        help="Transactional benchmark",
        action=argparse.BooleanOptionalAction,
        default=transactional,
    )
    parser.add_argument(
        "-dp",
        "--drop-caches",
        help="Drops system cashes before each benchmark",
        action=argparse.BooleanOptionalAction,
        default=drop_caches,
    )
    parser.add_argument(
        "-rd",
        "--run-docker",
        help="Runs the benchmark in a docker container",
        action=argparse.BooleanOptionalAction,
        default=run_in_docker_container,
    )
    parser.add_argument(
        "-eb",
        "--with-ebpf",
        help="Runs ebpf benchmarks",
        default=with_ebpf,
        dest="with_ebpf",
        action=argparse.BooleanOptionalAction
    )
    parser.add_argument(
        "-em",
        "--with-ebpf-memory",
        help="Enable memory related ebpf benchmarks",
        default=with_ebpf_memory,
        dest="with_ebpf_memory",
        action=argparse.BooleanOptionalAction
    )
    parser.add_argument(
        "-es",
        "--with-ebpf-syscall-details",
        help="Collect eBPF syscall stack traces",
        default=with_syscall_details,
        dest="with_syscall_details",
        action=argparse.BooleanOptionalAction
    )

    args = parser.parse_args()
    db_names = args.db_names
    sizes = args.sizes
    workload_names = args.workload_names
    main_dir_path = args.main_dir
    storage_disk_paths = args.storage_dirs
    threads_count = args.threads
    transactional = args.transactional
    drop_caches = args.drop_caches
    run_in_docker_container = args.run_docker
    with_ebpf = args.with_ebpf
    with_ebpf_memory = args.with_ebpf_memory
    with_syscall_details = args.with_syscall_details


def check_args():
    global db_names
    global sizes
    global workload_names

    if not db_names:
        sys.exit("Database(s) not specified")
    if not sizes:
        sys.exit("Database size(s) not specified")
    if not workload_names:
        sys.exit("Workload name(s) not specified")
    if run_in_docker_container and with_ebpf:
        sys.exit("Running ebpf benchmarks in docker container is not supported")
    if with_ebpf_memory and not with_ebpf:
        sys.exit("Memory related ebpf benchmarks require ebpf benchmarks to be enabled, run with --with-ebpf flag")


def main() -> None:
    if os.geteuid() != 0:
        print(termcolor.colored(f"Run as sudo!", "red"))
        sys.exit(-1)

    parse_args()
    check_args()

    # Cleanup old DBs (Note: It actually cleanups if the first workload is `Init`)
    if workload_names[0] == "Init":
        print(end="\x1b[1K\r")
        print(" [✱] Cleanup...", end="\r")
        for size in sizes:
            for db_name in db_names:
                # Remove DB main directory
                db_main_dir_path = get_db_main_dir_path(db_name, size, main_dir_path)
                if pathlib.Path(db_main_dir_path).exists():
                    shutil.rmtree(db_main_dir_path)

                # Remove DB storage directories
                db_storage_dir_paths = get_db_storage_dir_paths(
                    db_name, size, storage_disk_paths
                )
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
            db_storage_dir_paths = get_db_storage_dir_paths(
                db_name, size, storage_disk_paths
            )
            for db_storage_dir_path in db_storage_dir_paths:
                pathlib.Path(db_storage_dir_path).mkdir(parents=True, exist_ok=True)

            # Create results dir paths
            results_file_path = get_results_file_path(
                db_name,
                size,
                drop_caches,
                transactional,
                storage_disk_paths,
                threads_count,
            )
            pathlib.Path(results_file_path).parent.mkdir(parents=True, exist_ok=True)

            # Run benchmark
            if drop_caches:
                for i, workload_name in enumerate(workload_names):
                    if not run_in_docker_container:
                        drop_system_caches()
                    run(
                        db_name,
                        size,
                        [workload_name],
                        main_dir_path,
                        storage_disk_paths,
                        transactional,
                        drop_caches,
                        run_in_docker_container,
                        threads_count,
                        i,
                        len(workload_names),
                        with_ebpf,
                        with_ebpf_memory,
                        with_syscall_details
                    )
            else:
                run(
                    db_name,
                    size,
                    workload_names,
                    main_dir_path,
                    storage_disk_paths,
                    transactional,
                    drop_caches,
                    run_in_docker_container,
                    threads_count,
                    0,
                    1,
                    with_ebpf,
                    with_ebpf_memory,
                    with_syscall_details
                )


if __name__ == "__main__":
    main()
