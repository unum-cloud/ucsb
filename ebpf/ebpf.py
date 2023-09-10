#!/usr/bin/env python3

import json
import logging
import os
import resource
import signal
from time import sleep, time
from typing import Optional, Tuple

import pexpect
from bcc import BPF
from bcc.syscall import syscall_name
from pexpect import spawn

logging.basicConfig(filename='/tmp/ebpf.log', encoding='utf-8', level=logging.DEBUG)


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


def get_size_filter(min_size, max_size):
    if min_size is not None and max_size is not None:
        return "if (size < %d || size > %d) return 0;" % (min_size, max_size)
    elif min_size is not None:
        return "if (size < %d) return 0;" % min_size
    elif max_size is not None:
        return "if (size > %d) return 0;" % max_size
    else:
        return ""


class Allocation(object):
    def __init__(self, stack, size, address):
        self.stack = stack
        self.count = 1
        self.size = size
        self.address = address

    def update(self, size):
        self.count += 1
        self.size += size


def get_outstanding(bpf, pid, min_age_ns, top):
    alloc_info = {}
    allocs = bpf["allocs"]
    stack_traces = bpf["stack_traces"]
    for address, info in sorted(allocs.items(), key=lambda a: a[1].size):
        if BPF.monotonic_time() - min_age_ns < info.timestamp_ns or info.stack_id < 0:
            continue
        if info.stack_id in alloc_info:
            alloc_info[info.stack_id].update(info.size)
        else:
            stack = list(stack_traces.walk(info.stack_id))
            combined = []
            for addr in stack:
                func_name = bpf.sym(addr, pid, show_module=True, show_offset=True)
                formatted_address = ('0x' + format(addr, '016x') + ' ').encode('utf-8')
                combined.append(formatted_address + func_name)
            alloc_info[info.stack_id] = Allocation(combined, info.size, address.value)

    sorted_stacks = sorted(alloc_info.values(), key=lambda a: -a.size)[:top]
    return list(
        map(lambda alloc: {'stack': [s.decode('ascii') for s in alloc.stack], 'size': alloc.size, 'count': alloc.count},
            sorted_stacks))


class CombinedAlloc(object):
    def __init__(self, item):
        self.stack_id = item[0]
        self.free_size = item[1].free_size
        self.alloc_size = item[1].alloc_size
        self.number_of_frees = item[1].number_of_frees
        self.number_of_allocs = item[1].number_of_allocs

    def key(self):
        return self.alloc_size - self.free_size

    def __str__(self):
        return f"CombinedAlloc(stack_id={self.stack_id},\n" \
               f"\t free_size={self.free_size},\n" \
               f"\t alloc_size={self.alloc_size},\n" \
               f"\t number_of_frees={self.number_of_frees},\n" \
               f"\t number_of_allocs={self.number_of_allocs})\n"

    def __repr__(self):
        return self.__str__()

    def is_positive(self):
        return self.alloc_size > self.free_size


def get_statistics(bpf, pid, min_age_ns, top):
    stack_traces = bpf["stack_traces"]
    combined_alloc = list(
        sorted(
            map(CombinedAlloc, bpf["combined_allocs"].items()),
            key=lambda a: a.key(),
        )
    )
    memory = sum((c.alloc_size - c.free_size for c in combined_alloc))
    entries = []
    for allocation in combined_alloc:
        entries.append({
            'alloc_size': allocation.alloc_size,
            'free_size': allocation.free_size,
            'number_of_allocs': allocation.number_of_allocs,
            'number_of_frees': allocation.number_of_frees,
            'trace': get_trace_info_as_list(bpf, pid, stack_traces, allocation.stack_id.value),
        })
    return {
        "memory": memory,
        "combined_allocs": list(reversed(entries)),
        "stack_traces": len(list(stack_traces.items())),
        "outstanding": get_outstanding(bpf, pid, min_age_ns, top)
    }


def get_trace_info(bpf, pid, stack_traces, stack_id):
    trace = []
    for addr in walk_trace(stack_traces, stack_id):
        sym = bpf.sym(addr, pid, show_module=False, show_offset=True)
        trace.append(sym.decode())

    trace = "\n\t\t".join(trace)
    if not trace:
        trace = "stack information lost"
    return trace


def get_trace_info_as_list(bpf, pid, stack_traces, stack_id):
    trace = []
    for addr in walk_trace(stack_traces, stack_id):
        sym = bpf.sym(addr, pid, show_module=False, show_offset=True)
        trace.append(sym.decode())

    return trace


def walk_trace(stack_traces, stack_id):
    try:
        return stack_traces.walk(stack_id)
    except KeyError:
        return []


def gernel_kernel_cache(bpf, top):
    kernel_cache_allocs = list(
        sorted(filter(lambda a: a[1].alloc_size > a[1].free_size, bpf['kernel_cache_counts'].items()),
               key=lambda a: a[1].alloc_size - a[1].free_size)
    )[:top]
    if not kernel_cache_allocs:
        return
    caches = []
    to_remove = []
    for (k, v) in kernel_cache_allocs:
        caches.append({
            'name': str(k.name, "utf-8"),
            'alloc_count': v.alloc_count,
            'free_count': v.free_count,
            'alloc_size': v.alloc_size,
            'free_size': v.free_size,
        })
        if v.alloc_count >= v.free_count:
            to_remove.append(k)
    if len(to_remove) > 0:
        arr = (type(to_remove[0]) * len(to_remove))(*to_remove)
        bpf['kernel_cache_counts'].items_delete_batch(arr)
    return caches


def get_syscalls(bpf):
    syscall_counts = bpf["syscall_counts"]
    syscalls = {}
    for k, v in syscall_counts.items():
        key = k.value
        syscall_id = key >> 32
        thread_id = ((1 << 32) - 1) & key

        syscalls.setdefault(thread_id, []).append({
            'name': system_call_name(syscall_id),
            'count': v.count,
            'total_ns': v.total_ns,
        })
    syscall_counts.clear()
    return syscalls


def system_call_name(k):
    if k == 435:
        return "clone3"
    return syscall_name(k).decode('ascii')


def get_additional_syscall_info(bpf, pid):
    syscall_counts_stacks = bpf['syscall_counts_stacks']
    syscalls_per_thread = {}
    tid_to_syscall_timestamps = {}
    for k, v in syscall_counts_stacks.items():
        thread_id = ((1 << 32) - 1) & v.pid_tgid
        stack_dict = syscalls_per_thread.setdefault(thread_id, {}).setdefault(system_call_name(v.id), {})
        stack_trace = '\n\t'.join(get_trace_info_as_list(bpf, pid, bpf['stack_traces'], v.stack_id))
        stack_dict[stack_trace] = stack_dict.get(stack_trace, 0) + 1
        tid_to_syscall_timestamps.setdefault(thread_id, []).append(k.value)

    syscalls_per_thread = {thread_id: {
        call_name: [
                {
                    'stack_trace': trace.split('\n\t'),
                    'count': count,
                } for (trace, count) in per_syscall.items()
            ] for (call_name, per_syscall) in calls.items()}
        for (thread_id, calls) in syscalls_per_thread.items()}
    return {
        'syscall_stacks': syscalls_per_thread,
        'tid_to_syscall_timestamps': tid_to_syscall_timestamps
    }


def save_snapshot(
        bpf: BPF,
        pid: int,
        min_age_ns: int,
        top: int,
        snapshot_dir: str,
        with_memory: bool,
        with_syscall_details: bool,
        snapshot_prefix: Optional[str] = None
):
    current_time_millis = int(round(time() * 1000))
    snapshot = {'time': current_time_millis}

    if with_memory:
        snapshot['memory_stats'] = get_statistics(bpf, pid, min_age_ns, top)
        snapshot['kernel_caches'] = gernel_kernel_cache(bpf, top)

    snapshot['syscalls'] = get_syscalls(bpf)

    if with_syscall_details:
        snapshot['syscall_details'] = get_additional_syscall_info(bpf, pid)

    os.makedirs(snapshot_dir, exist_ok=True)
    with open(get_result_file_name(snapshot_dir, snapshot_prefix or 'snapshot', 'json'), 'w') as outfile:
        json.dump(snapshot, outfile, cls=SetEncoder)


def get_result_file_name(dir_name: str, prefix: str, suffix: str):
    index = 0
    path = f'{dir_name}/{prefix}.{suffix}'
    while os.path.isfile(path):
        index += 1
        path = f'{dir_name}/{prefix}_{index}.{suffix}'
    return path


def attach_probes(
        pid: int = -1,
        process: Optional[spawn] = None,
        command: Optional[str] = None,
        alloc_sample_every_n: int = 1,
        max_alloc_size: Optional[int] = None,
        min_alloc_size: Optional[int] = None,
        with_memory: bool = False,
        syscall_details: bool = False,
        communicate_with_signals: bool = False
) -> Optional[Tuple[BPF, int, spawn]]:
    if pid == -1 and command is None and process is None:
        logging.info("Either pid or command or process must be specified")
        return

    if command is not None:
        logging.info(f"Executing '{command}' and tracing the resulting process.")
        process = pexpect.spawn(command)
        pid = process.pid
    elif process is not None:
        pid = process.pid

    if communicate_with_signals:
        signal.signal(signal.SIGUSR2, signal_handler)

    # Constructing probes
    bpf = BPF(src_file='./ebpf/ebpf_main.c',
              usdt_contexts=[],
              cflags=[
                  "-Wno-macro-redefined",
                  f"-DPROCESS_ID={pid}",
                  f"-DSAMPLE_EVERY_N={alloc_sample_every_n}",
                  f"-DPAGE_SIZE={resource.getpagesize()}",
                  f"-DFILTER_BY_SIZE={get_size_filter(min_alloc_size, max_alloc_size)}",
                  "-DWITH_MEMORY" if with_memory else "",
                  "-DCOLLECT_SYSCALL_STACK_INFO" if syscall_details else "",
              ])

    # Attaching probes

    logging.info(f"Attaching to pid {pid}")

    wait_time = 0
    wait_interval = 0.01
    timeout = 5
    while not process.isalive():
        sleep(wait_interval)
        wait_time += wait_interval
        if process.terminated:
            raise Exception(f'Process is already terminated, with status code {process.exitstatus}')
        if wait_time > timeout:
            raise Exception('Process is not alive')

    if with_memory:
        for sym in ["malloc", "calloc", "realloc", "mmap", "posix_memalign", "valloc", "memalign", "pvalloc",
                    "aligned_alloc"]:
            bpf.attach_uprobe(name="c", sym=sym, fn_name=sym + "_enter", pid=pid)
            bpf.attach_uretprobe(name="c", sym=sym, fn_name=sym + "_exit", pid=pid)

        bpf.attach_uprobe(name="c", sym="free", fn_name="free_enter", pid=pid)
        bpf.attach_uprobe(name="c", sym="munmap", fn_name="munmap_enter", pid=pid)

        # kernel cache probes
        bpf.attach_kprobe(event='kmem_cache_alloc_lru', fn_name='trace_cache_alloc')
        bpf.attach_kprobe(event='kmem_cache_alloc_bulk', fn_name='trace_cache_alloc')
        bpf.attach_kprobe(event='kmem_cache_alloc_node', fn_name='trace_cache_alloc')

        bpf.attach_kprobe(event='kmem_cache_free', fn_name='trace_cache_free')
        bpf.attach_kprobe(event='kmem_cache_free_bulk', fn_name='trace_cache_free')

    return bpf, pid, process


SIGUSR2_received = False


def signal_handler(_sig_no, _stack_frame):
    global SIGUSR2_received
    SIGUSR2_received = True


def is_terminated(pid: int, process: Optional[spawn]):
    if process is None:
        try:
            # Sending signal 0 to a process with id {pid} will raise OSError if the process does not exist
            os.kill(pid, 0)
        except OSError:
            return True
        else:
            return False
    return SIGUSR2_received or not process.isalive()


def sleep_and_check(
        interval: float,
        pid: int,
        process: Optional[spawn],
        check_delay: float
):
    wait_til = time() + interval
    while time() < wait_til:
        sleep(check_delay)
        if is_terminated(pid, process):
            break


def harvest_ebpf(
        bpf: BPF,
        pid: int = -1,
        process: Optional[spawn] = None,
        top: int = 10,
        interval: int = 5,
        min_age_ns: int = 500,
        with_memory: bool = False,
        with_syscall_details: bool = False,
        save_snapshots: Optional[str] = None,
        snapshot_prefix: Optional[str] = None,
        communicate_with_signals: bool = False,
):
    if pid == -1 and process is None:
        raise ValueError("Either pid or process must be specified")

    if process is not None:
        pid = process.pid

    while True:
        logging.info(f"Sleeping for {interval} seconds...")
        try:
            sleep_and_check(interval, pid, process, check_delay=0.2)
        except KeyboardInterrupt:
            break
        save_snapshot(bpf, pid, min_age_ns, top, save_snapshots, with_memory, with_syscall_details, snapshot_prefix)
        if is_terminated(pid, process):
            break

    if communicate_with_signals:
        # Sending SIGUSR1 to the process will notify that the tracing is done
        os.kill(pid, signal.SIGUSR1)

    logging.info("Detaching...")
    bpf.cleanup()
