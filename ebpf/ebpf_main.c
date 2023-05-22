#include <uapi/linux/ptrace.h>
#include <linux/mm.h>
#include <linux/kasan.h>
#include <linux/nsproxy.h>
#include <linux/pid_namespace.h>
#include <linux/sched.h>

#define FILTER_BY_PID u32 __pid = bpf_get_current_pid_tgid() >> 32;if (__pid != PROCESS_ID) {return 0;}

#ifdef WITH_MEMORY

struct alloc_info_t {
    u64 size;
    u64 timestamp_ns;
    int stack_id;
};

struct combined_alloc_info_t {
    u64 alloc_size;
    u64 free_size;
    u64 number_of_allocs;
    u64 number_of_frees;
};

// count of allocations per stack trace
BPF_HASH(combined_allocs, u64, struct combined_alloc_info_t, 10240);

BPF_HASH(sizes, u64);
BPF_HASH(allocs, u64, struct alloc_info_t, 1000000);
BPF_HASH(memptrs, u64, u64);

BPF_STACK_TRACE(stack_traces, 10240);

static inline void update_statistics_add(u64 stack_id, u64 sz) {
    struct combined_alloc_info_t *existing_cinfo;
    struct combined_alloc_info_t cinfo = {0};

    existing_cinfo = combined_allocs.lookup(&stack_id);
    if (existing_cinfo != 0)
        cinfo = *existing_cinfo;

    lock_xadd(&cinfo.alloc_size, sz);
    lock_xadd(&cinfo.number_of_allocs, 1);

    combined_allocs.update(&stack_id, &cinfo);
}

static inline void update_statistics_del(u64 stack_id, u64 sz) {
    struct combined_alloc_info_t *existing_cinfo;
    struct combined_alloc_info_t cinfo = {0};

    existing_cinfo = combined_allocs.lookup(&stack_id);
    if (existing_cinfo != 0)
        cinfo = *existing_cinfo;

    lock_xadd(&cinfo.free_size, sz);
    lock_xadd(&cinfo.number_of_frees, 1);


    combined_allocs.update(&stack_id, &cinfo);
}

static inline int gen_alloc_enter(struct pt_regs *ctx, size_t size) {
    FILTER_BY_PID

    FILTER_BY_SIZE
    if (SAMPLE_EVERY_N > 1) {
        u64 ts = bpf_ktime_get_ns();
        if (ts % SAMPLE_EVERY_N != 0)
            return 0;
    }

    u64 pid = bpf_get_current_pid_tgid();
    u64 size64 = size;
    sizes.update(&pid, &size64);

    return 0;
}

static inline int gen_alloc_exit2(struct pt_regs *ctx, u64 address) {
    FILTER_BY_PID

    u64 pid = bpf_get_current_pid_tgid();
    u64 * size64 = sizes.lookup(&pid);
    struct alloc_info_t info = {0};

    if (size64 == 0)
        return 0; // missed alloc entry

    info.size = *size64;
    sizes.delete(&pid);

    if (address != 0) {
        info.timestamp_ns = bpf_ktime_get_ns();
        info.stack_id = stack_traces.get_stackid(ctx, BPF_F_USER_STACK);
        allocs.update(&address, &info);
        update_statistics_add(info.stack_id, info.size);
    }

    return 0;
}

static inline int gen_alloc_exit(struct pt_regs *ctx) {
    return gen_alloc_exit2(ctx, PT_REGS_RC(ctx));
}

static inline int gen_free_enter(struct pt_regs *ctx, void *address) {
    FILTER_BY_PID

    u64 addr = (u64)address;
    struct alloc_info_t *info = allocs.lookup(&addr);
    if (info == 0)
        return 0;

    allocs.delete(&addr);
    update_statistics_del(info->stack_id, info->size);

    return 0;
}

/** Probes */

int malloc_enter(struct pt_regs *ctx, size_t size) {
    return gen_alloc_enter(ctx, size);
}

int malloc_exit(struct pt_regs *ctx) { return gen_alloc_exit(ctx); }

int free_enter(struct pt_regs *ctx, void *address) {
    return gen_free_enter(ctx, address);
}

int calloc_enter(struct pt_regs *ctx, size_t nmemb, size_t size) {
    return gen_alloc_enter(ctx, nmemb * size);
}

int calloc_exit(struct pt_regs *ctx) { return gen_alloc_exit(ctx); }

int realloc_enter(struct pt_regs *ctx, void *ptr, size_t size) {
    gen_free_enter(ctx, ptr);
    return gen_alloc_enter(ctx, size);
}

int realloc_exit(struct pt_regs *ctx) { return gen_alloc_exit(ctx); }

int mmap_enter(struct pt_regs *ctx) {
    size_t size = (size_t) PT_REGS_PARM2(ctx);
    return gen_alloc_enter(ctx, size);
}

int mmap_exit(struct pt_regs *ctx) { return gen_alloc_exit(ctx); }

int munmap_enter(struct pt_regs *ctx, void *address) {
    return gen_free_enter(ctx, address);
}

int posix_memalign_enter(struct pt_regs *ctx, void **memptr, size_t alignment,
                         size_t size) {
    u64 memptr64 = (u64)(size_t)memptr;
    u64 pid = bpf_get_current_pid_tgid();

    memptrs.update(&pid, &memptr64);
    return gen_alloc_enter(ctx, size);
}

int posix_memalign_exit(struct pt_regs *ctx) {
    u64 pid = bpf_get_current_pid_tgid();
    u64 *memptr64 = memptrs.lookup(&pid);
    void *addr;

    if (memptr64 == 0)
        return 0;

    memptrs.delete(&pid);

    if (bpf_probe_read_user(&addr, sizeof(void *), (void *) (size_t) * memptr64))
        return 0;

    u64 addr64 = (u64)(size_t) addr;
    return gen_alloc_exit2(ctx, addr64);
}

int aligned_alloc_enter(struct pt_regs *ctx, size_t alignment, size_t size) {
    return gen_alloc_enter(ctx, size);
}

int aligned_alloc_exit(struct pt_regs *ctx) { return gen_alloc_exit(ctx); }

int valloc_enter(struct pt_regs *ctx, size_t size) {
    return gen_alloc_enter(ctx, size);
}

int valloc_exit(struct pt_regs *ctx) { return gen_alloc_exit(ctx); }

int memalign_enter(struct pt_regs *ctx, size_t alignment, size_t size) {
    return gen_alloc_enter(ctx, size);
}

int memalign_exit(struct pt_regs *ctx) { return gen_alloc_exit(ctx); }

int pvalloc_enter(struct pt_regs *ctx, size_t size) {
    return gen_alloc_enter(ctx, size);
}

int pvalloc_exit(struct pt_regs *ctx) { return gen_alloc_exit(ctx); }

/** Tracepoints
    Function names are in tracepoint__{{category}}__{{event}}
    alternatively we can use attach_tracepoint function in python api
*/

int tracepoint__kmem__kmalloc(struct tracepoint__kmem__kmalloc *args) {
    gen_alloc_enter((struct pt_regs *) args, args->bytes_alloc);
    return gen_alloc_exit2((struct pt_regs *) args, (size_t) args->ptr);
}

int tracepoint__kmem__kfree(struct tracepoint__kmem__kfree *args) {
    return gen_free_enter((struct pt_regs *) args, (void *) args->ptr);
}

int tracepoint__kmem__kmem_cache_alloc(
        struct tracepoint__kmem__kmem_cache_alloc *args) {
    gen_alloc_enter((struct pt_regs *) args, args->bytes_alloc);
    return gen_alloc_exit2((struct pt_regs *) args, (size_t) args->ptr);
}

int tracepoint__kmem__kmem_cache_free(
        struct tracepoint__kmem__kmem_cache_free *args) {
    return gen_free_enter((struct pt_regs *) args, (void *) args->ptr);
}

int tracepoint__kmem__mm_page_alloc(
        struct tracepoint__kmem__mm_page_alloc *args) {
    gen_alloc_enter((struct pt_regs *) args, PAGE_SIZE << args->order);
    return gen_alloc_exit2((struct pt_regs *) args, args->pfn);
}

int tracepoint__kmem__mm_page_free(
        struct tracepoint__kmem__mm_page_free *args) {
    return gen_free_enter((struct pt_regs *) args, (void *) args->pfn);
}

int tracepoint__kmem__kmalloc_node(struct tracepoint__kmem__kmalloc_node *args) {
    gen_alloc_enter((struct pt_regs *) args, args->bytes_alloc);
    return gen_alloc_exit2((struct pt_regs *) args, (size_t) args->ptr);
}

int tracepoint__kmem__kmem_cache_alloc_node(struct tracepoint__kmem__kmem_cache_alloc_node *args) {
    gen_alloc_enter((struct pt_regs *) args, args->bytes_alloc);
    return gen_alloc_exit2((struct pt_regs *) args, (size_t) args->ptr);
}

/** kernel cache */

// to resolve undefined error
// taken from bcc slabratetop tool
struct slab {
    unsigned long __page_flags;
#if defined(CONFIG_SLAB)
    struct kmem_cache *slab_cache;
    union {
        struct {
            struct list_head slab_list;
            void *freelist; /* array of free object indexes */
            void *s_mem;    /* first object */
        };
        struct rcu_head rcu_head;
    };
    unsigned int active;
#elif defined(CONFIG_SLUB)
    struct kmem_cache *slab_cache;
    union {
        struct {
            union {
                struct list_head slab_list;
#ifdef CONFIG_SLUB_CPU_PARTIAL
                struct {
                    struct slab *next;
                        int slabs;      /* Nr of slabs left */
                };
#endif
            };
            /* Double-word boundary */
            void *freelist;         /* first free object */
            union {
                unsigned long counters;
                struct {
                    unsigned inuse:16;
                    unsigned objects:15;
                    unsigned frozen:1;
                };
            };
        };
        struct rcu_head rcu_head;
    };
    unsigned int __unused;
#elif defined(CONFIG_SLOB)
    struct list_head slab_list;
    void *__unused_1;
    void *freelist;         /* first free block */
    long units;
    unsigned int __unused_2;
#else
#error "Unexpected slab allocator configured"
#endif
    atomic_t __page_refcount;
#ifdef CONFIG_MEMCG
    unsigned long memcg_data;
#endif
};

// slab_address() will not be used, and NULL will be returned directly, which
// can avoid adaptation of different kernel versions
static inline void *slab_address(const struct slab *slab) {
    return NULL;
}

#ifdef CONFIG_SLUB

#include <linux/slub_def.h>

#else

#include <linux/slab_def.h>

#endif

struct key_t {
    char name[32];
};

struct val_t {
    u64 alloc_count;
    u64 alloc_size;
    u64 free_count;
    u64 free_size;
};

BPF_HASH(kernel_cache_counts,
struct key_t, struct val_t);

int trace_cache_alloc(struct pt_regs *ctx, struct kmem_cache *cachep) {
    FILTER_BY_PID

    u64 size = cachep->size;

    FILTER_BY_SIZE

    struct key_t key = {};
    bpf_probe_read_kernel(&key.name, sizeof(key.name), cachep->name);

    struct val_t empty_val_t = {};

    struct val_t *val = kernel_cache_counts.lookup_or_try_init(&key, &empty_val_t);
    if (val) {
        val->alloc_count++;
        val->alloc_size += size;
    }

    return 0;
}

int trace_cache_free(struct pt_regs *ctx, struct kmem_cache *cachep) {
    FILTER_BY_PID

    u64 size = cachep->size;

    FILTER_BY_SIZE

    struct key_t key = {};
    bpf_probe_read_kernel(&key.name, sizeof(key.name), cachep->name);

    struct val_t empty_val_t = {};
    struct val_t *val = kernel_cache_counts.lookup_or_try_init(&key, &empty_val_t);
    if (val) {
        val->free_count ++;
        val->free_size += size;
    }

    return 0;
}

#endif


/** System Calls */

struct sys_call_data_t {
    u64 count;
    u64 total_ns;
};
BPF_HASH(syscall_start, u64, u64);
BPF_HASH(syscall_counts, u32, struct sys_call_data_t);

int tracepoint__raw_syscalls__sys_enter(struct tracepoint__raw_syscalls__sys_enter *args) {
    FILTER_BY_PID
    u64 pid_tgid = bpf_get_current_pid_tgid();
    u64 t = bpf_ktime_get_ns();
    syscall_start.update(&pid_tgid, &t);
    return 0;
}

int tracepoint__raw_syscalls__sys_exit(struct tracepoint__raw_syscalls__sys_exit *args) {
    FILTER_BY_PID

    u64 pid_tgid = bpf_get_current_pid_tgid();

    struct sys_call_data_t *val, zero = {};
    u64 *start_ns = syscall_start.lookup(&pid_tgid);
    if (!start_ns)
        return 0;
    u32 key = args->id;
    val = syscall_counts.lookup_or_try_init(&key, &zero);
    if (val) {
        lock_xadd(&val->count, 1);
        lock_xadd(&val->total_ns, bpf_ktime_get_ns() - *start_ns);
    }
    return 0;
}