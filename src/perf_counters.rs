//! Optional Linux hardware counters scoped to the calling benchmark worker.

use serde::Serialize;

#[derive(Default, Serialize)]
pub struct CounterSample {
    pub cycles: u64,
    pub instructions: u64,
    pub cache_misses: u64,
    pub branch_misses: u64,
}

#[cfg(all(target_os = "linux", feature = "perf-counters"))]
pub struct PerfCounters {
    group: perf_event::Group,
    cycles: perf_event::Counter,
    instructions: perf_event::Counter,
    cache_misses: perf_event::Counter,
    branch_misses: perf_event::Counter,
}

#[cfg(all(target_os = "linux", feature = "perf-counters"))]
impl PerfCounters {
    pub fn start() -> std::io::Result<Self> {
        use perf_event::{events::Hardware, Builder, Group};
        let mut group = Group::new()?;
        let cycles = group.add(&Builder::new(Hardware::CPU_CYCLES))?;
        let instructions = group.add(&Builder::new(Hardware::INSTRUCTIONS))?;
        let cache_misses = group.add(&Builder::new(Hardware::CACHE_MISSES))?;
        let branch_misses = group.add(&Builder::new(Hardware::BRANCH_MISSES))?;
        group.enable()?;
        Ok(Self {
            group,
            cycles,
            instructions,
            cache_misses,
            branch_misses,
        })
    }
    pub fn finish(mut self) -> std::io::Result<CounterSample> {
        self.group.disable()?;
        let counts = self.group.read()?;
        Ok(CounterSample {
            cycles: counts[&self.cycles],
            instructions: counts[&self.instructions],
            cache_misses: counts[&self.cache_misses],
            branch_misses: counts[&self.branch_misses],
        })
    }
}

#[cfg(not(all(target_os = "linux", feature = "perf-counters")))]
pub struct PerfCounters;

#[cfg(not(all(target_os = "linux", feature = "perf-counters")))]
impl PerfCounters {
    pub fn start() -> std::io::Result<Self> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "requires Linux and the perf-counters feature",
        ))
    }
    pub fn finish(self) -> std::io::Result<CounterSample> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "performance counters unavailable",
        ))
    }
}
