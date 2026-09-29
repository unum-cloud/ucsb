//! Explicit workload names, operation mixes, and checked size parsing.

use crate::data::{Distribution, RandomGenerator};
use serde::Serialize;
use std::{fmt, ops::RangeInclusive, str::FromStr};

pub const DEFAULT_WORKLOADS: &str = "bulk-load,read,batch-read-256,range-read-256,full-scan,read-50-update-50,read-latest-95-insert-5,batch-insert-1000,delete-oldest";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Operation {
    Insert,
    Read,
    Update,
    Delete,
    RangeRead,
    ReadModifyWrite,
    BulkLoad,
    FullScan,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Read => "read",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::RangeRead => "range-read",
            Self::ReadModifyWrite => "read-modify-write",
            Self::BulkLoad => "bulk-load",
            Self::FullScan => "full-scan",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Workload {
    pub name: String,
    pub operations: Vec<(Operation, u32)>,
    pub batch_size: usize,
    pub distribution: Distribution,
    pub range_size: Option<RangeInclusive<usize>>,
}

impl Workload {
    pub fn choose(&self, rng: &mut RandomGenerator) -> Operation {
        let mut draw = rng.below(100) as u32;
        for &(operation, share) in &self.operations {
            if draw < share {
                return operation;
            }
            draw -= share;
        }
        unreachable!("workload shares sum to 100")
    }
}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

impl FromStr for Workload {
    type Err = String;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        use Operation::*;
        let name = input;
        let distribution = Distribution::Zipf;
        let mut result = Self {
            name: name.into(),
            operations: Vec::new(),
            batch_size: 1,
            distribution,
            range_size: None,
        };
        result.operations = match name {
            "bulk-load" => {
                result.batch_size = 100_000;
                vec![(BulkLoad, 100)]
            }
            "read" => vec![(Read, 100)],
            "full-scan" => {
                result.batch_size = 256;
                vec![(FullScan, 100)]
            }
            "read-50-update-50" => vec![(Read, 50), (Update, 50)],
            "read-95-update-5" => vec![(Read, 95), (Update, 5)],
            "read-latest-95-insert-5" => {
                result.distribution = Distribution::Latest;
                vec![(Read, 95), (Insert, 5)]
            }
            "range-read-95-insert-5" => {
                result.range_size = Some(1..=100);
                vec![(RangeRead, 95), (Insert, 5)]
            }
            "read-50-read-modify-write-50" => vec![(Read, 50), (ReadModifyWrite, 50)],
            "delete-oldest" => vec![(Delete, 100)],
            _ => {
                let (prefix, count, operation) = [
                    ("batch-read-", Read),
                    ("range-read-", RangeRead),
                    ("batch-insert-", Insert),
                    ("bulk-load-", BulkLoad),
                ]
                .into_iter()
                .find_map(|(prefix, operation)| name.strip_prefix(prefix).map(|n| (prefix, n, operation)))
                .ok_or_else(|| format!("unknown workload: {name}"))?;
                result.batch_size =
                    usize::try_from(parse_count(count)?).map_err(|_| "batch size exceeds platform limit")?;
                if result.batch_size == 0 {
                    return Err("batch size must be positive".into());
                }
                result.name = format!("{prefix}{}", result.batch_size);
                vec![(operation, 100)]
            }
        };
        Ok(result)
    }
}

pub fn parse_count(input: &str) -> Result<u64, String> {
    let split = input.find(|c: char| !c.is_ascii_digit()).unwrap_or(input.len());
    let value: u64 = input[..split]
        .parse()
        .map_err(|_| format!("invalid integer: {input}"))?;
    let multiplier = match &input[split..] {
        "" | "B" => 1,
        "K" | "KB" => 1_000,
        "M" | "MB" => 1_000_000,
        "G" | "GB" => 1_000_000_000,
        "T" | "TB" => 1_000_000_000_000,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        _ => return Err(format!("invalid size suffix: {input}")),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| format!("integer overflow: {input}"))
}

pub fn parse_value_size(input: &str) -> Result<RangeInclusive<usize>, String> {
    let (min, max) = input.split_once("..").unwrap_or((input, input));
    let min = usize::try_from(parse_count(min)?).map_err(|_| "value size exceeds platform limit")?;
    let max = usize::try_from(parse_count(max)?).map_err(|_| "value size exceeds platform limit")?;
    if min < 24 || min > max {
        return Err("value sizes must be at least 24 bytes and the range must be ascending".into());
    }
    Ok(min..=max)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_workloads_round_trip() {
        for name in DEFAULT_WORKLOADS.split(',').chain([
            "read-95-update-5",
            "range-read-95-insert-5",
            "read-50-read-modify-write-50",
        ]) {
            let workload: Workload = name.parse().unwrap();
            assert_eq!(workload.to_string(), name);
            assert_eq!(workload.operations.iter().map(|(_, p)| p).sum::<u32>(), 100);
        }
        assert_eq!("batch-insert-1K".parse::<Workload>().unwrap().name, "batch-insert-1000");
        for invalid in [
            "R100",
            "batch-read-0",
            "read~uniform",
            "read-latest-95-insert-5~uniform",
        ] {
            assert!(invalid.parse::<Workload>().is_err());
        }
    }
    #[test]
    fn sizes_are_checked() {
        assert_eq!(parse_count("1M").unwrap(), 1_000_000);
        assert_eq!(parse_value_size("24B..1KiB").unwrap(), 24..=1024);
        for invalid in ["18446744073709551615K", "1.5K", "-1", "K"] {
            assert!(parse_count(invalid).is_err());
        }
        for invalid in ["23", "1024..24", "24.."] {
            assert!(parse_value_size(invalid).is_err());
        }
    }
}
