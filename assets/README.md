# Artwork

Editable sources and exports for the README illustrations.

- [LSM tree](lsm-tree.svg): memory and disk layout, sorted files, and compaction.
- [Flash cell states](slc-mlc-tlc-shape.svg): two through sixteen threshold-voltage distributions and their read thresholds.
- [SSD write cache](slc-mlc-tlc-specs.svg): a schematic transition from cached bursts to sustained writes; no measured rates.

The storage illustrations are editable in the [artwork file](https://www.figma.com/design/wMXL55Z6sUZpLOQtwSaaEc?node-id=2-86).
They explain storage concepts, not measured benchmark results.
The LSM sketch follows RocksDB's [leveled compaction model](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction); the example has no active snapshots and keeps a tombstone because older data may remain below.
The NAND figures follow KIOXIA's descriptions of [multi-level cells](https://www.kioxia.com/en-jp/rd/technology/multi-level-cell.html) and [endurance factors](https://americas.kioxia.com/content/dam/kioxia/en-us/business/memory/asset/KIOXIA-SSD-NAND-Endurance-Tech-Brief.pdf).
