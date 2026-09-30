# Scripts

`plot.py` reads the JSON reports emitted by the benchmark binaries and produces four PNG figures: successful-entry throughput, operation p99 latency, client RSS, and database size.
Each workload gets a separate panel; labels identify the engine, data model, record count, worker count, and configuration hash.
Only completed phases appear.

```sh
uv run scripts/plot.py results/
uv run scripts/plot.py results/ --output figures/
```

The script declares its Python and matplotlib dependencies inline for uv.
It uses a noninteractive rendering backend and accepts either a report directory or one JSON file.
The memory chart covers the client process; it does not imply that a server's memory is included.

## Checks

`./scripts/check.sh` checks module headers, import grouping and spacing, formatting, Clippy, Python lint, and all-feature unit tests using the pinned compiler.
`./scripts/check.sh --quick` checks headers, import grouping and spacing, formatting, Python lint, and the default feature's Clippy targets before a commit.
Install the local hook with `git config core.hooksPath scripts`; CI runs the full checks independently.
Checks do not rewrite files or modify the Git index.

`./scripts/check-servers.sh` runs the supported server/modality combinations against managed containers.
It first runs native document and graph contracts, including integer precision, missing records, and exact two-hop results on a fixed graph.
It requires Docker, writes logs and reports into a fresh temporary directory, and fails on execution errors or corrupted records.
