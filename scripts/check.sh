#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
for source in src/*.rs; do
    IFS= read -r header < "$source"
    case "$header" in
        '//!'*) ;;
        *) echo "$source: missing module documentation header" >&2; exit 1 ;;
    esac
done
cargo fmt --all -- --check
python3 scripts/check_imports.py
git diff --check
uvx ruff check --target-version py311 scripts/plot.py scripts/check_imports.py
uvx ruff format --check scripts/plot.py scripts/check_imports.py
if [ "${1:-}" = "--quick" ]; then
    cargo clippy --all-targets -- -D warnings
else
    cargo test --all-features
    cargo clippy --all-targets --all-features -- -D warnings
fi
