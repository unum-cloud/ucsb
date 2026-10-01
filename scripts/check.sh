#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

# Rejects Rust imports out of the standard library, external crates, own crate order, or without a blank line
# between those groups and after the last one, which rustfmt leaves alone. It reads the whole source from stdin.
check_rust_use_groups() { # check_rust_use_groups <own-crate> <path> < source
    local findings
    findings=$(awk -v own="$1" -v path="$2" -v quote="'" '
        BEGIN { literal = "r#*\"|\"|" quote "(\\\\.|[^" quote "\\\\])" quote "|//|/\\*" }
        function blanks(width) { return sprintf("%" width "s", "") }
        function count(text, bracket) { return gsub(bracket, "", text) }
        function group(root) {
            if (root == "std" || root == "core" || root == "alloc") return 0
            return root == own || root == "crate" || root == "self" || root == "super" ? 2 : 1
        }
        function blank(from, to,   line) {
            for (line = from; line < to; ++line) if (source[line] ~ /[^[:space:]]/) return 0
            return 1
        }
        {
            source[NR] = $0
            text = $0; masked = ""; plain = 0
            while (text != "") {
                if (depth) {
                    if (!match(text, /\/\*|\*\//)) break
                    depth += substr(text, RSTART, 2) == "/*" ? 1 : -1
                    taken = RSTART + 1
                } else if (closer != "" && !raw) {
                    if (!match(text, /^([^"\\]|\\.)*"/)) break
                    closer = ""; taken = RLENGTH
                } else if (closer != "") {
                    if (!(taken = index(text, closer))) break
                    taken += length(closer) - 1; closer = ""
                } else if (match(text, literal)) {
                    masked = masked substr(text, 1, RSTART - 1)
                    token = substr(text, RSTART, RLENGTH); text = substr(text, RSTART); taken = RLENGTH
                    if (token == "//") break
                    if (token == "/*") depth = 1
                    else if (token ~ /"$/) { raw = token ~ /^r/; closer = "\"" substr(token, 2, RLENGTH - 2) }
                } else { plain = 1; break }
                masked = masked blanks(taken); text = substr(text, taken + 1)
            }
            code[NR] = masked (plain ? text : blanks(length(text)))
        }
        END {
            for (line = 1; line <= NR; ++line) {
                if (!match(code[line], /^ *(pub(\([^)]*\))? +)?use +(::)?[A-Za-z0-9_]+/)) continue
                head = substr(code[line], 1, RLENGTH); rest = substr(code[line], RLENGTH + 1)
                root = head; sub(/^.*[ :]/, "", root)
                for (end = line; !index(rest, ";") && end < NR; rest = code[++end]) {}
                start = line
                while (start > 1 && code[start - 1] ~ /\][[:space:]]*$/) {
                    candidate = start; nesting = 0
                    while (candidate > 1) {
                        --candidate
                        nesting += count(code[candidate], "]") - count(code[candidate], "\\[")
                        if (nesting == 0) break
                    }
                    if (code[candidate] !~ /^[[:space:]]*#\[/) break
                    start = candidate
                }
                match(head, /^ */)
                ++uses; starts[uses] = start; ends[uses] = end; indents[uses] = RLENGTH; roots[uses] = root
            }
            for (use = 1; use <= uses; ++use) {
                category = group(roots[use]); after = source[ends[use] + 1]; gsub(/^[[:space:]]+|[[:space:]]+$/, "", after)
                if (use < uses && indents[use + 1] == indents[use] && blank(ends[use] + 1, starts[use + 1])) {
                    if (group(roots[use + 1]) < category)
                        printf "%s:%d: order imports as standard library, external crates, own crate\n", path, starts[use]
                    if (group(roots[use + 1]) != category && ends[use] + 1 == starts[use + 1])
                        printf "%s:%d: separate import groups with a blank line\n", path, ends[use]
                } else if (ends[use] < NR && after != "" && after != "}")
                    printf "%s:%d: add a blank line after the last import\n", path, ends[use]
            }
        }')
    [ -z "$findings" ] && return 0
    echo "pre-commit: $2 breaks the Rust import groups - standard library, external crates, then the own crate," \
        "a blank line apart:"
    printf '%s\n' "$findings" | head -5
    return 1
}

for source in src/*.rs; do
    IFS= read -r header < "$source"
    case "$header" in
        '//!'*) ;;
        *) echo "$source: missing module documentation header" >&2; exit 1 ;;
    esac
done
cargo fmt --all -- --check
imports_status=0
for source in src/*.rs; do
    check_rust_use_groups crudeval "$source" < "$source" || imports_status=1
done
[ "$imports_status" -eq 0 ]
git diff --check
uvx ruff check --target-version py311 --extend-select I scripts/plot.py
uvx ruff format --check scripts/plot.py
if [ "${1:-}" = "--quick" ]; then
    cargo clippy --all-targets -- -D warnings
else
    cargo test --all-features
    cargo clippy --all-targets --all-features -- -D warnings
fi
