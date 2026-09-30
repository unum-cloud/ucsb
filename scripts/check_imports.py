#!/usr/bin/env python3
"""Check Rust import groups after rustfmt, including the project's named crate."""

import re
import sys
import tomllib
from pathlib import Path

USE = re.compile(r"(?m)^( *)(?:pub(?:\([^)]*\))? +)?use +(?:::)?(\w+)\b")
LITERAL = re.compile(
    r"""r(\#*)".*?"\1|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\\n])'|//[^\n]*|/\*""", re.DOTALL
)


def mask_literals(source):
    parts, offset = [], 0
    while match := LITERAL.search(source, offset):
        end = match.end()
        if match.group() == "/*":
            depth = 1
            while depth:
                token = re.search(r"/\*|\*/", source[end:])
                if token is None:
                    raise ValueError("unterminated block comment")
                depth += 1 if token.group() == "/*" else -1
                end += token.end()
        parts.extend(
            (
                source[offset : match.start()],
                re.sub(r"[^\n]", " ", source[match.start() : end]),
            )
        )
        offset = end
    return "".join(parts) + source[offset:]


def imports(source):
    masked = mask_literals(source)
    lines = masked.splitlines(keepends=True)
    for match in USE.finditer(masked):
        start = masked.count("\n", 0, match.start())
        end = masked.find(";", match.end())
        if end < 0:
            raise ValueError("unterminated use declaration")
        end = masked.count("\n", 0, end) + 1
        while start and lines[start - 1].rstrip().endswith("]"):
            candidate, depth = start, 0
            while candidate:
                candidate -= 1
                depth += lines[candidate].count("]") - lines[candidate].count("[")
                if depth == 0:
                    break
            if not lines[candidate].lstrip().startswith("#["):
                break
            start = candidate
        yield start, end, len(match[1]), match[2]


def group(root, own_crate):
    if root in {"std", "core", "alloc"}:
        return 0
    return 2 if root in {own_crate, "crate", "self", "super"} else 1


def check(path, own_crate):
    source = path.read_text()
    lines = source.splitlines(keepends=True)
    declarations = list(imports(source))
    errors = []
    for index, (start, end, indent, root) in enumerate(declarations):
        category = group(root, own_crate)
        following = declarations[index + 1] if index + 1 < len(declarations) else None
        if (
            following
            and following[2] == indent
            and not "".join(lines[end : following[0]]).strip()
        ):
            next_group = group(following[3], own_crate)
            if next_group < category:
                errors.append(
                    (
                        start + 1,
                        "order imports as standard library, external crates, own crate",
                    )
                )
            if next_group != category and end == following[0]:
                errors.append((end, "separate import groups with a blank line"))
        elif end < len(lines) and lines[end].strip() and lines[end].strip() != "}":
            errors.append((end, "add a blank line after the last import"))
    for line, message in errors:
        print(f"{path}:{line}: {message}", file=sys.stderr)
    return bool(errors)


def main():
    manifest = tomllib.loads(Path("Cargo.toml").read_text())
    own_crate = manifest.get("lib", {}).get(
        "name", manifest["package"]["name"].replace("-", "_")
    )
    paths = sorted(Path("src").rglob("*.rs")) + sorted(Path("scripts").glob("*.rs"))
    return int(sum(check(path, own_crate) for path in paths) > 0)


if __name__ == "__main__":
    sys.exit(main())
