#!/usr/bin/env python3
"""Fix or explain `cargo vet --locked` failures caused by missing exemptions.

## Why this exists

Every estate repo gates on `cargo vet --locked`. Coverage comes from three
places, and which one applies varies by repo:

- `supply-chain/imports.lock` — crates vetted to a criterion. Repos with a
  populated file (e.g. `formula-lang`: 32 fully audited) legitimately need
  **no** exemption for those crates.
- `supply-chain/audits.toml` — crates covered by an audit record.
- `supply-chain/config.toml` `[[exemptions.NAME]]` — everything else.

So a package needs an exemption only when neither of the first two apply.
That decision belongs to cargo-vet, and reimplementing it here was wrong:
an earlier version of this script derived the required set from
`Cargo.lock` alone and cheerfully demanded 34 exemptions from
`formula-lang`, which passes `cargo vet --locked` with 32 crates fully
audited and none of them exempted. Wiring that version into the gate would
have turned nineteen green repos red.

This version therefore never decides coverage itself. It runs
`cargo vet --locked` and uses cargo-vet's own list of unmet packages as
the ground truth.

## Usage

```sh
# The fix: run after Cargo.lock is final. Appends exemptions for exactly the
# packages cargo-vet reports as missing, then canonicalises the formatting.
python3 scripts/resync-vet-exemptions.py --write

# The gate: runs cargo vet and, on failure, prints the missing packages and
# the fix. Exits with cargo-vet's own status.
python3 scripts/resync-vet-exemptions.py --check
```

## Why the write is additive

`--write` only ever **appends** `[[exemptions.NAME]]` blocks. It never
deletes or rewrites existing entries, never reorders them, and never
removes a version cargo-vet is happy with. That matters because:

- a repo with real `imports.lock` audits keeps them untouched;
- an entry a maintainer wrote by hand for a good reason survives;
- the diff shows exactly which new packages the current lockfile
  introduced, which is what a reviewer wants to see.

The cost is that genuinely-stale entries (a version no longer in the lock)
are left behind. `cargo vet` does not complain about those, so they are
harmless; `cargo vet fmt` keeps the file tidy. Prune them by hand if the
list ever grows unwieldy.

## Ordering

Regenerate **after** `Cargo.lock` is final. Writing exemptions and then
regenerating the lockfile leaves them stale again and produces a second
red run — an ordering mistake that has bitten this estate more than once.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys

DEFAULT_CONFIG = os.path.join("supply-chain", "config.toml")

# cargo-vet reports unmet packages as e.g.
#     sheet-engine:0.1.0 missing ["safe-to-deploy"]
_MISSING_RE = re.compile(r'^\s*([A-Za-z0-9_.+-]+):([^\s]+) missing \["([^"]+)"\]', re.M)


def run_cargo_vet() -> tuple[int, str]:
    """Run `cargo vet --locked`; return (exit status, combined output)."""
    if shutil.which("cargo") is None:
        sys.exit("error: cargo not on PATH")
    proc = subprocess.run(
        ["cargo", "vet", "--locked"],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout + proc.stderr


def semver_key(version: str) -> tuple:
    """Sort key that orders versions numerically, the way cargo-vet does.

    A plain string sort puts "0.10.2" before "0.9.6", so a crate present at two
    versions is appended in the wrong order and `cargo vet fmt` then rewrites the
    file -- which makes `--write` output unstable and every subsequent run look
    like a change. Parse the numeric components and fall back to the raw string
    for anything unparseable, so a pre-release or build suffix still sorts
    deterministically.
    """
    core = version.split("+", 1)[0]
    pre = ""
    if "-" in core:
        core, pre = core.split("-", 1)
    parts: list[int] = []
    for segment in core.split("."):
        try:
            parts.append(int(segment))
        except ValueError:
            return (1, (), version)
    # Pad so 1.2 and 1.2.0 compare equal, and any pre-release sorts after the
    # plain release of the same version.
    while len(parts) < 3:
        parts.append(0)
    return (0, tuple(parts), "~" + pre if pre else "")


def parse_missing(output: str) -> list[tuple[str, str, str]]:
    """Unmet `(name, version, criteria)` triples from cargo-vet's output."""
    seen: set[tuple[str, str, str]] = set()
    out: list[tuple[str, str, str]] = []
    for name, version, criteria in _MISSING_RE.findall(output):
        key = (name, version, criteria)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return sorted(out, key=lambda t: (t[0], semver_key(t[1]), t[2]))


def existing_exemptions(config_path: str) -> set[tuple[str, str, str]]:
    """`(name, version, criteria)` triples already present in the config."""
    with open(config_path, encoding="utf-8") as fh:
        text = fh.read()
    found: set[tuple[str, str, str]] = set()
    for block in text.split("[[exemptions.")[1:]:
        name = block.split("]]")[0]
        version = re.search(r'version = "(.+)"', block)
        criteria = re.search(r'criteria = "(.+)"', block)
        if version and criteria:
            found.add((name, version.group(1), criteria.group(1)))
    return found


def append_exemptions(config_path: str, missing: list[tuple[str, str, str]]) -> int:
    """Append blocks for `missing` that are not already present. Returns count added."""
    have = existing_exemptions(config_path)
    additions = [m for m in missing if m not in have]
    if not additions:
        return 0
    with open(config_path, "a", encoding="utf-8") as fh:
        if not fh.tell() == 0 and not _ends_with_newline(config_path):
            fh.write("\n")
        for name, version, criteria in additions:
            fh.write(f'\n[[exemptions.{name}]]\nversion = "{version}"\ncriteria = "{criteria}"\n')
    return len(additions)


def _ends_with_newline(path: str) -> bool:
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        if fh.tell() == 0:
            return True
        fh.seek(-1, os.SEEK_END)
        return fh.read(1) == b"\n"


def canonicalise_store() -> bool:
    """Run `cargo vet fmt`. Returns True when it succeeded.

    cargo-vet refuses to *report* coverage while the store is unformatted --
    an out-of-order `[[exemptions.NAME]]` block for a crate present at two
    versions makes it emit a store-consistency error instead of the missing
    package list, so `--write` has nothing to act on. Formatting first is what
    makes the tool single-command; doing it inside the script removes the
    ordering footgun rather than documenting it.
    """
    if shutil.which("cargo") is None:
        return False
    return (
        subprocess.run(
            ["cargo", "vet", "fmt"], capture_output=True, text=True, check=False
        ).returncode
        == 0
    )


def report(missing: list[tuple[str, str, str]], output: str) -> None:
    print("error: `cargo vet --locked` failed; the following packages need coverage:", file=sys.stderr)
    for name, version, criteria in missing:
        print(f"    {name} {version}  ->  criteria {criteria!r}", file=sys.stderr)
    if not missing:
        # A failure that is not a missing exemption (bad imports.lock, a real
        # audit failure, a parse error). Do not pretend it is fixable here.
        print(file=sys.stderr)
        print("    This is not a missing-exemption failure. Full cargo-vet output:", file=sys.stderr)
        print(file=sys.stderr)
        for line in output.strip().splitlines():
            print(f"    {line}", file=sys.stderr)
        print(file=sys.stderr)
        print("    See docs/cargo-vet.md.", file=sys.stderr)
        return
    print(file=sys.stderr)
    print("    Fix (Cargo.lock must already be final — regenerate in this order):", file=sys.stderr)
    print("        python3 scripts/resync-vet-exemptions.py --write", file=sys.stderr)
    print("        cargo vet fmt", file=sys.stderr)
    print("        cargo vet --locked", file=sys.stderr)
    print(file=sys.stderr)
    print("    A repo with a populated supply-chain/imports.lock needs no exemption", file=sys.stderr)
    print("    for an audited crate; if one of these should be vetted rather than", file=sys.stderr)
    print("    exempted, add it to imports.lock instead (`cargo vet --import-keys`).", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="path to supply-chain/config.toml")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="run cargo vet; explain failures (the gate)")
    mode.add_argument("--write", action="store_true", help="append exemptions for what cargo vet reports missing")
    args = ap.parse_args()

    status, output = run_cargo_vet()
    missing = parse_missing(output)

    if args.check:
        if status == 0:
            summary = next(
                (ln.strip() for ln in output.splitlines() if "Vetting Succeeded" in ln), ""
            )
            print(summary or "cargo vet --locked succeeded")
            return 0
        report(missing, output)
        return status or 1

    # --write
    if status == 0:
        print("cargo vet --locked already succeeds — nothing to add")
        return 0
    if not missing:
        # An unformatted store hides the coverage list. Canonicalise and retry
        # once; only report a dead end if cargo-vet still cannot enumerate.
        if "consistency errors" in output or "not correctly formatted" in output:
            if canonicalise_store():
                status, output = run_cargo_vet()
                missing = parse_missing(output)
                if status == 0:
                    print("cargo vet fmt was all that was needed; the store is now clean")
                    return 0
    if not missing:
        report([], output)
        return status or 1
    if not os.path.exists(args.config):
        sys.exit(
            f"error: {args.config} not found. cargo-vet wants coverage for "
            f"{len(missing)} package(s) but this repo has no exemption file; add them "
            "to supply-chain/imports.lock instead (`cargo vet --import-keys`)."
        )

    added = append_exemptions(args.config, missing)
    if added:
        print(f"appended {added} exemption block(s) to {args.config}")
        # Canonicalise so the next run is a no-op and the diff shows only the
        # version bumps rather than a reshuffled file.
        if canonicalise_store():
            final, _ = run_cargo_vet()
            if final == 0:
                summary = next(
                    (ln.strip() for ln in _ if "Vetting Succeeded" in ln), ""
                )
                print(summary or "cargo vet --locked now succeeds")
                return 0
        print("run `cargo vet fmt`, then `cargo vet --locked`")
    else:
        print("every reported package already has an exemption block")
        print("the failure is elsewhere — re-running cargo vet for detail:")
        print()
        print(output.strip())
    return 0


if __name__ == "__main__":
    sys.exit(main())
