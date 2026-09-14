#!/usr/bin/env bash
# Usage: bash scripts/check_openapi.sh BASE.json CURRENT.json REPORT_DIR
# Requires Python 3 and pinned oasdiff on PATH. After report setup, exits 0
# (pass), 1 (breaking), or 2 (invalid input/tool error). Setup failures also
# exit nonzero. REPORT_DIR must be outside the PR checkout.
set -euo pipefail

if [[ "$#" != 3 ]]; then
    printf 'Usage: %s BASE.json CURRENT.json REPORT_DIR\n' "$0" >&2
    exit 2
fi

base="$(realpath -m -- "$1")"
current="$(realpath -m -- "$2")"
mkdir -p -- "$3"
# Do not auto-load a PR's .oasdiff.* (or legacy oasdiff.*) configuration.
cd -- "$3"

: > breaking-changes.txt
printf 'Comparison not run: generated input validation failed.\n' > api-diff.md
breaking_status='not run'
diff_status='not run'
result=ERROR
status=2

# This is only generated-document sanity, not a new OpenAPI lint policy.
# oasdiff's lenient loader otherwise accepts {} as an empty specification.
if python3 - "$base" "$current" > input-errors.txt 2>&1 <<'PY'
import json
import re
import sys

for label, filename in zip(("base", "current"), sys.argv[1:]):
    try:
        with open(filename, encoding="utf-8") as source:
            spec = json.load(source)
        if not isinstance(spec, dict):
            raise ValueError("expected an OpenAPI document object")
        version = spec.get("openapi")
        if not isinstance(version, str) or not re.fullmatch(r"3\.\d+\.\d+", version):
            raise ValueError("expected an OpenAPI 3.x version")
        info = spec.get("info")
        if not isinstance(info, dict) or not all(
            isinstance(info.get(key), str) and info[key].strip()
            for key in ("title", "version")
        ):
            raise ValueError("expected an info object with title and version strings")
        if not isinstance(spec.get("paths"), dict):
            raise ValueError("expected a paths object")
    except (OSError, ValueError) as error:
        print(f"Invalid {label} generated specification: {error}", file=sys.stderr)
        sys.exit(1)
PY
then
    breaking_status=0
    oasdiff breaking "$base" "$current" --fail-on ERR --allow-external-refs=false \
        > breaking-changes.txt 2>&1 || breaking_status=$?
    diff_status=0
    oasdiff diff "$base" "$current" --format markdown --allow-external-refs=false \
        > api-diff.md 2>&1 || diff_status=$?

    # 1 is a definite break only for `breaking --fail-on ERR`. Any detailed
    # diff failure (including 1), or other breaking failure, is a tool error.
    if [[ "$diff_status" == 0 ]]; then
        case "$breaking_status" in
            0) result=PASS; status=0 ;;
            1) result=BREAKING; status=1 ;;
        esac
    fi
fi

{
    printf '# OpenAPI specification comparison\n\nStatus: %s\n\n' "$result"
    printf 'Threshold: **ERR**. WARN-only changes are reported but do not block.\n\n'
    case "$result" in
        PASS) printf 'No ERR-level breaking changes detected.\n\n' ;;
        BREAKING) printf 'Breaking API changes detected; review the compatibility impact.\n\n' ;;
        ERROR) printf 'Comparison failed: invalid generated input or tool error. Do not treat this as compatible.\n\n' ;;
    esac
    printf 'breaking exit code: %s; diff exit code: %s\n\n' "$breaking_status" "$diff_status"
    printf 'Download the OpenAPI artifact for generated specs, diagnostics, and api-diff.md.\n'
    if [[ -s input-errors.txt || -s breaking-changes.txt ]]; then
        printf '\n## Breaking-check diagnostics\n\n```text\n'
        cat input-errors.txt breaking-changes.txt
        printf '\n```\n'
    fi
} > summary.md

exit "$status"
