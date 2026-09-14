#!/usr/bin/env bash
# Run with oasdiff 1.32.0 on PATH; all fixtures and reports stay in /tmp.
set -euo pipefail

helper="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/check_openapi.sh"
real_oasdiff="$(command -v oasdiff)"
export REAL_OASDIFF="$real_oasdiff"
work="$(mktemp -d /tmp/check-openapi-test.XXXXXX)"
trap 'rm -rf -- "$work"' EXIT

python3 - "$work" <<'PY'
import copy
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
base = {"openapi": "3.0.3", "info": {"title": "Test", "version": "1.0.0"},
        "paths": {"/pets": {"get": {"responses": {"200": {"description": "OK"}}}}}}

def save(name, spec):
    (root / (name + ".json")).write_text(json.dumps(spec))

save("base", base)
additive = copy.deepcopy(base)
additive["paths"]["/new"] = copy.deepcopy(base["paths"]["/pets"])
save("additive", additive)
removed = copy.deepcopy(base)
removed["paths"] = {}
save("removed", removed)
# Both allOf branches require id: dropping one branch's requirement is WARN,
# because the other still guarantees it (not a definite ERR-level break).
warning = copy.deepcopy(base)
warning["components"] = {"schemas": {name: {
    "type": "object", "required": ["id"], "properties": {"id": {"type": "string"}}
} for name in ["A", "B"]}}
warning["paths"]["/pets"]["get"]["responses"]["200"]["content"] = {
    "application/json": {"schema": {"allOf": [
        {"$ref": "#/components/schemas/A"}, {"$ref": "#/components/schemas/B"}
    ]}}}
save("warning-base", warning)
del warning["components"]["schemas"]["A"]["required"]
save("warning-current", warning)
save("empty-object", {})
save("non-object", [])
for name, key, value in [("wrong-version", "openapi", "2.0"),
                         ("invalid-info", "info", {}),
                         ("invalid-paths", "paths", [])]:
    invalid = copy.deepcopy(base)
    invalid[key] = value
    save(name, invalid)
(root / "empty-file.json").write_text("")
(root / "malformed.json").write_text("{not json")
# A valid external target makes removing the protection a false pass, not an
# unrelated missing-ref error. Both tool invocations must refuse to load it.
(root / "external-path.json").write_text(json.dumps(base["paths"]["/pets"]))
external = copy.deepcopy(base)
external["paths"]["/pets"] = {"$ref": str(root / "external-path.json")}
save("external", external)
PY

run_case() {
    local name="$1" expected="$2" result="$3" base="$4" current="$5" status=0
    local report="$work/reports/$name"
    bash "$helper" "$base" "$current" "$report" || status=$?
    if [[ "$status" != "$expected" ]]; then
        printf 'FAIL %s: expected exit %s, got %s\n' "$name" "$expected" "$status" >&2
        exit 1
    fi
    grep -Fq "Status: $result" "$report/summary.md"
    grep -Fq 'ERR' "$report/summary.md"
    test -f "$report/breaking-changes.txt"
    test -f "$report/api-diff.md"
    if [[ "$expected" != 0 ]]; then
        if grep -Fq 'Status: PASS' "$report/summary.md"; then
            printf 'FAIL %s: error reported as success\n' "$name" >&2
            exit 1
        fi
    fi
    printf 'PASS %s (exit %s, %s)\n' "$name" "$status" "$result"
}

run_case identical 0 PASS "$work/base.json" "$work/base.json"
run_case additive 0 PASS "$work/base.json" "$work/additive.json"
grep -Fq '/new' "$work/reports/additive/api-diff.md"
run_case removed-endpoint 1 BREAKING "$work/base.json" "$work/removed.json"
grep -Fq 'api-path-removed-without-deprecation' "$work/reports/removed-endpoint/breaking-changes.txt"
run_case warning-only 0 PASS "$work/warning-base.json" "$work/warning-current.json"
grep -Fq 'response-property-became-optional' "$work/reports/warning-only/summary.md"

for invalid in malformed missing empty-object empty-file non-object wrong-version invalid-info invalid-paths; do
    run_case "invalid-base-$invalid" 2 ERROR "$work/$invalid.json" "$work/base.json"
    run_case "invalid-current-$invalid" 2 ERROR "$work/base.json" "$work/$invalid.json"
done
run_case both-missing 2 ERROR "$work/missing-base.json" "$work/missing-current.json"
run_case external-ref 2 ERROR "$work/base.json" "$work/external.json"
grep -Fq 'breaking exit code: 102' "$work/reports/external-ref/summary.md"
grep -Fq 'diff exit code: 102' "$work/reports/external-ref/summary.md"

# Starting in a PR checkout must not auto-load that checkout's oasdiff config.
mkdir -p "$work/untrusted checkout"
printf 'invalid: [\n' > "$work/untrusted checkout/.oasdiff.yaml"
(
    cd "$work/untrusted checkout"
    run_case ignores-checkout-config 0 PASS "$work/base.json" "$work/base.json"
)

# Keep the real CLI for the command not under fault injection.
mkdir -p "$work/bin"
cat > "$work/bin/oasdiff" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
case "${FAULT:-}:$1" in
    breaking-failure:breaking | diff-failure:diff)
        printf 'Injected tool failure\n' >&2
        exit 42
        ;;
    bad-flag:breaking)
        exec "$REAL_OASDIFF" "$@" --not-a-real-flag
        ;;
esac
exec "$REAL_OASDIFF" "$@"
SH
chmod +x "$work/bin/oasdiff"
export PATH="$work/bin:$PATH"
FAULT=breaking-failure run_case tool-failure 2 ERROR "$work/base.json" "$work/base.json"
grep -Fq 'breaking exit code: 42' "$work/reports/tool-failure/summary.md"
FAULT=bad-flag run_case bad-flag 2 ERROR "$work/base.json" "$work/base.json"
grep -Fq 'breaking exit code: 100' "$work/reports/bad-flag/summary.md"
FAULT=diff-failure run_case diff-failure 2 ERROR "$work/base.json" "$work/base.json"
grep -Fq 'diff exit code: 42' "$work/reports/diff-failure/summary.md"
FAULT=diff-failure run_case breaking-and-diff-failure 2 ERROR "$work/base.json" "$work/removed.json"
grep -Fq 'breaking exit code: 1' "$work/reports/breaking-and-diff-failure/summary.md"
grep -Fq 'diff exit code: 42' "$work/reports/breaking-and-diff-failure/summary.md"

printf 'All OpenAPI comparison regression tests passed.\n'
