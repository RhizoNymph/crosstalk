#!/usr/bin/env bash
# Run the Postgres-mode restart and outage tests N times each, one test
# binary per run, and keep every run's full output (each failure prints
# its bounded wait's diagnosis: pending deliveries, blocking sessions,
# takes in flight, the reaper, the gateway's readiness and sessions).
#
#   scripts/loop-pg-tests.sh [N] [OUT_DIR]
#
# N defaults to 10. OUT_DIR defaults to ../runs/<branch>/loop-<timestamp>
# beside the worktree (on the home disk, never /tmp). Needs the test
# database (TEST_DATABASE_URL, or .env.test above the worktree); without
# one every test skips and passes. Prints a summary line per target and
# exits non-zero when any run failed.
set -u

N="${1:-10}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BRANCH="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD | tr '/' '-')"
OUT="${2:-$ROOT/../runs/$BRANCH/loop-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"

# package:target, one test binary each.
TARGETS=(
  "crosstalk-e2e:restart"
  "crosstalk-e2e:spool"
  "crosstalk-e2e:spool_outage"
  "crosstalk-e2e:spool_outage_twice"
  "crosstalk-e2e:spool_outage_serve_runtime"
  "crosstalk-e2e:spool_kill"
  "crosstalk-e2e:postgres"
)

cargo test --manifest-path "$ROOT/Cargo.toml" -p crosstalk-e2e --tests --no-run >"$OUT/build.txt" 2>&1 || {
  echo "build failed; see $OUT/build.txt"
  exit 2
}

failed=0
for target in "${TARGETS[@]}"; do
  package="${target%%:*}"
  test="${target##*:}"
  passes=0
  for i in $(seq 1 "$N"); do
    log="$OUT/$test-$i.txt"
    if cargo test --manifest-path "$ROOT/Cargo.toml" -p "$package" --test "$test" -- --nocapture >"$log" 2>&1; then
      passes=$((passes + 1))
    else
      failed=1
      echo "FAILED: $test run $i ($log)"
    fi
  done
  echo "$test: $passes/$N passed"
done
echo "outputs: $OUT"
exit "$failed"
