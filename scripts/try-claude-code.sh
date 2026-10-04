#!/usr/bin/env bash
# Builds the crosstalk binary, starts it on localhost with a config derived
# from crates/gateway/config.example.json, and prints the ANTHROPIC_BASE_URL
# to export so a real Claude Code session runs through it. Ctrl-C stops the
# gateway gracefully (in-flight streams finish, the exchange log is synced).
#
#   scripts/try-claude-code.sh
#   CROSSTALK_PROXY_PORT=18080 CROSSTALK_OPS_PORT=19464 scripts/try-claude-code.sh
#
# Everything the run writes is under the run directory
# (CROSSTALK_RUN_DIR, default target/crosstalk-dev, which git ignores):
#   config.json                       the derived config
#   .env                              the generated deployment secret (mode 600)
#   blobs/                            message bodies, content-addressed
#   exchanges/exchange-log.jsonl      one ExchangeCaptured envelope per line
#
# The derived config drops the example's `store` section: capture does not
# need Postgres yet, and without it /readyz does not wait for a database.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_dir="${CROSSTALK_RUN_DIR:-$root/target/crosstalk-dev}"
proxy_port="${CROSSTALK_PROXY_PORT:-8080}"
ops_port="${CROSSTALK_OPS_PORT:-9464}"
target_dir="${CARGO_TARGET_DIR:-$root/target}"
binary="$target_dir/debug/crosstalk"

mkdir -p "$run_dir"

echo "==> building crosstalk"
(cd "$root" && cargo build -p crosstalk-gateway --bin crosstalk)

# The deployment secret keys the credential digests. Reuse the run's own
# .env so digests stay stable across runs; generate one the first time.
if [[ -z "${CROSSTALK_SECRET_V1:-}" ]]; then
    if [[ ! -f "$run_dir/.env" ]]; then
        echo "==> generating a deployment secret in $run_dir/.env"
        secret="$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"
        (umask 077 && printf 'CROSSTALK_SECRET_V1=%s\n' "$secret" > "$run_dir/.env")
    fi
    set -a
    # shellcheck disable=SC1091
    . "$run_dir/.env"
    set +a
fi

echo "==> writing $run_dir/config.json"
python3 - "$root/crates/gateway/config.example.json" "$run_dir/config.json" "$proxy_port" "$ops_port" <<'EOF'
import json
import sys

example, out, proxy_port, ops_port = sys.argv[1:]
with open(example) as source:
    config = json.load(source)
config.pop("store", None)
config["ingress"]["listen"] = f"127.0.0.1:{proxy_port}"
config["ops"]["listen"] = f"127.0.0.1:{ops_port}"
config["blobs"]["root"] = "blobs"
with open(out, "w") as target:
    json.dump(config, target, indent=2)
    target.write("\n")
EOF

cat <<EOF

crosstalk is starting on 127.0.0.1:${proxy_port} (ops on 127.0.0.1:${ops_port}).

In another terminal, point Claude Code at it and use it as usual:

    export ANTHROPIC_BASE_URL=http://127.0.0.1:${proxy_port}/anthropic
    claude

Requests go on to https://api.anthropic.com with your own credentials,
unchanged; crosstalk keeps only keyed hashes of them.

Watch it work:

    curl -s http://127.0.0.1:${ops_port}/healthz
    curl -s http://127.0.0.1:${ops_port}/metrics
    $binary inspect --config $run_dir/config.json
    $binary inspect --config $run_dir/config.json <exchange-id>

Logs are JSON lines on stdout (RUST_LOG=debug for more). Ctrl-C stops it.

EOF

exec "$binary" serve --role all --config "$run_dir/config.json"
