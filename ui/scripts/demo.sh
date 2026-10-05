#!/usr/bin/env bash
# Builds the elements and the UI, bundles the assets and serves the UI over
# the fixture world on the address in config.json (http://127.0.0.1:3000).
#
#   CARGO=...                cargo to use (default: cargo on PATH)
#   CROSSTALK_UI_CONFIG=...  another config file (default: config.json)
set -euo pipefail
cd "$(dirname "$0")/.."

CARGO="${CARGO:-cargo}"
# The Topcoat CLI runs `cargo` itself; make it the same one.
case "$CARGO" in
  */*) export PATH="$(dirname "$CARGO"):$PATH" ;;
esac
TOPCOAT="target/tools/bin/topcoat"
if [ ! -x "$TOPCOAT" ]; then
  "$CARGO" install topcoat-cli --version =0.9.0 --locked --root target/tools
fi

(cd elements && pnpm install --frozen-lockfile && pnpm build)
"$CARGO" build
"$TOPCOAT" asset bundle
exec ./target/debug/crosstalk-ui
