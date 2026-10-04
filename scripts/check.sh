#!/usr/bin/env bash
# Runs every workspace check in order and stops at the first failure,
# naming the step that failed. CARGO_TARGET_DIR is respected.
#
# The toolchain is rust-toolchain.toml's channel, selected through
# RUSTUP_TOOLCHAIN (unless the caller sets it), so the checks need only that
# channel's rustfmt and clippy, not every component the file lists (miri).
#
#   scripts/check.sh
#   CARGO_TARGET_DIR=/tmp/target scripts/check.sh
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root" || exit 1

if [[ -z "${RUSTUP_TOOLCHAIN:-}" ]]; then
    channel="$(sed -n 's/^channel *= *"\(.*\)"/\1/p' rust-toolchain.toml)"
    if [[ -z "$channel" ]]; then
        echo "check failed at step: toolchain (no channel in rust-toolchain.toml)" >&2
        exit 1
    fi
    export RUSTUP_TOOLCHAIN="$channel"
fi

step() {
    local name="$1"
    shift
    echo "==> ${name}"
    if ! "$@"; then
        echo "check failed at step: ${name}" >&2
        exit 1
    fi
}

step "fmt" cargo fmt --all --check
step "clippy" cargo clippy --workspace --all-targets -- -D warnings
step "test" cargo test --workspace
step "doc" env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
step "invariants" python3 scripts/inv_check.py spec/invariants

echo "all checks passed"
