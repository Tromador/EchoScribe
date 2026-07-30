#!/usr/bin/env sh

SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit $?
cargo run --release --manifest-path "$SCRIPT_DIRECTORY/Cargo.toml" -- "$@"
exit $?
