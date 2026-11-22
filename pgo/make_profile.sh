#!/bin/bash
# Regenerates pgo/x86.profdata: instrumented x86_64 build, run on a generated dataset.
# Usage: pgo/make_profile.sh DATA_DIR [RUNNER]   (RUNNER e.g. qemu-x86_64 when not on x86)
set -euo pipefail
DATA=$1; RUNNER=${2:-}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TD=${PGO_TARGET_DIR:-/tmp/pgo-target}
rm -rf /tmp/pgo-raw
RUSTFLAGS="-C target-cpu=x86-64-v3 -C linker=rust-lld -C link-self-contained=yes -Cprofile-generate=/tmp/pgo-raw" \
  cargo +${PGO_TOOLCHAIN:-1.85} build --release --locked --target x86_64-unknown-linux-musl --target-dir "$TD" --manifest-path "$ROOT/Cargo.toml"
rm -rf /tmp/pgo-out
DATA_DIR=$DATA OUTPUT_DIR=/tmp/pgo-out NCPU=2 $RUNNER "$TD/x86_64-unknown-linux-musl/release/sessions" 2>/dev/null
rm -rf /tmp/pgo-out
PD=$(ls -d "$(rustc +${PGO_TOOLCHAIN:-1.85} --print sysroot)"/lib/rustlib/*/bin)/llvm-profdata
"$PD" merge -o "$ROOT/pgo/x86.profdata" /tmp/pgo-raw/*.profraw
ls -la "$ROOT/pgo/x86.profdata"
