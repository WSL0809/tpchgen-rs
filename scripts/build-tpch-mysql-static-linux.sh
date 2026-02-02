#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-tpchgen-rs-musl:1.89.0}"
TARGET="${TARGET:-x86_64-unknown-linux-musl}"

cd "$ROOT"

docker build -f docker/tpch-mysql-static/Dockerfile -t "$IMAGE" .

docker run --rm \
  -v "$ROOT":/work \
  -w /work \
  "$IMAGE" \
  cargo build -p tpchgen-mysql-cli --release --locked --target "$TARGET"

echo "Built: $ROOT/target/$TARGET/release/tpch-mysql"
