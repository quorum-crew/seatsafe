#!/usr/bin/env bash
# Regenerates the Go code in gen/ from proto/.
# protoc runs inside a pinned Docker image, so every machine and CI produce
# byte-identical output.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=seatsafe-protoc:1
docker build -q -t "$IMAGE" tools/protoc >/dev/null

rm -rf gen
docker run --rm -u "$(id -u):$(id -g)" -v "$PWD":/src -w /src "$IMAGE" \
  protoc -I proto \
    --go_out=. --go_opt=module=github.com/quorum-crew/seatsafe \
    --go-grpc_out=. --go-grpc_opt=module=github.com/quorum-crew/seatsafe \
    proto/seatsafe/v1/*.proto

echo "Generated:"
ls gen/seatsafe/v1
