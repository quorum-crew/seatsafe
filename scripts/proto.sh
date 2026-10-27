#!/usr/bin/env bash
# Regenerates the Go code in gen/ from proto/.
# protoc runs inside a pinned Docker image, so every machine and CI produce
# byte-identical output. Output goes to a temp dir first, so a failed run
# leaves the existing gen/ untouched.
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=seatsafe-protoc:1
docker build -q -t "$IMAGE" tools/protoc >/dev/null

tmp=$(mktemp -d ./.proto-out.XXXXXX)
trap 'rm -rf "$tmp"' EXIT

docker run --rm -u "$(id -u):$(id -g)" -v "$PWD":/src -w /src "$IMAGE" \
  protoc -I proto \
    --go_out="$tmp" --go_opt=module=github.com/quorum-crew/seatsafe \
    --go-grpc_out="$tmp" --go-grpc_opt=module=github.com/quorum-crew/seatsafe \
    proto/seatsafe/v1/*.proto

rm -rf gen
mv "$tmp/gen" gen

echo "Generated:"
ls gen/seatsafe/v1
