#!/usr/bin/env bash
# Unattended chaos campaign with the linearizability check. Needs only
# Docker: without a local Go, the chaos tool is built in a golang:1.24
# container for this machine.
#
#   scripts/campaign.sh                 # 20 runs at 300 orders/s
#   RUNS=40 RATE=200 scripts/campaign.sh -seed 7 -duration 45s
#
# The runs go through the mix in turn: kill-leader, pause-leader, partition
# and a no-fault control, the faults alternating between a shard and the
# coordinator (20 runs = 5 of each fault). A fifth of the orders are
# cross-shard and a few browses per second are recorded, so the Porcupine
# check after every run sees 2PC and reads as well as bookings.
#
# The cluster is wiped every 5 runs, after any run that did not pass, and
# when less than 3 GiB of memory is free before a run; otherwise the next run
# waits 9 s (-hold-drain) so holds left by cross-shard orders are resolved
# before its baseline. A run that breaks (the
# cluster does not come up, the load generator dies) is recorded as such and
# the campaign goes on. results/chaos-<stamp>/summary.md has one row per run
# (fault, target, failover, audit rules a to e, the bookings verdict
# PASS/FAIL/TIMEOUT, stale browses, check time, UNKNOWN orders, elections)
# and the totals. A run passes or fails on the bookings verdict (orders,
# baseline and final read); stale browses are reported only. The summary
# is rewritten after every run, so it is complete up to the last finished
# run even if the campaign is stopped.
#
# Knobs: RUNS, RATE, COMPOSE_PROJECT. Further arguments go to cmd/chaos
# (go run ./cmd/chaos -h lists them). Generate a dataset first:
# go run ./cmd/datagen -out deploy (scripts/bench.sh does it too).
#
# Exit code: 0 if every run passed, 2 if a run failed, timed out in the
# check, or broke.
set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
RUNS=${RUNS:-20}
RATE=${RATE:-300}
PROJECT=${COMPOSE_PROJECT:-seatsafe}

if ! ls deploy/seed/shard*.json >/dev/null 2>&1 || [ ! -f deploy/shardmap.json ]; then
  echo "no dataset in deploy/: run 'go run ./cmd/datagen -out deploy' first" >&2
  exit 1
fi

here() { pwd -W 2>/dev/null || pwd; }
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) GOOS=windows EXE=.exe ;;
  Darwin) GOOS=darwin EXE= ;;
  *) GOOS=linux EXE= ;;
esac
case "$(uname -m)" in
  arm64 | aarch64) GOARCH=arm64 ;;
  *) GOARCH=amd64 ;;
esac

echo "== image"
docker compose -p "$PROJECT" build

echo "== chaos tool ($GOOS/$GOARCH)"
mkdir -p bin
if command -v go >/dev/null 2>&1; then
  go build -o "bin/chaos$EXE" ./cmd/chaos
else
  docker run --rm -v "$(here):/src" -v seatsafe-gomod:/go/pkg/mod -v seatsafe-gobuild:/root/.cache/go-build -w /src \
    -e CGO_ENABLED=0 -e GOOS="$GOOS" -e GOARCH="$GOARCH" golang:1.24 go build -o "bin/chaos$EXE" ./cmd/chaos
fi

echo "== campaign: $RUNS runs at $RATE orders/s"
exec "bin/chaos$EXE" -campaign -runs "$RUNS" -rate "$RATE" -project "$PROJECT" "$@"
