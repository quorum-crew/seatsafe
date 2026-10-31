#!/usr/bin/env bash
# Standard benchmark suite. Needs only Docker: Go runs in a golang:1.24
# container and the load generator runs from the cluster's image, on the
# compose network, so its CPU shows up next to the cluster's in docker stats.
#
#   scripts/bench.sh
#
# Runs, each with its own directory under results/<stamp>/:
#   1-ramp   fresh workload, ramp to the max sustainable rate
#   2-fresh  fresh, open loop at 80% of that
#   3-flash  flash, open loop at the same rate
#   4-users  flash, closed loop, 10,000 users
#
# The cluster is wiped before every run (RESET_BETWEEN=1, the default), so each
# run starts from the same state: the flash run collides with its own traffic
# instead of seats the fresh run booked, and no run inherits an earlier one's
# Raft log. Set RESET_BETWEEN=0 to run them all on one cluster.
#
# Knobs: SECTIONS, ROWS, SEATS_PER_ROW (dataset), RATE (skip the ramp and use
# this as the base rate), DURATION, USERS, RAMP_STEP, RAMP_STEP_DURATION,
# RAMP_END, SLO.
set -euo pipefail
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
SECTIONS=${SECTIONS:-1000}
ROWS=${ROWS:-20}
SEATS_PER_ROW=${SEATS_PER_ROW:-50}
DURATION=${DURATION:-60s}
USERS=${USERS:-10000}
RAMP_START=${RAMP_START:-500}
RAMP_STEP=${RAMP_STEP:-500}
RAMP_END=${RAMP_END:-40000}
RAMP_STEP_DURATION=${RAMP_STEP_DURATION:-10s}
SLO=${SLO:-100ms}
RESET_BETWEEN=${RESET_BETWEEN:-1}

here() { pwd -W 2>/dev/null || pwd; }
HOST=$(here)
STAMP=$(date +%Y%m%d-%H%M%S)
OUT=results/$STAMP
mkdir -p "$OUT"
COMMIT=$(git rev-parse HEAD 2>/dev/null || echo unknown)
COMPOSE=(docker compose -f docker-compose.yml -f deploy/compose.bench.yml)
NETWORK=seatsafe_seatsafe
STORAGE=()
for s in 1 2 3; do for n in 1 2 3; do STORAGE+=("shard$s-node$n"); done; done

echo "== dataset: $SECTIONS sections x $ROWS rows x $SEATS_PER_ROW seats"
docker run --rm -v "$HOST:/src" -w /src -e CGO_ENABLED=0 golang:1.24 \
  go run ./cmd/datagen -out deploy -events 0 -flash-sections "$SECTIONS" -flash-rows "$ROWS" -flash-seats-per-row "$SEATS_PER_ROW" | tee "$OUT/dataset.txt"

# seconds_between prints b - a for two docker log timestamps.
seconds_between() { echo $(( $(date -d "$2" +%s%3N) - $(date -d "$1" +%s%3N) )) | awk '{printf "%.1f", $1/1000}'; }

# fresh_cluster wipes everything, starts the cluster and times seeding.
"${COMPOSE[@]}" build >/dev/null
fresh_cluster() {
  local label=$1 t0 t1
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  t0=$(date +%s)
  "${COMPOSE[@]}" up -d --wait --wait-timeout 900 >/dev/null
  t1=$(date +%s)
  {
    echo "cluster up and seeded in $((t1 - t0))s"
    for s in 1 2 3; do
      local logs first last
      logs=$("${COMPOSE[@]}" logs --timestamps --no-log-prefix "shard$s-node1" "shard$s-node2" "shard$s-node3" 2>/dev/null)
      first=$(echo "$logs" | grep 'seed: leader seeding' | awk '{print $1}' | sort | head -1)
      last=$(echo "$logs" | grep 'shard is seeded' | awk '{print $1}' | sort | tail -1)
      if [ -n "$first" ] && [ -n "$last" ]; then
        echo "shard $s: seeded on every replica in $(seconds_between "$first" "$last")s"
      fi
    done
    echo "memory after seeding:"
    docker stats --no-stream --format '  {{.Name}} {{.MemUsage}}' | grep seatsafe- | sort
  } | tee "$OUT/seed-$label.txt"
}

# sample_stats appends docker stats to a file until killed.
sample_stats() {
  while :; do
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null | grep '^seatsafe' >>"$1" || true
  done
}

# peaks prints peak CPU and memory per container from sample_stats output.
peaks() {
  awk -F, '
    function mib(s,  v, u) {
      split(s, p, " "); v = p[1]; u = v; gsub(/[0-9.]/, "", u); gsub(/[^0-9.]/, "", v)
      if (u == "GiB") return v * 1024
      if (u == "KiB" || u == "kB") return v / 1024
      if (u == "B") return v / 1048576
      return v
    }
    { c = $2; gsub(/%/, "", c); m = mib($3)
      if (c + 0 > cpu[$1] + 0) cpu[$1] = c
      if (m + 0 > mem[$1] + 0) mem[$1] = m }
    END { for (n in cpu) printf "| %s | %.0f%% | %.0f MiB |\n", n, cpu[n], mem[n] }
  ' "$1" | sed 's/seatsafe-//; s/-1 |/ |/' | sort
}

# run_load label args...: one load generator run with docker stats alongside.
# A failed audit (exit 2) is reported at the end instead of stopping the suite.
AUDIT_FAILED=0
run_load() {
  local label=$1 dir=$OUT/$1 stats_pid
  shift
  mkdir -p "$dir"
  sample_stats "$dir/docker-stats.csv" &
  stats_pid=$!
  docker run --rm --name seatsafe-loadgen --network "$NETWORK" \
    -v "$HOST/deploy:/deploy:ro" -v "$HOST/$dir:/out" seatsafe:dev \
    loadgen -target router:7000 -shardmap /deploy/shardmap.json -seed-dir /deploy/seed \
    -out /out -label "$label" -git-commit "$COMMIT" "$@" | tee "$dir/console.txt" | grep -v '^ *[0-9]*s sent=' || true
  kill "$stats_pid" 2>/dev/null || true
  wait "$stats_pid" 2>/dev/null || true
  if grep -q 'audit FAIL' "$dir/console.txt"; then
    AUDIT_FAILED=1
    echo "AUDIT FAILED in $label"
  fi
  if [ -f "$dir/history.jsonl" ]; then
    docker run --rm -v "$HOST/$dir:/out:ro" seatsafe:dev checkhistory /out/history.jsonl | tee "$dir/checkhistory.txt" || AUDIT_FAILED=1
  fi
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' | grep '^seatsafe' | sort >"$dir/memory-after.txt"
}

reset() { if [ "$RESET_BETWEEN" = 1 ]; then fresh_cluster "$1"; fi; }

fresh_cluster first

if [ -z "${RATE:-}" ]; then
  echo "== 1-ramp"
  run_load 1-ramp -mode ramp -workload fresh -warmup 5s -rate-start "$RAMP_START" -rate-end "$RAMP_END" \
    -rate-step "$RAMP_STEP" -step-duration "$RAMP_STEP_DURATION" -slo "$SLO"
  MAX=$(sed -n 's/.*"max_sustainable_rate": *\([0-9.]*\).*/\1/p' "$OUT/1-ramp/summary.json" | head -1)
  MAX=${MAX:-0}
  echo "max sustainable rate: $MAX/s"
  RATE=$(awk -v m="$MAX" 'BEGIN { printf "%.0f", m * 0.8 }')
  reset 2
else
  MAX=
fi
if [ "$RATE" = 0 ]; then
  echo "the ramp found no sustainable rate, stopping" >&2
  exit 1
fi

echo "== 2-fresh at $RATE/s"
run_load 2-fresh -mode open -workload fresh -rate "$RATE" -warmup 5s -duration "$DURATION" -history
reset 3
echo "== 3-flash at $RATE/s"
run_load 3-flash -mode open -workload flash -rate "$RATE" -warmup 5s -duration "$DURATION" -history
reset 4
echo "== 4-users, $USERS users"
run_load 4-users -mode closed -workload flash -users "$USERS" -warmup 5s -duration "$DURATION" -history

{
  echo "# Benchmark $STAMP"
  echo
  echo "Commit \`$COMMIT\`, dataset: $(head -1 "$OUT/dataset.txt")."
  echo
  if [ -n "$MAX" ]; then
    echo "Max sustainable rate (p99 under $SLO, under 1% errors): **$MAX/s**. Runs 2 and 3 used 80% of it: $RATE/s."
    echo
  fi
  docker run --rm -v "$HOST/$OUT:/out:ro" seatsafe:dev loadgen -table $(for d in "$OUT"/*/; do echo "/out/$(basename "$d")/summary.json"; done)
  for d in "$OUT"/*/; do
    echo
    echo "## $(basename "$d"): peak per container"
    echo
    echo "| Container | Peak CPU (100% = 1 core) | Peak memory |"
    echo "|---|---|---|"
    peaks "$d/docker-stats.csv"
  done
} | tee "$OUT/table.md"

echo "results in $OUT"
exit "$AUDIT_FAILED"
