#!/usr/bin/env bash
# Run the overload scenario against N server replicas and report what the fleet's controllers did.
#
#   scripts/fleet.sh <replicas> [out-dir]
#
# Each server runs its own QueueMonitor and RateLimiter against the shared queue. This is not a
# throughput comparison: what it produces is count-shaped, admitted events per second against a
# floor the operator set. The counters are read from each replica's own scrape rather than from
# Prometheus, which scrapes every 5 s, so the scrape interval stays out of a delta over a window.
set -euo pipefail

REPLICAS="${1:-2}"
OUT_DIR="${2:-benchmarks/out/fleet-$REPLICAS}"

SCENARIO="${SCENARIO:-benchmarks/scenarios/fleet-overload.yaml}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:8085}"

SETTLE_SECONDS="${SETTLE_SECONDS:-10}"

COMPOSE_FLEET=(docker compose -f docker-compose.yml -f docker-compose.scale.yml)

# The arguments are needed: `source` leaves the caller's positional parameters visible, so ab.sh
# would otherwise read the replica count as its axis name.
AB_SOURCE_ONLY=1 . scripts/ab.sh writer 0

[[ "$REPLICAS" =~ ^[1-9][0-9]*$ ]] || fail "replicas must be a positive integer, got '$REPLICAS'"
command -v java >/dev/null || fail "java not found"
[[ -f Client/SkierClient/target/skier-client.jar ]] \
  || fail "client jar missing; run: mvn -q -pl Client/SkierClient -am package -DskipTests"
[[ -f "$SCENARIO" ]] || fail "scenario not found: $SCENARIO"

mkdir -p "$OUT_DIR"


info "starting $REPLICAS server replica(s)..."
"${COMPOSE_FLEET[@]}" up -d --build --scale "server=$REPLICAS" >/dev/null 2>&1 \
  || fail "compose up --scale server=$REPLICAS failed"

# Host ports are ephemeral by design, so they are read back rather than assumed.
SERVER_URLS=()
for i in $(seq 1 "$REPLICAS"); do
  mapping=$("${COMPOSE_FLEET[@]}" port --index "$i" server 8080 2>/dev/null || true)
  port="${mapping##*:}"
  [[ -n "$port" && "$port" != "$mapping" ]] || fail "could not read the host port for replica $i"
  SERVER_URLS+=("http://localhost:$port")
done
info "  replicas: ${SERVER_URLS[*]}"

wait_ready() {
  local url="$1" _
  for _ in $(seq 1 90); do
    curl -fsS "$url/actuator/health/readiness" >/dev/null 2>&1 && return 0
    sleep 2
  done
  fail "replica at $url never became ready"
}
for url in "${SERVER_URLS[@]}"; do wait_ready "$url"; done


reset_datastores
"${COMPOSE_FLEET[@]}" up -d consumer >/dev/null 2>&1 || fail "could not restart the consumer"
for _ in $(seq 1 60); do
  curl -fsS "$CONSUMER_URL/actuator/health/readiness" >/dev/null 2>&1 && break
  sleep 2
done
assert_clean_state "fleet-$REPLICAS"

info "  recreating replicas so the counters start from zero..."
"${COMPOSE_FLEET[@]}" up -d --force-recreate --scale "server=$REPLICAS" server >/dev/null 2>&1 \
  || fail "could not recreate the replicas"
SERVER_URLS=()
for i in $(seq 1 "$REPLICAS"); do
  mapping=$("${COMPOSE_FLEET[@]}" port --index "$i" server 8080 2>/dev/null || true)
  port="${mapping##*:}"
  [[ -n "$port" && "$port" != "$mapping" ]] || fail "could not read the host port for replica $i"
  SERVER_URLS+=("http://localhost:$port")
done
for url in "${SERVER_URLS[@]}"; do wait_ready "$url"; done
info "  replicas: ${SERVER_URLS[*]}"


# One scrape per replica per tick: separate curls would sample different instants.
sample_one() {
  curl -fsS "$1/actuator/prometheus" 2>/dev/null | awk '
    /^skier_ingest_accepted_total\{/    { a=$2 }
    /^skier_ingest_shed_total\{/        { s=$2 }
    /^skier_admission_rate\{/           { r=$2 }
    /^skier_admission_rate_floor\{/     { f=$2 }
    /^skier_admission_floor_pinned_total\{/ { p=$2 }
    /^skier_queue_depth\{/              { d=$2 }
    /^skier_fleet_size\{/               { n=$2 }
    /^skier_fleet_registry_staleness_seconds\{/ { g=$2 }
    /^skier_admission_refill_invocations\{/ { v=$2 }
    /^skier_admission_refill_gap_max_seconds\{/ { m=$2 }
    /^skier_admission_permits_discarded\{/ { x=$2 }
    /^skier_admission_tokens\{/          { tk=$2 }
    END { printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", a, s, r, f, p, d, n, g, v, m, x, tk }'
}

# CPU alongside the scrapes, because a replica admitting less than its own floor is either running
# its scheduled work late or starved of host CPU, and no application metric tells the two apart.
STATS="$OUT_DIR/docker-stats.csv"
stats_sampler() {
  local start now
  start=$(python3 -c 'import time; print(int(time.monotonic()*1000))')
  echo "elapsed_ms,name,cpu_percent,mem_usage" > "$STATS"
  while :; do
    now=$(python3 -c 'import time; print(int(time.monotonic()*1000))')
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null \
      | while IFS= read -r line; do echo "$((now - start)),$line" >> "$STATS"; done
    sleep 3
  done
}

SAMPLES="$OUT_DIR/samples.csv"
# fleet_size is what each replica believed the fleet size to be; without it "aggregate admission
# came out at the configured floor" reads the same whether coordination worked or the arm was N=1.
echo "elapsed_ms,replica,accepted,shed,rate,floor,floor_pinned,depth,fleet_size,registry_staleness,refill_invocations,refill_gap_max,permits_discarded,admission_tokens" \
  > "$SAMPLES"

sampler() {
  local start now line i
  start=$(python3 -c 'import time; print(int(time.monotonic()*1000))')
  while :; do
    now=$(python3 -c 'import time; print(int(time.monotonic()*1000))')
    i=0
    for u in "${SERVER_URLS[@]}"; do
      i=$((i + 1))
      line=$(sample_one "$u")
      [[ -n "$line" ]] && echo "$((now - start)),$i,$line" >> "$SAMPLES"
    done
    sleep 1
  done
}


# The joined target list matters: with one URL the other replicas' controllers still tick and shed
# but admit nothing, so aggregate admission would equal the N=1 case however many replicas are up.
BASE_URLS=$(IFS=,; echo "${SERVER_URLS[*]}")

info "offering load to $REPLICAS replica(s): $BASE_URLS"
sampler &
SAMPLER_PID=$!
stats_sampler &
STATS_PID=$!
trap 'kill "$SAMPLER_PID" "$STATS_PID" 2>/dev/null || true' EXIT

java -jar Client/SkierClient/target/skier-client.jar run \
  --scenario "$SCENARIO" \
  --base-url "$BASE_URLS" \
  --out "$OUT_DIR/client" \
  --fail-under-success-rate 0.0 >"$OUT_DIR/client.log" 2>&1 \
  || fail "load generator failed (see $OUT_DIR/client.log)"

kill "$SAMPLER_PID" "$STATS_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
wait "$STATS_PID" 2>/dev/null || true
trap - EXIT

cp "$SCENARIO" "$OUT_DIR/scenario.yaml"


python3 scripts/fleet-analyse.py "$SAMPLES" "$REPLICAS" "$SETTLE_SECONDS" \
  | tee "$OUT_DIR/summary.txt"

green ""
green "samples: $SAMPLES"
green "summary: $OUT_DIR/summary.txt"
