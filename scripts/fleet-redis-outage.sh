#!/usr/bin/env bash
# Take Redis away from a live fleet and record what each replica's admission floor does.
#
#   scripts/fleet-redis-outage.sh [replicas] [out-dir]
#
# `RedisFleetRegistry` holds its last known fleet size when a refresh fails rather than falling back
# to 1, because an instance that believes it is alone takes the whole admission floor instead of its
# share. No load is offered: the quantity is a gauge computed on the heartbeat thread whether or not
# a request arrives, so load would only fold in the consumer's drain rate.
#
# Five phases into one CSV with a `phase` column. redis-restored is sampled faster than the rest,
# because Redis has no persistence and returns with an empty sorted set, so the first replica to
# heartbeat into it reads a cardinality of 1 for a couple of seconds. The two coldstart phases reach
# the branch holding cannot cover: a process that never completed a refresh has no value to hold.
set -euo pipefail

# Re-analyse a recorded arm without re-running it:  --analyse <out-dir> [replicas]. The summary is
# a function of samples.csv alone, so every archived arm stays replayable.
if [[ "${1:-}" == "--analyse" ]]; then
  DIR="${2:?usage: scripts/fleet-redis-outage.sh --analyse <out-dir> [replicas]}"
  python3 scripts/fleet-redis-outage-analyse.py "$DIR/samples.csv" "${3:-2}" \
    > "$DIR/summary.txt"
  cat "$DIR/summary.txt"
  exit 0
fi

REPLICAS="${1:-2}"
OUT_DIR="${2:-benchmarks/out/redis-outage-$REPLICAS}"

BASELINE_SECONDS="${BASELINE_SECONDS:-12}"
DOWN_SECONDS="${DOWN_SECONDS:-60}"
RESTORED_SECONDS="${RESTORED_SECONDS:-30}"
COLDSTART_SECONDS="${COLDSTART_SECONDS:-30}"

COMPOSE_FLEET=(docker compose -f docker-compose.yml -f docker-compose.scale.yml)

# Same helpers as the other harnesses. The arguments stop ab.sh reading the replica count as an axis.
AB_SOURCE_ONLY=1 . scripts/ab.sh writer 0

[[ "$REPLICAS" =~ ^[1-9][0-9]*$ ]] || fail "replicas must be a positive integer, got '$REPLICAS'"

mkdir -p "$OUT_DIR"

info "starting $REPLICAS server replica(s)..."
"${COMPOSE_FLEET[@]}" up -d --build --scale "server=$REPLICAS" >/dev/null 2>&1 \
  || fail "compose up --scale server=$REPLICAS failed"

SERVER_URLS=()
read_ports() {
  SERVER_URLS=()
  local i mapping port
  for i in $(seq 1 "$1"); do
    mapping=$("${COMPOSE_FLEET[@]}" port --index "$i" server 8080 2>/dev/null || true)
    port="${mapping##*:}"
    [[ -n "$port" && "$port" != "$mapping" ]] || fail "could not read the host port for replica $i"
    SERVER_URLS+=("http://localhost:$port")
  done
}
read_ports "$REPLICAS"
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

# A recorded arm carries the configuration that produced it. There is no scenario.yaml because
# there is no load generator, so the effective config is read out of a running replica instead.
{
  echo "# Effective configuration, read from replica 1 with \`printenv\` at the start of the arm."
  echo "replicas: $REPLICAS"
  echo "phase_seconds: {baseline: $BASELINE_SECONDS, redis_down: $DOWN_SECONDS,"
  echo "                redis_restored: $RESTORED_SECONDS, coldstart: $COLDSTART_SECONDS}"
  echo "server_env:"
  "${COMPOSE_FLEET[@]}" exec -T --index 1 server \
    sh -c 'printenv | grep -E "^(ADMISSION_|FLEET_|QUEUE_MONITOR_|REDIS_)" | sort' 2>/dev/null \
    | sed 's/^/  /'
} > "$OUT_DIR/config.yaml"

# All values out of one scrape, for the reason scripts/fleet.sh does it. Readiness and container
# state are recorded alongside, so "the replica stayed up and answered" is in the artifact.
sample_one() {
  local url="$1" scrape readiness
  scrape=$(curl -fsS --max-time 5 "$url/actuator/prometheus" 2>/dev/null || true)
  readiness=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
    "$url/actuator/health/readiness" 2>/dev/null || echo 000)
  [[ -n "$scrape" ]] || { echo ",,,,,,$readiness"; return; }
  printf '%s,%s' \
    "$(printf '%s\n' "$scrape" | awk '
      /^skier_fleet_size\{/                        { n=$2 }
      /^skier_admission_rate_floor\{/              { f=$2 }
      /^skier_fleet_registry_staleness_seconds\{/  { g=$2 }
      /^skier_fleet_registry_failures_total\{/     { e=$2 }
      /^skier_fleet_registry_shrink_total\{/       { h=$2 }
      /^skier_admission_rate\{/                    { r=$2 }
      END { printf "%s,%s,%s,%s,%s,%s", n, f, g, e, h, r }')" \
    "$readiness"
}

SAMPLES="$OUT_DIR/samples.csv"
echo "elapsed_ms,phase,replica,fleet_size,floor,registry_staleness,registry_failures,registry_shrinks,rate,readiness" \
  > "$SAMPLES"

START_MS=$(python3 -c 'import time; print(int(time.monotonic()*1000))')

sample_for() {
  local phase="$1" seconds="$2" interval="$3"
  local deadline now i
  deadline=$(python3 -c "import time; print(int(time.monotonic()*1000) + $seconds * 1000)")
  while :; do
    now=$(python3 -c 'import time; print(int(time.monotonic()*1000))')
    [[ "$now" -ge "$deadline" ]] && break
    i=0
    for u in "${SERVER_URLS[@]}"; do
      i=$((i + 1))
      echo "$((now - START_MS)),$phase,$i,$(sample_one "$u")" >> "$SAMPLES"
    done
    sleep "$interval"
  done
}

info "phase 1/5: baseline, Redis up (${BASELINE_SECONDS}s)"
sample_for baseline "$BASELINE_SECONDS" 2

info "phase 2/5: stopping redis (${DOWN_SECONDS}s)"
"${COMPOSE_FLEET[@]}" stop redis >/dev/null 2>&1 || fail "could not stop redis"
"${COMPOSE_FLEET[@]}" ps --format '{{.Service}} {{.State}}' > "$OUT_DIR/ps-redis-down.txt" 2>&1 || true
sample_for redis-down "$DOWN_SECONDS" 2

info "phase 3/5: starting redis (${RESTORED_SECONDS}s)"
"${COMPOSE_FLEET[@]}" start redis >/dev/null 2>&1 || fail "could not start redis"
sample_for redis-restored "$RESTORED_SECONDS" 0.5

COLD=$((REPLICAS + 1))
info "phase 4/5: stopping redis, then starting replica $COLD into the outage (${COLDSTART_SECONDS}s)"
"${COMPOSE_FLEET[@]}" stop redis >/dev/null 2>&1 || fail "could not stop redis"
# --no-deps is required: without it Compose satisfies `depends_on: redis: service_healthy` by
# starting Redis, which removes the outage this phase is about.
"${COMPOSE_FLEET[@]}" up -d --no-deps --scale "server=$COLD" server >/dev/null 2>&1 \
  || fail "could not start replica $COLD with redis down"
read_ports "$COLD"
info "  replicas: ${SERVER_URLS[*]}"
# Deliberately not wait_ready: whether the new replica becomes ready with Redis away is measured.
sample_for coldstart-down "$COLDSTART_SECONDS" 2

info "phase 5/5: starting redis with replica $COLD live (${COLDSTART_SECONDS}s)"
"${COMPOSE_FLEET[@]}" start redis >/dev/null 2>&1 || fail "could not start redis"
sample_for coldstart-restored "$COLDSTART_SECONDS" 1

"${COMPOSE_FLEET[@]}" ps --format '{{.Service}} {{.State}}' > "$OUT_DIR/ps-final.txt" 2>&1 || true
for i in $(seq 1 "$COLD"); do
  "${COMPOSE_FLEET[@]}" logs --index "$i" --no-log-prefix server 2>/dev/null \
    | grep -i 'fleet' > "$OUT_DIR/replica-$i-fleet.log" || true
done

python3 scripts/fleet-redis-outage-analyse.py "$SAMPLES" "$REPLICAS" | tee "$OUT_DIR/summary.txt"

green ""
green "samples: $SAMPLES"
green "summary: $OUT_DIR/summary.txt"
