#!/usr/bin/env bash
# A/B one axis of the consumer's configuration, holding everything else fixed.
#
#   scripts/ab.sh writer       [events]   single PutItem vs coalesced BatchWriteItem
#   scripts/ab.sh cardinality  [events]   exact sentinel vs Redis HyperLogLog
#   AB_REVERSE=1 scripts/ab.sh writer     run the arms in the opposite order
#
# Three things the default configuration does not give are needed to isolate the write path.
# Admission control is off: the queue-depth controller throttles the front door so the queue never
# builds, which is correct behaviour and useless here, because a writer benchmark needs a full
# queue to drain. The measurement is consumer-side request and item counters rather than client
# latency, because batching changes the number of requests and not DynamoDB's per-item cost. And
# every arm starts from empty datastores rather than from whatever the previous arm left; see
# `reset_datastores`.
set -euo pipefail

AXIS="${1:-writer}"
EVENTS="${2:-20000}"

case "$AXIS" in
  writer)      SWEEP_VAR=WRITER_MODE;            ARMS=(single batch) ;;
  cardinality) SWEEP_VAR=CARDINALITY_STRATEGY;   ARMS=(dynamodb redis-hll) ;;
  *) echo "unknown axis '$AXIS' (expected: writer | cardinality)" >&2; exit 2 ;;
esac

# Arm order is a variable in this experiment, not a constant: a result that survives the swap is one
# the state reset de-biased. Reporting stays canonical, so two runs compare column by column.
RUN_ORDER=("${ARMS[@]}")
if [[ "${AB_REVERSE:-0}" == "1" ]]; then
  RUN_ORDER=("${ARMS[1]}" "${ARMS[0]}")
fi

BASE_URL="${BASE_URL:-http://localhost:8080}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:8085}"
OUT="${OUT:-benchmarks/out/ab-$AXIS}"

# The schema tool applies the tables under AWS_REGION=us-west-2 and LocalStack namespaces by region,
# so an `awslocal` call defaulting to us-east-1 sees an empty account and reports every table as
# absent. That failure looks exactly like "the reset worked".
DDB_REGION="${AWS_REGION:-us-west-2}"
TABLES=(LiftRides SkierCounts SkierTracking)
MAIN_QUEUE=liftRideQueue

info()  { printf '\033[36m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
fail()  { printf '\033[31mFAIL: %s\033[0m\n' "$*"; exit 1; }

metric() {
  curl -fsS "$CONSUMER_URL/actuator/prometheus" \
    | awk -v name="$1" '$1 ~ "^"name"\\{" { gsub(/[^0-9.eE+-]/, "", $2); s += $2 } END { printf "%.0f", s+0 }'
}

RABBITMQ_USERNAME="${RABBITMQ_USERNAME:-guest}"
RABBITMQ_PASSWORD="${RABBITMQ_PASSWORD:-guest}"
RABBITMQ_MANAGEMENT_URL="${RABBITMQ_MANAGEMENT_URL:-http://localhost:15672}"

queue_depth_of() {
  curl -fsS -u "$RABBITMQ_USERNAME:$RABBITMQ_PASSWORD" \
    "$RABBITMQ_MANAGEMENT_URL/api/queues/%2F/$1" 2>/dev/null \
    | sed -n 's/.*"messages":\([0-9]*\).*/\1/p' | head -1
}

queue_depth() {
  queue_depth_of "$MAIN_QUEUE"
}

staging_depth() {
  metric skier_write_staging_depth
}

ddb() {
  docker compose exec -T localstack awslocal --region "$DDB_REGION" dynamodb "$@"
}

# Return every arm to the same starting state. A first sighting is the absence of a sentinel row in
# `SkierTracking`, or a skier absent from the HyperLogLog sketch, so carrying either across an arm
# boundary makes the first arm do strictly more DynamoDB work. Per-arm namespacing cannot
# substitute: `resort-id` is the only free coordinate, and a fixed mapping collides with the last run.
reset_datastores() {
  info "  resetting datastore state..."

  # Stop the writer first: a write against a deleted table is an opaque ResourceNotFoundException.
  docker compose stop consumer >/dev/null 2>&1 || true
  docker compose exec -T rabbitmq rabbitmqctl purge_queue "$MAIN_QUEUE" >/dev/null 2>&1 || true

  local table
  for table in "${TABLES[@]}"; do
    ddb delete-table --table-name "$table" >/dev/null 2>&1 || true
  done

  # DeleteTable is asynchronous even on LocalStack, and re-creating before the delete lands returns
  # ResourceInUseException, which the schema tool reports as success while the old rows survive.
  local remaining
  for _ in $(seq 1 60); do
    remaining=$(ddb list-tables --query 'TableNames' --output text 2>/dev/null || echo '?')
    [[ "$remaining" != *SkierTracking* && "$remaining" != *SkierCounts* && "$remaining" != *LiftRides* ]] \
      && break
    sleep 1
  done

  docker compose run --rm schema >/dev/null 2>&1 || fail "schema re-apply failed after table reset"

  # FLUSHALL, not just `skiers:hll:*`: Redis also holds the read cache, which is carried-over state.
  docker compose exec -T redis redis-cli FLUSHALL >/dev/null 2>&1 || fail "redis flush failed"
}

# Assert the invariant the reset exists to establish rather than trusting that the reset ran. Empty
# or not is exact; "the two arms reported similar totals" passes whenever both are wrong alike.
assert_clean_state() {
  local arm="$1" problems=() table count keys depth

  for table in "${TABLES[@]}"; do
    count=$(ddb scan --table-name "$table" --select COUNT --query 'Count' --output text 2>/dev/null \
      || echo '?')
    [[ "$count" == "0" ]] || problems+=("$table holds $count item(s)")
  done

  # Every key except the fleet-coordination set, which each running server re-adds within a
  # heartbeat of any flush. awk rather than `grep -v`: an empty key list makes grep exit 1, which
  # pipefail turns into a failure of the check on its healthiest possible input.
  keys=$(docker compose exec -T redis redis-cli KEYS '*' 2>/dev/null \
    | awk '{ sub(/\r$/, "") } $0 != "" && $0 != "skier:admission:fleet" { n++ } END { print n + 0 }')
  [[ "$keys" == "0" ]] || problems+=("redis holds $keys non-fleet key(s)")

  depth=$(queue_depth || echo 0)
  [[ "${depth:-0}" -eq 0 ]] || problems+=("$MAIN_QUEUE holds $depth message(s)")

  if [[ ${#problems[@]} -gt 0 ]]; then
    # `${array[*]}` joins on the first character of IFS only, so a two-character separator is built.
    fail "arm '$arm' would start from carried-over state: $(printf '%s; ' "${problems[@]}")"
  fi
  info "  state clean: all tables empty, redis empty apart from fleet membership, queue empty"
}

run_one() {
  local arm="$1" position="$2"
  info ""
  info "=============================================================="
  info " $SWEEP_VAR=$arm  ($EVENTS events, run position $position)"
  info "=============================================================="

  reset_datastores
  assert_clean_state "$arm"

  QUEUE_MONITOR_ENABLED=false ADMISSION_INITIAL_CAPACITY=8000 \
    docker compose up -d --force-recreate server >/dev/null 2>&1
  env "$SWEEP_VAR=$arm" docker compose up -d --force-recreate consumer >/dev/null 2>&1

  for _ in $(seq 1 60); do
    curl -fsS "$BASE_URL/actuator/health/readiness" >/dev/null 2>&1 \
      && curl -fsS "$CONSUMER_URL/actuator/health/readiness" >/dev/null 2>&1 && break
    sleep 2
  done

  # Confirm the consumer selected the requested strategy: both factories warn and degrade instead.
  local selected expected
  selected=$(docker compose logs consumer 2>/dev/null \
    | grep -oE 'writer=[a-z-]+|cardinality=[a-z-]+' | tail -2 | tr '\n' ' ')
  info "  consumer reports $selected"

  case "$AXIS:$arm" in
    writer:single)           expected="writer=single-put" ;;
    writer:batch)            expected="writer=batch-write-item" ;;
    cardinality:dynamodb)    expected="cardinality=dynamodb-transactional" ;;
    cardinality:redis-hll)   expected="cardinality=redis-hyperloglog" ;;
  esac

  # Matched against the captured string rather than by re-grepping the stream. `grep -q` in a
  # pipeline exits on its first match, sends SIGPIPE upstream, and under `set -o pipefail` that
  # propagates as a pipeline failure, so a re-grep fails this assertion precisely when it matches.
  [[ "$selected" == *"$expected"* ]] \
    || fail "consumer did not select $AXIS arm '$arm' (expected $expected, got: $selected)"

  local items_before requests_before unique_before dup_before cardreq_before
  items_before=$(metric skier_write_total)
  requests_before=$(metric skier_write_batch_size_count)
  # The cardinality path's own request count, which unlike first_sightings IS comparable across the
  # arms of this axis: it counts requests issued, a property of the code rather than of PFADD.
  cardreq_before=$(metric skier_cardinality_write_requests_total)
  # first_sightings and duplicates_suppressed are NOT comparable across the cardinality arms: exact
  # under `dynamodb`, from PFADD's changed-register signal under `redis-hll`. Report, never diff.
  unique_before=$(metric skier_cardinality_unique_observed_total)
  dup_before=$(metric skier_cardinality_duplicate_suppressed_total)

  info "  publishing $EVENTS events..."
  local publish_start publish_end
  publish_start=$(python3 -c 'import time; print(time.time())')

  java -jar Client/SkierClient/target/skier-client.jar run \
    --scenario "$OUT/scenario.yaml" \
    --base-url "$BASE_URL" \
    --out "$OUT/$arm" \
    --fail-under-success-rate 0.0 >/dev/null 2>&1 \
    || fail "load generator failed for $SWEEP_VAR=$arm"

  publish_end=$(python3 -c 'import time; print(time.time())')

  # Both the broker queue and the staging queue must reach zero; the broker empties first.
  info "  draining..."
  local drained=0
  for _ in $(seq 1 300); do
    local q s
    q=$(queue_depth || echo 0)
    s=$(staging_depth || echo 0)
    if [[ "${q:-0}" -eq 0 && "${s:-0}" -eq 0 ]]; then
      sleep 2  # settle, in case a batch is mid-flight
      q=$(queue_depth || echo 0); s=$(staging_depth || echo 0)
      [[ "${q:-0}" -eq 0 && "${s:-0}" -eq 0 ]] && { drained=1; break; }
    fi
    sleep 1
  done
  local drain_end
  drain_end=$(python3 -c 'import time; print(time.time())')
  [[ "$drained" -eq 1 ]] || info "  (warning: did not fully drain within 300s)"

  local items_after requests_after unique_after dup_after cardreq_after
  items_after=$(metric skier_write_total)
  requests_after=$(metric skier_write_batch_size_count)
  unique_after=$(metric skier_cardinality_unique_observed_total)
  dup_after=$(metric skier_cardinality_duplicate_suppressed_total)
  cardreq_after=$(metric skier_cardinality_write_requests_total)

  python3 - "$arm" "$items_before" "$items_after" "$requests_before" "$requests_after" \
      "$publish_start" "$publish_end" "$drain_end" "$OUT" \
      "$unique_before" "$unique_after" "$dup_before" "$dup_after" "$position" \
      "$cardreq_before" "$cardreq_after" <<'PY'
import json, os, sys
mode, ib, ia, rb, ra, ps, pe, de, out, ub, ua, db, da, pos, cb, ca = sys.argv[1:]
items = int(ia) - int(ib)
requests = int(ra) - int(rb)
publish_s = float(pe) - float(ps)
total_s = float(de) - float(ps)
result = {
    "mode": mode,
    "run_position": int(pos),
    "items_written": items,
    # Lift-ride BatchWriteItem calls only. Read `cardinality_write_requests` for that axis.
    "dynamodb_write_requests": requests,
    "cardinality_write_requests": int(ca) - int(cb),
    "items_per_request": round(items / requests, 2) if requests else 0,
    "publish_seconds": round(publish_s, 1),
    "total_seconds_to_drained": round(total_s, 1),
    "write_throughput_items_per_second": round(items / total_s, 1) if total_s else 0,
    "first_sightings": int(ua) - int(ub),
    "duplicates_suppressed": int(da) - int(db),
}
# The order-independent form: the first-sighting share should not depend on which arm ran first.
result["first_sighting_share"] = round(result["first_sightings"] / items, 4) if items else 0
seen = result["first_sightings"] + result["duplicates_suppressed"]
result["cardinality_ops"] = seen
# Computed before the dump: the comparison block reads this key back out of the per-arm JSON.
result["cardinality_requests_per_event"] = (
    round(result["cardinality_write_requests"] / items, 3) if items else 0
)
os.makedirs(out, exist_ok=True)
with open(os.path.join(out, f"{mode}.json"), "w") as f:
    json.dump(result, f, indent=2)
print(f"\033[32m  items written        {items:,}\033[0m")
print(f"\033[32m  DynamoDB requests    {requests:,}  (lift-ride writes only)\033[0m")
print(f"\033[32m  cardinality requests {result['cardinality_write_requests']:,}"
      f"  ({result['cardinality_requests_per_event']} per event)\033[0m")
print(f"\033[32m  items per request    {result['items_per_request']}\033[0m")
print(f"\033[32m  drained in           {result['total_seconds_to_drained']}s"
      f"  ({result['write_throughput_items_per_second']:,.0f} items/s)\033[0m")
print(f"\033[32m  first sightings      {result['first_sightings']:,}"
      f"  ({result['first_sighting_share']:.1%} of items)\033[0m")
print(f"\033[32m  duplicates suppressed{result['duplicates_suppressed']:,}\033[0m")
PY
}

# Sourceable, so the state helpers can be reused and the assertions exercised against a deliberately
# dirty datastore:  AB_SOURCE_ONLY=1 . scripts/ab.sh && assert_clean_state probe
# A sourced script sees the caller's positional parameters, so it has to be given an axis;
# scripts/fleet.sh and scripts/recovery.sh both do.
[[ "${AB_SOURCE_ONLY:-0}" == "1" ]] && return 0

mkdir -p "$OUT"

# Sized to fill the queue fast without making the client the constraint: a backlog, not an API test.
cat > "$OUT/scenario.yaml" <<YAML
name: ab-$AXIS
description: >
  Fixed event count used to A/B one axis of the consumer's configuration. Admission control is
  disabled for this run so the events reach the queue; see scripts/ab.sh.
workload:
  skiers: 100000
  resort-id: 5
  season-id: 2025
  day-id: 1
  lifts: 40
  minutes-in-ski-day: 360
  seed: 20250421
http:
  max-connections: 400
  max-connections-per-route: 200
  max-retries: 2
phases:
  - name: warmup
    mode: closed-loop
    warmup: true
    threads: 16
    requests-per-thread: 50
  - name: load
    mode: closed-loop
    threads: 64
    requests-per-thread: $(( EVENTS / 64 ))
YAML

command -v java >/dev/null || fail "java not found"
[[ -f Client/SkierClient/target/skier-client.jar ]] \
  || fail "client jar missing; run: mvn -q -pl Client/SkierClient -am package -DskipTests"
curl -fsS "$BASE_URL/actuator/health" >/dev/null 2>&1 || fail "stack not running; run: make up"

info "run order: ${RUN_ORDER[0]} then ${RUN_ORDER[1]}"
position=0
for arm in "${RUN_ORDER[@]}"; do
  position=$((position + 1))
  run_one "$arm" "$position"
done

info ""
info "=============================================================="
python3 - "$OUT" "${ARMS[0]}" "${ARMS[1]}" <<'PY'
import json, os, sys
out, arm_a, arm_b = sys.argv[1:4]
rows = []
for mode in (arm_a, arm_b):
    path = os.path.join(out, f"{mode}.json")
    if os.path.exists(path):
        rows.append(json.load(open(path)))

if len(rows) == 2:
    s, b = rows
    print("\033[36m Result\033[0m")
    print(f"  {'':22} {arm_a:>12} {arm_b:>12}  {'change':>10}")
    def line(label, key, fmt="{:,.0f}", better_lower=True):
        sv, bv = s[key], b[key]
        change = "-"
        if sv:
            pct = (bv - sv) / sv * 100
            change = f"{pct:+.1f}%"
        print(f"  {label:22} {fmt.format(sv):>12} {fmt.format(bv):>12}  {change:>10}")
    line("items written", "items_written")
    line("lift-ride requests", "dynamodb_write_requests")
    line("items per request", "items_per_request", "{:,.2f}")
    # Reported on both axes: a writer run that moved this would mean the axes are not independent.
    line("cardinality requests", "cardinality_write_requests")
    line("cardinality req/event", "cardinality_requests_per_event", "{:,.3f}")
    line("drain seconds", "total_seconds_to_drained", "{:,.1f}")
    line("items/second", "write_throughput_items_per_second", "{:,.1f}")
    line("first sightings", "first_sightings")
    line("first-sighting share", "first_sighting_share", "{:.4f}")
    line("duplicates suppressed", "duplicates_suppressed")
    print(f"  {'run order':22} {s['run_position']:>12} {b['run_position']:>12}")
    with open(os.path.join(out, "comparison.json"), "w") as f:
        json.dump({arm_a: s, arm_b: b}, f, indent=2)
    print(f"\n  written to {out}/comparison.json")
PY
