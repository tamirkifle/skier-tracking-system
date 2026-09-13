#!/usr/bin/env bash
# Measures accept-to-queryable: the interval from a 201 on the ingest endpoint to the first read
# through the public API that reflects the event.
#
#   scripts/queryability.sh [--out DIR] [--include-totals]
#
# SLO objective 5 measures accept-to-durable. What a client experiences is that figure plus however
# long the read cache keeps serving the answer that predates the event: 10 s for `skierDayVertical`,
# 30 s for `resortSkierCount`, 5 min for `skierResortTotals`.
#
# `SkierService` never caches a zero, so a brand-new key has no TTL term at all and the TTL bites
# only on an update to a key already holding a non-zero value. Each cache is therefore measured in
# three modes and the contrast between them is the measurement: `first` on a fresh key, `warm` on a
# key read before the second post, `cold` on a key never read. Each arm also records the pipeline
# freshness of its own events, so queryable minus freshness is the cache's contribution.
set -euo pipefail

# Re-render a recorded arm without running it again:  --summarise <out-dir>.
summarise() {  # result.json
  python3 - "$1" <<'PY'
import json, sys

with open(sys.argv[1]) as fh:
    run = json.load(fh)

print(f"accept-to-queryable, {run['started_utc']}, poll interval {run['poll_interval_seconds']}s")
print()
print(f"{'arm':<34}{'TTL':>6}{'queryable':>12}{'freshness':>12}{'cache term':>12}")
for a in run["arms"]:
    fresh = a["freshness_mean_seconds"]
    term = a["cache_term_seconds"]
    print(
        f"{a['arm']:<34}{a['ttl_seconds']:>5}s{a['queryable_seconds']:>11.2f}s"
        f"{('n/a' if fresh is None else f'{fresh:.3f}'):>12}"
        f"{('n/a' if term is None else f'{term:.3f}'):>12}"
        + ("" if a["flipped"] else "   NEVER FLIPPED")
    )
print()
for cache in dict.fromkeys(a["cache"] for a in run["arms"]):
    arms = {a["mode"]: a for a in run["arms"] if a["cache"] == cache}
    if "first" in arms and "warm" in arms:
        ttl = arms["warm"]["ttl_seconds"]
        print(
            f"{cache}: first {arms['first']['queryable_seconds']:.2f}s vs warm "
            f"{arms['warm']['queryable_seconds']:.2f}s against a configured TTL of {ttl}s"
        )
PY
}

if [[ "${1:-}" == "--summarise" ]]; then
  DIR="${2:?usage: scripts/queryability.sh --summarise <out-dir>}"
  summarise "$DIR/result.json" > "$DIR/summary.txt"
  cat "$DIR/summary.txt"
  exit 0
fi

BASE_URL="${BASE_URL:-http://localhost:8080}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:8085}"
SEASON="${SEASON:-2025}"
DAY="${DAY:-1}"
POLL_INTERVAL="${POLL_INTERVAL:-0.25}"

# The three TTLs under measurement, from CacheConfig, so the run records what it believed.
TTL_VERTICAL=10
TTL_COUNT=30
TTL_TOTALS=300

OUT_DIR=""
INCLUDE_TOTALS=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="${2:?--out needs a directory}"; shift 2 ;;
    --include-totals) INCLUDE_TOTALS=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
info()  { printf '\033[36m%s\033[0m\n' "$*"; }
fail()  { red "FAIL: $*"; exit 1; }

# EPOCHREALTIME is bash 5+ and always carries six decimals; the separator is locale-dependent, hence
# the character class. A python3 call per poll would add ~30 ms of its own to the measurement.
now_ms() { local t="${EPOCHREALTIME/[.,]/}"; echo $(( 10#$t / 1000 )); }

read_vertical() {  # resort skier
  curl -fsS --max-time 10 "$BASE_URL/skiers/$1/seasons/$SEASON/days/$DAY/skiers/$2" 2>/dev/null \
    | sed -n 's/.*"totalVertical":\([0-9]*\).*/\1/p' | head -1
}

read_count() {  # resort
  curl -fsS --max-time 10 "$BASE_URL/resorts/$1/seasons/$SEASON/day/$DAY/skiers" 2>/dev/null \
    | sed -n 's/.*"uniqueNumSkiers":\([0-9]*\).*/\1/p' | head -1
}

read_totals() {  # resort skier
  curl -fsS --max-time 10 "$BASE_URL/skiers/$2/vertical?resort=$1&season=$SEASON" 2>/dev/null \
    | sed -n 's/.*"totalVert":\([0-9]*\).*/\1/p' | head -1
}

consumer_scrape() { curl -fsS --max-time 10 "$CONSUMER_URL/actuator/prometheus" 2>/dev/null || true; }

# index($0,m)==1 rather than a regex: a prefix match on a HELP or TYPE line returns the wrong field.
metric_of() {  # scrape name
  awk -v m="$2" 'index($0,m)==1 { n=split($0,a," "); v=a[n] } END { printf "%s", (v==""?"0":v) }' \
    <<<"$1"
}

POLL_VALUE=0
POLL_MS=0
poll_until_at_least() {  # target deadline_s reader args...
  local target="$1" deadline_s="$2"; shift 2
  local end val now
  end=$(( $(now_ms) + deadline_s * 1000 ))
  while :; do
    val=$("$@")
    now=$(now_ms)
    val="${val:-0}"
    POLL_VALUE="$val"
    POLL_MS="$now"
    [[ "$val" -ge "$target" ]] && return 0
    [[ "$now" -ge "$end" ]] && return 1
    sleep "$POLL_INTERVAL"
  done
}

POST_MS=0
post_ride() {  # resort skier lift time
  local response
  response=$(curl -fsS -o /dev/null -w '%{http_code}' --max-time 10 -X POST \
    -H 'Content-Type: application/json' \
    -d "{\"liftID\":$3,\"time\":$4}" \
    "$BASE_URL/skiers/$1/seasons/$SEASON/days/$DAY/skier/$2") \
    || fail "POST for skier $2 at resort $1 failed"
  # Taken after the response, not before: the server publishes before it answers, so measuring from
  # here can only understate accept-to-queryable, and an overstatement is the dangerous direction.
  POST_MS=$(now_ms)
  [[ "$response" == "201" ]] || fail "expected 201 from the ingest endpoint, got $response"
}

rand_skier() { echo $(( (RANDOM % 80000) + 10000 )); }


info "Accept-to-queryable probe against $BASE_URL (consumer $CONSUMER_URL)"

curl -fsS --max-time 10 -o /dev/null "$BASE_URL/actuator/health/readiness" \
  || fail "server is not ready at $BASE_URL, run \`make up\` first"
[[ -n "$(consumer_scrape)" ]] || fail "consumer metrics are not reachable at $CONSUMER_URL"

# The resortSkierCount arms need a resort-day whose count is genuinely zero, because the arm asserts
# 0 -> 1 -> 2. Probing is free of side effects: a zero is never cached, so this loop warms nothing.
VIRGIN=()
for r in $(seq 1 10); do
  if [[ "${#VIRGIN[@]}" -ge 4 ]]; then break; fi
  observed_count=$(read_count "$r")
  if [[ -z "$observed_count" || "$observed_count" == "0" ]]; then VIRGIN+=("$r"); fi
done
[[ "${#VIRGIN[@]}" -ge 4 ]] \
  || fail "need 4 resort-days with a zero unique-skier count and found ${#VIRGIN[@]}; \`make down && make up\` for a clean stack"
RESORT_VERTICAL="${VIRGIN[0]}"
info "  resorts: vertical/totals arms on ${RESORT_VERTICAL}, count arms on ${VIRGIN[1]} ${VIRGIN[2]} ${VIRGIN[3]}"
info "  poll interval ${POLL_INTERVAL}s, every figure below is quantised to that"
echo


ARMS_JSON=""
FAILURES=0

ARM_FRESH_SUM_BEFORE=0
ARM_FRESH_COUNT_BEFORE=0
arm_begin() {
  local scrape
  scrape=$(consumer_scrape)
  ARM_FRESH_SUM_BEFORE=$(metric_of "$scrape" skier_pipeline_freshness_seconds_sum)
  ARM_FRESH_COUNT_BEFORE=$(metric_of "$scrape" skier_pipeline_freshness_seconds_count)
}

arm_end() {  # name cache ttl mode queryable_ms observed target ok
  local name="$1" cache="$2" ttl="$3" mode="$4" queryable_ms="$5" observed="$6" target="$7" ok="$8"
  local scrape sum_after count_after d_sum d_count fresh_mean q_s ttl_term
  scrape=$(consumer_scrape)
  sum_after=$(metric_of "$scrape" skier_pipeline_freshness_seconds_sum)
  count_after=$(metric_of "$scrape" skier_pipeline_freshness_seconds_count)
  read -r d_sum d_count fresh_mean q_s ttl_term < <(awk \
    -v sa="$sum_after" -v sb="$ARM_FRESH_SUM_BEFORE" \
    -v ca="$count_after" -v cb="$ARM_FRESH_COUNT_BEFORE" -v q="$queryable_ms" \
    'BEGIN { ds = sa - sb; dc = ca - cb; m = (dc > 0 ? ds / dc : -1);
             printf "%.3f %d %.3f %.3f %.3f\n", ds, dc, m, q / 1000, (m >= 0 ? q / 1000 - m : -1) }')
  # The trailing newline is required: `read` returns non-zero at EOF without one, and under `set -e`
  # that ends the run silently at the first arm, after printing the header and exiting 0 via a pipe.

  # A freshness mean over zero samples is absent, not zero: the arm's events never reached the
  # histogram. Reporting it as a number would let a broken instrument read as a fast pipeline.
  local fresh_json="$fresh_mean" term_json="$ttl_term" fresh_text="$fresh_mean" term_text="$ttl_term"
  if [[ "$d_count" -le 0 ]]; then
    fresh_json=null; term_json=null; fresh_text="n/a"; term_text="n/a"
  fi

  if [[ "$ok" == "1" ]]; then
    green "$(printf '  %-34s %7.2fs queryable   freshness %7s   cache term %7s' \
      "$name" "$q_s" "$fresh_text" "$term_text")"
  else
    red "$(printf '  %-34s TIMED OUT after %.2fs (saw %s, wanted %s)' \
      "$name" "$q_s" "$observed" "$target")"
    FAILURES=$((FAILURES + 1))
  fi

  ARMS_JSON="$ARMS_JSON$(printf '\n    {"arm": "%s", "cache": "%s", "ttl_seconds": %s, "mode": "%s", "queryable_seconds": %s, "freshness_mean_seconds": %s, "cache_term_seconds": %s, "freshness_samples": %s, "observed": %s, "target": %s, "flipped": %s},' \
    "$name" "$cache" "$ttl" "$mode" "$q_s" "$fresh_json" "$term_json" "$d_count" "$observed" "$target" \
    "$([[ "$ok" == "1" ]] && echo true || echo false)")"
}

# Waits for the consumer to durably write `n` more items without reading a cached endpoint. That is
# what makes `cold` cold: any read would populate the entry whose absence the mode is about.
await_writes() {  # n deadline_s
  local before after end
  before=$(metric_of "$(consumer_scrape)" skier_write_total)
  end=$(( $(now_ms) + $2 * 1000 ))
  while :; do
    after=$(metric_of "$(consumer_scrape)" skier_write_total)
    if awk -v a="$after" -v b="$before" -v n="$1" 'BEGIN { exit !(a - b >= n) }'; then return 0; fi
    if [[ "$(now_ms)" -ge "$end" ]]; then fail "consumer did not write $1 item(s) within $2s"; fi
    sleep 0.25
  done
}


info "skierDayVertical (TTL ${TTL_VERTICAL}s)"

# first: a fresh skier's first ride. Nothing can be cached under this key, so this is the control.
arm_begin
S=$(rand_skier); L=21
post_ride "$RESORT_VERTICAL" "$S" "$L" 217
T0=$POST_MS
poll_until_at_least $((L * 10)) $((TTL_VERTICAL + 60)) read_vertical "$RESORT_VERTICAL" "$S" && OK=1 || OK=0
arm_end "vertical/first (fresh key)" skierDayVertical "$TTL_VERTICAL" first \
  $((POLL_MS - T0)) "$POLL_VALUE" $((L * 10)) "$OK"

# warm: the poll above is the read that populates the entry, so a second ride is the client's case.
arm_begin
S=$(rand_skier); L=21
post_ride "$RESORT_VERTICAL" "$S" "$L" 218
poll_until_at_least $((L * 10)) $((TTL_VERTICAL + 60)) read_vertical "$RESORT_VERTICAL" "$S" \
  || fail "vertical/warm: the first ride never became queryable"
post_ride "$RESORT_VERTICAL" "$S" "$L" 219
T0=$POST_MS
poll_until_at_least $((L * 20)) $((TTL_VERTICAL + 60)) read_vertical "$RESORT_VERTICAL" "$S" && OK=1 || OK=0
arm_end "vertical/warm (read before write)" skierDayVertical "$TTL_VERTICAL" warm \
  $((POLL_MS - T0)) "$POLL_VALUE" $((L * 20)) "$OK"

# cold: as warm, except durability is witnessed on the consumer, so the key is never read first.
arm_begin
S=$(rand_skier); L=21
post_ride "$RESORT_VERTICAL" "$S" "$L" 220
await_writes 1 $((TTL_VERTICAL + 60))
post_ride "$RESORT_VERTICAL" "$S" "$L" 221
T0=$POST_MS
poll_until_at_least $((L * 20)) $((TTL_VERTICAL + 60)) read_vertical "$RESORT_VERTICAL" "$S" && OK=1 || OK=0
arm_end "vertical/cold (no read before)" skierDayVertical "$TTL_VERTICAL" cold \
  $((POLL_MS - T0)) "$POLL_VALUE" $((L * 20)) "$OK"

# The same three modes on a cache with three times the TTL.

info "resortSkierCount (TTL ${TTL_COUNT}s)"

arm_begin
R="${VIRGIN[1]}"
post_ride "$R" "$(rand_skier)" 21 217
T0=$POST_MS
poll_until_at_least 1 $((TTL_COUNT + 60)) read_count "$R" && OK=1 || OK=0
arm_end "count/first (virgin resort-day)" resortSkierCount "$TTL_COUNT" first \
  $((POLL_MS - T0)) "$POLL_VALUE" 1 "$OK"

arm_begin
R="${VIRGIN[2]}"
post_ride "$R" "$(rand_skier)" 21 217
poll_until_at_least 1 $((TTL_COUNT + 60)) read_count "$R" \
  || fail "count/warm: the first skier never became queryable"
post_ride "$R" "$(rand_skier)" 21 218
T0=$POST_MS
poll_until_at_least 2 $((TTL_COUNT + 60)) read_count "$R" && OK=1 || OK=0
arm_end "count/warm (read before write)" resortSkierCount "$TTL_COUNT" warm \
  $((POLL_MS - T0)) "$POLL_VALUE" 2 "$OK"

arm_begin
R="${VIRGIN[3]}"
post_ride "$R" "$(rand_skier)" 21 217
await_writes 1 $((TTL_COUNT + 60))
# The cardinality update is applied after the item write and before the ack, so `skier_write_total`
# leads the counter row. Settling here keeps the arm about the cache rather than about that gap.
sleep 1
post_ride "$R" "$(rand_skier)" 21 218
T0=$POST_MS
poll_until_at_least 2 $((TTL_COUNT + 60)) read_count "$R" && OK=1 || OK=0
arm_end "count/cold (no read before)" resortSkierCount "$TTL_COUNT" cold \
  $((POLL_MS - T0)) "$POLL_VALUE" 2 "$OK"

# Opt-in, because one warm arm costs its own TTL in wall clock.

if [[ "$INCLUDE_TOTALS" == "1" ]]; then
  info "skierResortTotals (TTL ${TTL_TOTALS}s)"

  arm_begin
  S=$(rand_skier); L=21
  post_ride "$RESORT_VERTICAL" "$S" "$L" 222
  T0=$POST_MS
  poll_until_at_least $((L * 10)) $((TTL_TOTALS + 120)) read_totals "$RESORT_VERTICAL" "$S" && OK=1 || OK=0
  arm_end "totals/first (fresh key)" skierResortTotals "$TTL_TOTALS" first \
    $((POLL_MS - T0)) "$POLL_VALUE" $((L * 10)) "$OK"

  arm_begin
  S=$(rand_skier); L=21
  post_ride "$RESORT_VERTICAL" "$S" "$L" 223
  poll_until_at_least $((L * 10)) $((TTL_TOTALS + 120)) read_totals "$RESORT_VERTICAL" "$S" \
    || fail "totals/warm: the first ride never became queryable"
  post_ride "$RESORT_VERTICAL" "$S" "$L" 224
  T0=$POST_MS
  poll_until_at_least $((L * 20)) $((TTL_TOTALS + 120)) read_totals "$RESORT_VERTICAL" "$S" && OK=1 || OK=0
  arm_end "totals/warm (read before write)" skierResortTotals "$TTL_TOTALS" warm \
    $((POLL_MS - T0)) "$POLL_VALUE" $((L * 20)) "$OK"
fi


if [[ -n "$OUT_DIR" ]]; then
  mkdir -p "$OUT_DIR"
  # A recorded run carries the configuration that produced it. There is no scenario.yaml because
  # there is no load generator, so the effective config is read out of the running containers.
  {
    echo "# Effective configuration, read from the running containers at the end of the run."
    echo "probe: scripts/queryability.sh"
    echo "poll_interval_seconds: $POLL_INTERVAL"
    echo "include_totals: $INCLUDE_TOTALS"
    echo "cache_ttls_seconds:   # CacheConfig constants, not environment"
    echo "  skierDayVertical: $TTL_VERTICAL"
    echo "  resortSkierCount: $TTL_COUNT"
    echo "  skierResortTotals: $TTL_TOTALS"
    echo "resorts: {vertical: $RESORT_VERTICAL, count: [${VIRGIN[1]}, ${VIRGIN[2]}, ${VIRGIN[3]}]}"
    echo "server_env:"
    docker compose exec -T server \
      sh -c 'printenv | grep -E "^(CACHE_|ADMISSION_|QUEUE_MONITOR_|WRITER_|CARDINALITY_)" | sort' \
      2>/dev/null | sed 's/^/  /'
    echo "consumer_env:"
    docker compose exec -T consumer \
      sh -c 'printenv | grep -E "^(WRITER_|CARDINALITY_|BATCH_|CONSUMER_)" | sort' \
      2>/dev/null | sed 's/^/  /'
  } > "$OUT_DIR/config.yaml"

  {
    echo '{'
    printf '  "probe": "scripts/queryability.sh",\n'
    printf '  "started_utc": "%s",\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '  "poll_interval_seconds": %s,\n' "$POLL_INTERVAL"
    printf '  "arms": [%s\n  ]\n' "${ARMS_JSON%,}"
    echo '}'
  } > "$OUT_DIR/result.json"
  summarise "$OUT_DIR/result.json" > "$OUT_DIR/summary.txt"
  info ""
  info "recorded to $OUT_DIR"
fi

echo
if [[ "$FAILURES" -gt 0 ]]; then
  fail "$FAILURES arm(s) never became queryable within their bound"
fi
green "All arms became queryable within their bound."
