#!/usr/bin/env bash
# Proves the pipeline end to end against a running stack: post an event, wait for the consumer to
# persist it, then read it back through each query endpoint. Exits non-zero when it does not.

set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:8085}"
RESORT="${RESORT:-5}"
SEASON="${SEASON:-2025}"
DAY="${DAY:-1}"
SKIER="${SKIER:-$(( (RANDOM % 90000) + 10000 ))}"
LIFT="${LIFT:-21}"
TIME_MINUTE="${TIME_MINUTE:-217}"
EXPECTED_VERTICAL=$(( LIFT * 10 ))

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
info()  { printf '\033[36m%s\033[0m\n' "$*"; }

fail() { red "FAIL: $*"; exit 1; }

info "Smoke test against $BASE_URL (skier $SKIER, resort $RESORT, day $DAY)"

info "1/7  readiness"
status=$(curl -fsS -o /dev/null -w '%{http_code}' "$BASE_URL/actuator/health/readiness") \
  || fail "server is not reachable at $BASE_URL"
[[ "$status" == "200" ]] || fail "readiness returned $status"
green "     ready"

info "2/7  input validation"
status=$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  -H 'Content-Type: application/json' \
  -d '{"liftID":9999,"time":217}' \
  "$BASE_URL/skiers/$RESORT/seasons/$SEASON/days/$DAY/skier/$SKIER")
[[ "$status" == "400" ]] || fail "expected 400 for an out-of-range liftID, got $status"

status=$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  -H 'Content-Type: application/json' \
  -d "{\"liftID\":$LIFT,\"time\":$TIME_MINUTE}" \
  "$BASE_URL/skiers/999/seasons/$SEASON/days/$DAY/skier/$SKIER")
[[ "$status" == "400" ]] || fail "expected 400 for an out-of-range resortID, got $status"
green "     out-of-domain input rejected"

# Assert on the delta, not an absolute total. An absolute holds only against an empty table, so it
# passes on a fresh CI runner and fails on any machine that has ever run a benchmark.
read_vertical() {
  curl -fsS "$BASE_URL/skiers/$RESORT/seasons/$SEASON/days/$DAY/skiers/$SKIER" 2>/dev/null \
    | sed -n 's/.*"totalVertical":\([0-9]*\).*/\1/p'
}
baseline_vertical=$(read_vertical)
baseline_vertical=${baseline_vertical:-0}
target_vertical=$(( baseline_vertical + EXPECTED_VERTICAL ))
[[ "$baseline_vertical" != "0" ]] && info "     (skier $SKIER already has $baseline_vertical ft; expecting $target_vertical)"

info "3/7  ingest"
response=$(curl -fsS -D - -o /dev/null -X POST \
  -H 'Content-Type: application/json' \
  -d "{\"liftID\":$LIFT,\"time\":$TIME_MINUTE}" \
  "$BASE_URL/skiers/$RESORT/seasons/$SEASON/days/$DAY/skier/$SKIER")

echo "$response" | grep -qE 'HTTP/1.1 201|HTTP/2 201' \
  || fail "expected 201 Created, got: $(echo "$response" | head -1)"
event_id=$(echo "$response" | tr -d '\r' | awk -F': ' '/^x-event-id:/ {print $2}')
[[ -n "$event_id" ]] || fail "response carried no x-event-id header"
green "     201 Created, event $event_id"

info "4/7  waiting for the consumer to persist it"
vertical=""
for _ in $(seq 1 60); do
  vertical=$(read_vertical)
  [[ "${vertical:-0}" == "$target_vertical" ]] && break
  sleep 1
done
[[ "${vertical:-0}" == "$target_vertical" ]] \
  || fail "expected totalVertical to reach $target_vertical, got '${vertical:-none}' after 60s"
green "     persisted, totalVertical=$vertical (+$EXPECTED_VERTICAL)"

info "5/7  resort unique-skier count"
count=0
for _ in $(seq 1 30); do
  count_body=$(curl -fsS "$BASE_URL/resorts/$RESORT/seasons/$SEASON/day/$DAY/skiers")
  count=$(echo "$count_body" | sed -n 's/.*"uniqueNumSkiers":\([0-9]*\).*/\1/p')
  [[ -n "$count" && "$count" -ge 1 ]] && break
  sleep 1
done
[[ -n "$count" && "$count" -ge 1 ]] || fail "expected a unique skier count of at least 1, got '$count_body'"
green "     uniqueNumSkiers=$count"

info "6/7  skier resort totals (CS-Index)"
totals=$(curl -fsS "$BASE_URL/skiers/$SKIER/vertical?resort=$RESORT&season=$SEASON")
echo "$totals" | grep -q "\"totalVert\":$target_vertical" \
  || fail "expected totalVert $target_vertical in $totals"
green "     $totals"

info "7/7  pipeline freshness is instrumented"
metrics=$(curl -fsS "$CONSUMER_URL/actuator/prometheus") \
  || fail "consumer metrics are not reachable at $CONSUMER_URL"

# grep -q would SIGPIPE its producer and, under `set -o pipefail`, fail this step on success.
samples=$(echo "$metrics" | sed -n 's/^skier_pipeline_freshness_seconds_count{[^}]*} \([0-9.]*\)$/\1/p')
[[ -n "$samples" ]] || fail "skier_pipeline_freshness_seconds_count is absent from $CONSUMER_URL"
[[ "${samples%%.*}" -ge 1 ]] || fail "freshness histogram is empty ($samples samples) after a durable write"

unstamped=$(echo "$metrics" | sed -n 's/^skier_pipeline_freshness_unstamped_total{[^}]*} \([0-9.]*\)$/\1/p')
skewed=$(echo "$metrics" | sed -n 's/^skier_pipeline_freshness_skewed_total{[^}]*} \([0-9.]*\)$/\1/p')
[[ "${unstamped%%.*}" == "0" ]] \
  || fail "$unstamped durable event(s) carried no x-published-at; the server is not stamping it"
[[ "${skewed%%.*}" == "0" ]] \
  || fail "$skewed freshness sample(s) came out negative; server and consumer clocks disagree"
green "     $samples sample(s), 0 unstamped, 0 skewed"

echo
green "All checks passed."
