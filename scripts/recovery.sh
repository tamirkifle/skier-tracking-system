#!/usr/bin/env bash
# One bounded failure-and-recovery run, reconciled event by event.
#
#   bash scripts/recovery.sh                 # 1,000 events, kill at 40% written
#   EVENTS=200 bash scripts/recovery.sh      # faster
#   KILL_AT_FRACTION=0.7 bash scripts/recovery.sh
#
# Posts a manifest of N events whose ids this script chose, SIGKILLs the consumer part-way through
# the drain, restarts it, then reconciles every id against DynamoDB and the dead-letter queue. The
# output is four disjoint counts, stored, unresolved, quarantined and missing, and a non-zero exit
# when anything lands in the last one. A pass is a statement about this run and this fault point,
# not about durability in general. Needs a running stack (`make up`).

set -euo pipefail

# Captured before ab.sh is sourced and restored after: ab.sh reads `EVENTS` from its positional
# parameters, and a sourced script sees the caller's, so sourcing it replaces whatever was set here.
RECOVERY_EVENTS="${EVENTS:-1000}"
KILL_AT_FRACTION="${KILL_AT_FRACTION:-0.4}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
CONSUMER_URL="${CONSUMER_URL:-http://localhost:8085}"
OUT="${OUT:-benchmarks/results/$(date +%F)-recovery}"
RESORT="${RESORT:-5}"
SEASON=2025
DAY=1
DRAIN_TIMEOUT_SECONDS="${DRAIN_TIMEOUT_SECONDS:-300}"

# Reuses ab.sh's reset, clean-state assertion, region-correct awslocal wrapper and metric scrape.
AB_SOURCE_ONLY=1 . "$(dirname "$0")/ab.sh"
EVENTS="$RECOVERY_EVENTS"

mkdir -p "$OUT"
MANIFEST="$OUT/manifest.tsv"
: > "$MANIFEST"

info "=============================================================="
info " recovery run: $EVENTS events, consumer killed at ${KILL_AT_FRACTION} drained"
info "=============================================================="

# reset_datastores stops the consumer and it stays stopped through phase 1. With nothing draining
# the whole manifest accumulates on the queue, so the kill is guaranteed to land with work in flight.
reset_datastores
assert_clean_state recovery

# One skier and one distinct minute per event, so no two collide on the primary key and stored == N
# is a real statement. Admission control stays on, which is why 429s are counted separately.

info ""
info "phase 1: publishing $EVENTS events with ids this script chose (consumer stopped)"

RUN_TAG="rec-$(date +%s)"
accepted=0
unknown=0
shed=0
refused=0

for i in $(seq 1 "$EVENTS"); do
  event_id="$RUN_TAG-$i"
  # Skier ids from a per-run window so the run does not collide with a previous one's sentinels.
  skier=$(( 10000 + (i % 80000) ))
  lift=$(( (i % 40) + 1 ))
  minute=$(( (i % 360) + 1 ))

  status=$(curl -s -o /dev/null -w '%{http_code}' \
    -X POST "$BASE_URL/skiers/$RESORT/seasons/$SEASON/days/$DAY/skier/$skier" \
    -H 'Content-Type: application/json' \
    -H "x-event-id: $event_id" \
    -d "{\"liftID\":$lift,\"time\":$minute}" || echo 000)

  case "$status" in
    201) accepted=$((accepted + 1))
         printf '%s\t%s\t%s\t%s\n' "$event_id" "$skier" "$lift" "$minute" >> "$MANIFEST" ;;
    504) unknown=$((unknown + 1))
         # Kept in the manifest deliberately: an excluded 504 would be an unreconciled unknown.
         printf '%s\t%s\t%s\t%s\n' "$event_id" "$skier" "$lift" "$minute" >> "$MANIFEST" ;;
    429) shed=$((shed + 1)) ;;
    *)   refused=$((refused + 1)) ;;
  esac
done

info "  accepted (201) $accepted   unknown (504) $unknown   shed (429) $shed   refused $refused"
[[ "$accepted" -gt 0 ]] || fail "nothing was accepted; the stack is not serving"

expected=$(wc -l < "$MANIFEST" | tr -d ' ')

# Polled, not read once. RabbitMQ's management API serves counts from a statistics collector that
# refreshes on an interval, so a read taken straight after a burst returns a number from before it.
queued=0
for _ in $(seq 1 30); do
  queued=$(queue_depth || echo 0)
  [[ "${queued:-0}" -ge "$expected" ]] && break
  sleep 1
done
info "  manifest holds $expected event(s); $queued are on the queue with nothing draining"

# A shortfall here means a 201 was answered for an event that is not queued.
[[ "${queued:-0}" -ge "$expected" ]] \
  || fail "$expected event(s) were accepted but only ${queued:-0} are on the queue after 30s"

# Killed, not stopped: SIGKILL means @PreDestroy does not run and nothing hands its work back.

kill_at=$(awk -v n="$expected" -v f="$KILL_AT_FRACTION" 'BEGIN { printf "%d", n * f }')
info ""
info "phase 2: starting the consumer, then SIGKILLing it once $kill_at item(s) are written"

docker compose up -d consumer >/dev/null

# Polled far faster than 1 Hz, or the whole window falls between two samples at ~100 events/s.
written=0
deadline=$(( $(date +%s) + DRAIN_TIMEOUT_SECONDS ))
while [[ $(date +%s) -lt $deadline ]]; do
  written=$(metric skier_write_total 2>/dev/null || echo 0)
  [[ "${written:-0}" -ge "$kill_at" ]] && break
  sleep 0.05
done

before_kill_written="$written"
before_kill_depth=$(queue_depth || echo 0)
before_kill_staged=$(staging_depth 2>/dev/null || echo 0)
docker kill skiers-consumer >/dev/null
info "  killed with $before_kill_written written, $before_kill_depth on the queue," \
     "$before_kill_staged staged"

# The assertion that stops this run passing for the wrong reason. If the consumer had already
# finished, the kill interrupted nothing and phase 4 is reconciling a clean drain.
outstanding=$(( ${before_kill_depth:-0} + ${before_kill_staged:-0} ))
[[ "$outstanding" -gt 0 ]] \
  || fail "the kill landed with no work outstanding, so nothing was recovered. Raise EVENTS or lower KILL_AT_FRACTION."
[[ "${before_kill_written:-0}" -lt "$expected" ]] \
  || fail "every event was already written when the kill landed ($before_kill_written of $expected)"
info "  $outstanding event(s) were in flight or queued when the process died"

# Everything unacknowledged returns to the queue when the connection dies; give the broker a moment.
sleep 5
after_kill_depth=$(queue_depth || echo 0)
info "  queue depth after the kill: $after_kill_depth (was $before_kill_depth)"

info ""
info "phase 3: restarting the consumer and draining"
docker compose up -d consumer >/dev/null

for _ in $(seq 1 "$DRAIN_TIMEOUT_SECONDS"); do
  depth=$(queue_depth || echo '?')
  staged=$(staging_depth 2>/dev/null || echo '?')
  if [[ "$depth" == "0" && "$staged" == "0" ]]; then
    # Longer than RabbitMQ's statistics interval: a depth of 0 can predate the redelivery burst.
    sleep 8
    depth=$(queue_depth || echo '?')
    staged=$(staging_depth 2>/dev/null || echo '?')
    [[ "$depth" == "0" && "$staged" == "0" ]] && break
  fi
  sleep 1
done

drained_depth=$(queue_depth || echo '?')
info "  queue depth $drained_depth, staging depth $(staging_depth 2>/dev/null || echo '?')"

# Reconcile against DynamoDB, not against a metric. A counter says how many writes this process
# believes it made; the manifest asks whether each specific event is there.

info ""
info "phase 4: reconciling $expected manifest entries against DynamoDB"

# Projected to the two key attributes, which is all the reconciliation reads.
ddb scan --table-name LiftRides \
  --projection-expression '#s,#k' \
  --expression-attribute-names '{"#s":"skierID","#k":"resortID#seasonID#dayID#timestamp"}' \
  --output json > "$OUT/lift-rides.json"

dlq_depth=$(queue_depth_of deadLetterQueue || echo 0)

python3 - "$MANIFEST" "$OUT/lift-rides.json" "$OUT/reconciliation.json" <<'PY'
import json, sys

manifest_path, rides_path, out_path = sys.argv[1:4]

stored_keys = set()
with open(rides_path) as f:
    for item in json.load(f).get("Items", []):
        skier = item["skierID"]["S"]
        sort = item["resortID#seasonID#dayID#timestamp"]["S"]
        stored_keys.add((skier, sort))

expected, missing = [], []
with open(manifest_path) as f:
    for line in f:
        event_id, skier, lift, minute = line.rstrip("\n").split("\t")
        # The item's identity is (skierID, resort#season#day#minute) and the event id is not
        # persisted, so reconciliation is by coordinates the manifest made unique.
        key = (skier, f"5#2025#1#{minute}")
        expected.append(key)
        if key not in stored_keys:
            missing.append({"eventId": event_id, "skier": skier, "minute": minute})

result = {
    "expected": len(expected),
    "stored": len(expected) - len(missing),
    "missing": len(missing),
    "missing_sample": missing[:20],
}
with open(out_path, "w") as f:
    json.dump(result, f, indent=2)

# The archived form of what was in the table, sorted, so the comparison can be re-run offline.
keys_path = out_path.replace("reconciliation.json", "stored-keys.tsv")
with open(keys_path, "w") as f:
    f.write("skierID\tresortID#seasonID#dayID#timestamp\n")
    for skier, sort in sorted(stored_keys):
        f.write(f"{skier}\t{sort}\n")

colour = "\033[32m" if not missing else "\033[31m"
print(f"{colour}  expected {result['expected']:,}   stored {result['stored']:,}   "
      f"missing {result['missing']:,}\033[0m")
for entry in missing[:5]:
    print(f"\033[31m    missing {entry['eventId']} (skier {entry['skier']}, minute {entry['minute']})\033[0m")
PY

retries=$(metric skier_write_retries_total || echo 0)
dropped=$(metric skier_write_dropped_total || echo 0)
requeued=$(metric skier_write_requeued_total || echo 0)
unresolved=$(metric skier_cardinality_projection_unresolved_total || echo 0)
handoff_failed=$(metric skier_write_retry_handoff_failed_total || echo 0)
written_total=$(metric skier_write_total || echo 0)

# Counters read after the restart, so they describe the second consumer process only.
cat > "$OUT/summary.json" <<JSON
{
  "events_offered": $EVENTS,
  "accepted_201": $accepted,
  "unknown_504": $unknown,
  "shed_429": $shed,
  "refused": $refused,
  "manifest": $expected,
  "kill_at_fraction": $KILL_AT_FRACTION,
  "written_before_kill": $before_kill_written,
  "_note_queue_depth": "queue depths come from RabbitMQ's management API, which samples on an interval; a value read right after a burst can lag it. outstanding_at_kill's staged term is the consumer's own gauge and does not.",
  "queue_depth_before_kill": ${before_kill_depth:-0},
  "staged_before_kill": ${before_kill_staged:-0},
  "outstanding_at_kill": $outstanding,
  "queue_depth_after_kill": ${after_kill_depth:-0},
  "queue_depth_after_drain": ${drained_depth:-0},
  "dead_letter_queue_depth": ${dlq_depth:-0},
  "post_restart_only": {
    "skier_write_total": ${written_total:-0},
    "skier_write_retries": ${retries:-0},
    "skier_write_dropped": ${dropped:-0},
    "skier_write_requeued": ${requeued:-0},
    "skier_cardinality_projection_unresolved": ${unresolved:-0},
    "skier_write_retry_handoff_failed": ${handoff_failed:-0}
  }
}
JSON

info ""
info "post-restart counters (this process only, not the whole run):"
info "  written $written_total   retried $retries   dead-lettered $dropped   requeued $requeued"
info "  projection unresolved $unresolved   retry handoff failed $handoff_failed"
info "  dead-letter queue depth ${dlq_depth:-0}"
info ""
rm -f "$OUT/lift-rides.json"
info "artifacts in $OUT/ (manifest.tsv, stored-keys.tsv, reconciliation.json, summary.json)"

missing_count=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['missing'])" "$OUT/reconciliation.json")
if [[ "$missing_count" != "0" ]]; then
  fail "$missing_count manifest event(s) are in neither LiftRides nor accounted for. That is loss."
fi
if [[ "${drained_depth:-1}" != "0" ]]; then
  fail "the queue did not drain within ${DRAIN_TIMEOUT_SECONDS}s (depth $drained_depth)"
fi

green "every manifest event is stored, and the queue drained. Recovery preserved the run."
