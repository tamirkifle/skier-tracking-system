# ADR-0006: Keep exact cardinality as the default, and make it one transaction

Date: 2026-09-07
Status: accepted

## Context

`GET /resorts/{id}/seasons/{s}/day/{d}/skiers` returns the number of distinct skiers at a resort on a
day. Computing it on read means scanning a resort-day partition per request, so it is maintained on
the write path. That requires deciding, per event, whether this skier has already been seen today,
across every consumer instance, and reaching the same answer when the message is redelivered.

Two implementations exist, selectable at runtime via `CARDINALITY_STRATEGY`.

| | `dynamodb` | `redis-hll` |
|---|---|---|
| Accuracy | Exact | +0.24% measured on `PFCOUNT` at 12,000 distinct identities |
| Mechanism | Conditional sentinel put plus `ADD` on the counter row | `PFADD` |
| Writes per event | About 1.9 cardinality write requests | One Redis op, plus one DynamoDB write per distinct skier-day |
| Storage | One row per distinct skier-day | 12 KiB per resort-day at any cardinality |
| Idempotency | Transactional, the conditional write is the coordination primitive | Structural, re-adding an identity cannot move the estimate |
| Durability | DynamoDB | Redis; a flush loses the sketch |

At the benchmark's shape, 100,000 skiers and 200,000 events, roughly 87% of events are repeat
sightings.

## Decision

`dynamodb` stays the default, and its two items go in one `TransactWriteItems`: the conditional `Put`
of the `SkierTracking` sentinel and the `ADD` on the `SkierCounts` row. Either both land or neither
does. A failed update is not settled, so the event is retried, and an exhausted budget dead-letters
it with the ride already stored.

`redis-hll` remains available and mirrors `PFCOUNT` into `SkierCounts` on cardinality growth, so the
read path is unchanged whichever strategy is running.

## Consequences

**Two requests are not exact, and the gap is a step of reasoning rather than a coding error.** A
conditional put proves exactly one writer can *claim* a skier-day. Exactly one increment is a
different statement. A throttle, a timeout or a process death between the put and the update leaves
the claim taken and the count unmoved, and every later sighting then loses the condition and does
nothing. The claim is precisely what prevents the repair, so the undercount is permanent.

**The transaction costs capacity on every event.** DynamoDB bills a transactional write at twice the
ordinary rate: roughly 4 write units for a first sighting against 2, and 2 against 1 for a repeat.
Requests per event fall from two-or-one to one, so a request count understates this arm's cost;
`skier_cardinality_write_items_total` counts items and is the row to divide by.

**A sketch answers "how many" accurately and "is this one new" badly.** `PFCOUNT` reads the whole
register array and stays inside its stated error. `PFADD`'s return value is a different, one-sided
estimator: as the sketch fills, adds of genuinely new identities increasingly change nothing
observable. At 12,000 distinct identities `PFCOUNT` reported +0.24% and the `PFADD`-changed count
-20.2%. The first-sighting meter under `redis-hll` is therefore a documented lower bound feeding a
dashboard, not an alert.

**Why exact is still the default.** The endpoint returns a count of people. If that number ever
informs a lift-capacity decision, a staffing decision or a bill, an estimate is the wrong answer. The
tracking table is also recoverable and auditable, and `RD-Index` exists so the counter can be
recomputed from the events if it drifts. The sketch lives in one process's memory with no
persistence. `redis-hll` stays as a labelled experiment because its idempotency is structurally
better and its storage is flat, and it becomes the right choice the moment the number is a dashboard
figure rather than a reported one.

## Alternatives rejected

- **Keep two requests and add a repair job.** Cheaper per event, and where a large system ends up.
  The repair implies an ingestion pause, which is a bigger operational claim than the transaction's
  capacity cost. `scripts/repair-cardinality.sh` is worth having as defence in depth, not as the
  primary mechanism.
- **Increment first, then claim.** Trades a permanent undercount for a permanent over-count. A lost
  claim can be re-attempted; a surplus increment cannot be identified afterwards.
- **Drop "exact" and document the drift.** This strategy has exactly one advantage over `redis-hll`,
  which is already implemented and considerably cheaper. An arm that is approximate and expensive has
  no reason to be the default.
- **Let the server read the sketch.** It gives the server its own copy of the consumer's strategy
  setting, and a disagreement between the two returns 0 with nothing logged.
- **A process-local set of every skier seen.** Grows without bound, and process-local state cannot
  deduplicate across instances.
- **`DISTINCT` at read time.** No write-path cost, a partition scan per request.
