# ADR-0002: Partition `LiftRides` by skier, not resort

Date: 2025-04-20
Status: accepted

## Context

Every read is scoped to a resort, a day, or both. The intuitive partition key is therefore the resort.

## Decision

`PK = skierID`, `SK = resortID#seasonID#dayID#minute#liftID` (ADR-0009). Resort-scoped access goes
through global secondary indexes.

## Consequences

DynamoDB limits a single partition to 1,000 write capacity units. With ten resorts, partitioning by
resort caps the write path at 10,000 writes per second regardless of table provisioning. The
benchmark scenario sends all traffic to one resort, so it would cap it at 1,000 per second. With
100,000 skiers the same load spreads across the full key space and no partition is ever the
constraint.

The price is three GSIs, each of which is a write amplification and a storage cost. Projections are
therefore chosen narrowly: `KEYS_ONLY` where the keys are the answer, `INCLUDE` elsewhere, `ALL`
nowhere.

## Alternatives rejected

**`PK = resortID#dayID`.** Reads become trivial. Writes hit the per-partition ceiling immediately.

**Write sharding, `PK = skierID#hash`.** Sharding exists to spread a hot key, and with 100,000 skiers
there is no hot key to spread. It would also multiply every read by the shard count, since a skier's
rides would be split across shards.
