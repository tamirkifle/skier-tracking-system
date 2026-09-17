# ADR-0009: Put the lift in the ride sort key

Date: 2026-09-16
Status: accepted

## Context

The `LiftRides` sort key was `resortID#seasonID#dayID#timestamp`, where the last component is the
minute of the ski day. That makes the primary key `(skier, resort, season, day, minute)`, and it is
what makes a redelivered message an overwrite instead of a second row.

It also means a skier who rides two different lifts inside the same minute writes one item, not two.
The second ride overwrites the first and its vertical feet are lost. ADR-0007 recorded this as an
accepted cost of the idempotency, on the reasoning that the two properties were the same property.

They are not. The collision comes from the key being too coarse, and the idempotency comes from the
key being a pure function of the message body. A component can be added without touching the
second property, as long as that component is in the payload and identical on every delivery.

## Decision

Append `liftID`:

```
resortID#seasonID#dayID#minute#liftID
```

Rename the attribute to match, since its old name ended in `timestamp` while its value was always a
minute index. Delete the standalone `timestamp` and `seasonID` item attributes, which no read path
ever touched.

## Consequences

`liftID` is in the payload, so it is identical on every redelivery of one event, so two deliveries
still produce one item. Two rides that differ by lift now produce two. `GET /skiers/{id}/vertical`
gets more correct rather than differently correct: it sums rides that were previously overwritten.

The in-batch deferral from ADR-0004 stays, and its reason changes. Two items with the same primary
key still fail a whole `BatchWriteItem` with `ValidationException`, and a broker redelivery can
still put two copies of one event in one batch. What narrows is the population: from any two rides
by a skier in a minute, down to the same event arriving twice.

Deleting the two attributes trades a small amount of readability for one source of truth. A scan
dump now shows `5#2025#1#217#21` rather than a named `timestamp` field, and the attribute name
states the format. In exchange, nothing can drift: a denormalised copy of a key component is a
second place the minute is written, and nothing enforced that the two agreed.

Renaming a key attribute is a table migration in general. It is free here because `make down` runs
`docker compose down -v` and the schema is reapplied from `Infra/SchemaTool` on the next `make up`.
That also means the change is not backward compatible with an existing table, and
`SchemaBootstrap` treats `ResourceInUseException` as success, so it will not migrate one.

No index changes. All three GSIs key on their own attributes, and DynamoDB carries the base key into
every index entry, so entries get a few bytes larger and nothing else moves.

Both A/Bs and the four-point recovery run were re-measured against this key, because the item and
request counts move when collisions stop: `benchmarks/results/2026-09-16-ab-state-reset/` and
`benchmarks/results/2026-09-16-consumer-kill-recovery/`. The 96% request reduction holds, 96.1% and
95.9% across the two arm orders, which is expected: it is a property of batching and not of the key.

## Alternatives rejected

**A server-stamped timestamp.** Breaks idempotency outright. The value differs per delivery, so a
redelivery writes a second item.

**A client-supplied timestamp.** Content-derived in principle. In practice a client retries by
rebuilding the request, which re-evaluates `Instant.now()`, so idempotency becomes contingent on
client discipline the server cannot check.

**`x-event-id`.** Same weakness. ADR-0008 asks the client to reuse it on retry and the server mints
one when it is absent, so it is a request identity rather than a capture identity. Keeping it
unkeyed leaves it available for a dedup store or log correlation later, which is what ADR-0007
recorded it for.

**Keep the standalone `timestamp` attribute for range filters.** It is stored as a String, so a
numeric range filter would compare lexically and sort `"9"` above `"100"`. Making that work means
changing the attribute's type to `N`, which is the same work whether the attribute was deleted
first or not.
