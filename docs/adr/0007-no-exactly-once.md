# ADR-0007: Do not attempt exactly-once delivery

Date: 2026-07-31
Status: accepted

## Context

Acknowledgement happens after the durable write (ADR-0003), which makes duplicate delivery a normal
occurrence rather than an edge case. The next question is whether to suppress duplicates and claim
exactly-once.

## Decision

No. Deliver at-least-once and make the effects idempotent.

## Consequences

Exactly-once delivery between a broker and a datastore requires either a distributed transaction
across both, or a deduplication store consulted on every write. The second is itself a datastore
needing its own delivery guarantees, so the problem recurses rather than terminating.

Idempotent effects give the same observable outcome for far less.

- **Ride writes** are keyed `(skierID, resort#season#day#minute)`, so a duplicate is an overwrite.
- **Cardinality updates** are gated by a conditional sentinel inside a transaction, so exactly one
  increment happens per skier-day regardless of how many deliveries occur.

What this does not cover:

- **Two rides in the same minute on different lifts collide**, and the second overwrites the first.
  The property that makes redelivery safe is the same property that loses that event.
- **A client retry after a timeout** can produce a genuinely new event with a new minute value, which
  is indistinguishable from a real second ride unless the client reuses `x-event-id`.
- The producer stamps `x-event-id` and the consumer reads it, but nothing keys on it. It exists so
  that a future dedup store, or log correlation, has an identity to use.

## Alternatives rejected

**A dedup table keyed on `x-event-id`.** One extra conditional write per event, plus TTL management,
plus a decision about what to do when the dedup write succeeds and the ride write fails. It buys
nothing the primary key does not already provide for this schema.

**Publisher confirms plus consumer transactions.** AMQP transactions are per-channel and do not span
DynamoDB, so this gives exactly-once within the broker and nothing across the boundary that matters.

**Claim exactly-once anyway.** The most common option in practice, and the reason the phrase is
mistrusted.
