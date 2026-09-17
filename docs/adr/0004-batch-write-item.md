# ADR-0004: Coalesce writes into `BatchWriteItem`

Date: 2026-07-31
Status: accepted

## Context

One `PutItem` per event means 200,000 HTTP requests for the benchmark's 200,000 events, each with its
own SigV4 signature and round trip.

## Decision

Accumulate up to 25 events, the API's hard limit, and write them in one `BatchWriteItem`. Keep the
single-put writer, selectable via `WRITER_MODE`, as the benchmark baseline.

## Consequences

It does not reduce cost. DynamoDB charges write capacity per item, not per request. What batching
removes is per-request overhead: 8,000 requests instead of 200,000.

It adds up to `WRITER_LINGER_MS` (default 20 ms) to an event's end-to-end latency while a partial
batch waits to fill. That is acceptable because the pipeline is already asynchronous and its lag is
measured in hundreds of milliseconds. The linger applies only once at least one event is in hand, so
an idle consumer never adds latency to the first event of a burst.

Two API behaviours have to be handled explicitly, and both are easy to get wrong.

- **Partial success is normal, not exceptional.** `BatchWriteItem` returns HTTP 200 with an
  `UnprocessedItems` map when individual items throttle. Code that only watches for thrown exceptions
  drops those items silently. They are mapped back to their originating events and returned for retry.
- **Duplicate keys in one request fail the whole request.** Two items with the same primary key
  produce a `ValidationException` for the entire batch. Since ADR-0009 the sort key ends in the lift,
  so what collides is a redelivered copy of one event rather than two different rides: rare per
  event, and guaranteed somewhere across 200,000. Colliding events are deferred to a later batch
  rather than allowed to poison the current one.

Keeping both writers is what makes the comparison a measurement rather than an assertion. Same jar,
same table, same seeded workload, one variable.

## Alternatives rejected

**`TransactWriteItems` for the ride write.** It would let the ride write and the cardinality update be
atomic. Rejected because transactional writes cost twice as much, cap at 100 items, and defeat
batching. The ride write is already idempotent on its primary key, so it needs no transaction. The
cardinality pair does need one, for reasons that are specific to it; see ADR-0006.

**DynamoDB Streams to maintain counts.** Move cardinality off the write path and derive it from the
change stream. It removes two round trips per first sighting and makes the counter a pure function of
the table. Rejected because it adds a Lambda or a KCL consumer to operate, and because stream
processing has its own at-least-once semantics that would need the same sentinel to be idempotent.
Recorded as the natural next step.
