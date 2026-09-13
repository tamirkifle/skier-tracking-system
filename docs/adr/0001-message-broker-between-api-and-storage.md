# ADR-0001: Put a message broker between ingest and storage

Date: 2025-04-11
Status: accepted

## Context

The ingest endpoint receives lift-ride events at a high, bursty rate. The obvious implementation
writes to DynamoDB inside the request handler.

## Decision

Publish to RabbitMQ and return. A separate consumer performs the write.

## Consequences

The API's availability and latency stop being a function of the datastore's. A throttled table
becomes a growing queue rather than an ingest outage. Burst absorption becomes the broker's disk
rather than the table's provisioned write capacity. The write path can also be scaled, tuned and
redeployed independently of the read path.

Read-your-writes is gone. A 201 means the broker owns the event, not that it is queryable. That is a
real API weakening, and it is stated in the endpoint's OpenAPI description rather than left for a
client to find out.

It adds an operational component, and it makes delivery semantics something that has to be reasoned
about explicitly. See ADR-0003 and ADR-0007.

## Alternatives rejected

**Synchronous write.** Simplest, and correct for a low-rate service. Rejected because the workload's
defining feature is a write rate that outruns what a single table absorbs smoothly, and because it
makes every DynamoDB hiccup a client-visible 5xx.

**Write to a local buffer and flush.** No broker to operate, but the buffer is process memory. A
crash loses it, and there is no way to add consumers.

**Kinesis or DynamoDB Streams.** Streams would give ordered, replayable ingest with no broker to run.
Rejected because the events have no ordering requirement, and because a lease-based shard worker is a
heavier thing to reason about than a queue with a dead-letter queue. Worth revisiting if replay from
an arbitrary point ever becomes a requirement.
