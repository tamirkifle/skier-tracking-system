# Liftline: Write-Heavy Event Ingestion Pipeline

A write-heavy ingestion pipeline for ski-lift scan events, built to accept 200,000 events sent as
fast as a client can issue them. It decouples synchronous intake from persistent storage with a
message broker, throttles the front door against downstream queue depth, and loses no events to a
consumer crash.

**Core Stack:** Java 17, Spring Boot, RabbitMQ, DynamoDB (via LocalStack), Redis, Docker Compose,
Prometheus, Grafana

## Key Engineering Outcomes

* **96% reduction in database calls.** Dynamic micro-batching of up to 25 items per flush cut
  DynamoDB write operations from 13,664 to 532, averaging 24.8 items per request.
  [run](benchmarks/results/2026-09-16-ab-state-reset/)
* **Zero event loss under crash faults.** At-least-once delivery validated by issuing `SIGKILL` to
  the consumer at four points during peak drain. 0 of 1,000 in-flight events lost in every case,
  absorbing 41 to 135 redeliveries cleanly through idempotent key design.
  [run](benchmarks/results/2026-09-16-consumer-kill-recovery/)
* **Predictable load shedding.** Under a 3x load spike, from 2,000 to 6,000 req/s offered, accepted
  intake held stable at roughly 100 events/s with early HTTP 429 rejections, preventing a downstream
  cascade. [run](benchmarks/results/2026-07-31-localstack-sweep/)

## System Architecture & Data Flow

```text
POST  ──▶ [ Ingest API x N ] ──▶ [ RabbitMQ ] ──▶ [ Consumer Pool ] ──▶ [ DynamoDB ]
              │                      │                │                     ▲
              ├─ token bucket        ├─ retry queue   └─ batch flush        │
              │  (queue depth)       │  (500 ms TTL)     (<= 25 items)      │
              │                      └─ dead-letter queue                   │
              ▼ 429 + Retry-After                                           │
                                                                            │
GET   ──▶ [ Read API ] ──▶ [ Redis cache-aside ] ──── miss ─────────────────┘
```

1. **Synchronous intake and settlement.** The ingest route,
   `POST /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skier/{skierID}`, publishes to a
   durable RabbitMQ queue and defers its 201 until the broker's publisher confirm
   arrives, so the status code reflects real settlement rather than a successful socket write.
2. **Decoupled asynchronous processing.** A 16-thread consumer worker pool pulls from the queue,
   coalesces records, and flushes up to 25 items per `BatchWriteItem` into DynamoDB.
3. **Cache-aside read path.** Three aggregate endpoints, including `GET /skiers/{skierID}/vertical`,
   read through Redis with per-endpoint TTLs of 10 s, 30 s and 5 min, sized to how fast each value
   changes.

## Technical Highlights & Trade-offs

* **Hotspot-resistant partitioning.** Partitioned the primary table by `skierID` instead of
  `resortID`. DynamoDB caps individual partition throughput at 1,000 WCU, and the benchmark drives
  all traffic at a single resort, so a resort key would have capped the whole write path there. The
  cost is that no resort-scoped question can be answered from the base table.
* **Inherent idempotency via sort key design.** Composite sort keys are formatted
  `resortID#seasonID#dayID#minute#liftID`, and every component of that comes out of the message
  body. A redelivered message rebuilds the same key and overwrites itself, so no deduplication store
  is needed. The lift is in the key for the reason a server-stamped timestamp or a request id could
  not be: it is identical on every delivery of one event and different for two different rides
  ([ADR-0009](docs/adr/0009-lift-in-the-ride-sort-key.md)).
* **Dynamic admission control.** Rather than a static limit, API nodes read RabbitMQ queue depth
  every 200 ms and set a local token bucket rate from it: halve above a depth of 200, step down
  linearly above 150, probe for headroom below 100. Halving stays per replica, since N instances
  halving one shared rate would collapse it by 1/2^N.
* **Poison-pill isolation.** Retriable consumer write failures route to an intermediate retry queue
  with a fixed 500 ms TTL. Attempt counts live in the message header (`x-attempts`) rather than in
  consumer memory, so they survive a worker crash, and a spent budget of five attempts ejects the
  event to a dead-letter queue.
* **Coordinated atomic aggregation.** Unique daily skiers are tracked with an `attribute_not_exists`
  conditional write as the coordination primitive, paired with an atomic increment inside a single
  `TransactWriteItems` call, which avoids a read-modify-write race under concurrency.
* **Bounded read staleness, measured.** A durable write becomes queryable only once the cached key
  expires. At 10 s, 30 s and 300 s of configured TTL that took 10.07 s, 30.13 s and 300.11 s, while
  internal metrics reported the same writes durable within 0.035 to 0.948 s. Read-your-writes is not
  offered. [run](benchmarks/results/2026-08-01-accept-to-queryable/)

## Quickstart

**Requirements:** Java 17+, Docker and Docker Compose

```bash
# 1. Verify dependencies, then start broker, cache, LocalStack, both services, Prometheus, Grafana
make preflight
make up

# 2. Post one event and read it back through every query endpoint
make smoke

# 3. Unit and integration suites
make test
make it

# 4. Fault injection: SIGKILL the consumer mid-drain, then reconcile every published event
make recovery

# Every gate CI runs, in CI's order
make ci-local
```

## Project Structure

* [`Server/`](Server/): REST ingestion API, publisher confirms, adaptive rate limiting.
* [`Consumer/`](Consumer/): batching consumer, retry and dead-letter routing, transactional writers.
* [`Infra/`](Infra/): DynamoDB schema as code, Prometheus and Grafana provisioning.
* [`docs/adr/`](docs/adr/): Architecture Decision Records, with the alternatives rejected and why.
* [`benchmarks/`](benchmarks/): open-loop load generator, fault injection scripts, raw run output.
