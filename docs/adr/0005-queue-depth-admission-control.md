# ADR-0005: Steer admission from queue depth

Date: 2026-08-01
Status: accepted

## Context

The ingest API has to refuse work the pipeline cannot finish. A fixed rate limit requires knowing the
pipeline's capacity in advance, and that capacity changes with consumer count, DynamoDB provisioning,
and whatever else is happening to the table. The API is also replicated, so a fleet-wide quantity held
per process is wrong by the replica count.

## Decision

A token bucket whose rate is adjusted by a controller that samples RabbitMQ queue depth every 200 ms,
using AIMD. The floor is `skier.admission.min-rate`, default 100 permits/s, refused at startup if out
of range; `MAX_RATE` stays a compiled-in constant.

| Depth | Action |
|---|---|
| >= 200 | `R <- R / 2` |
| > 150 | `R <- R - 1000` |
| < 100 | `R <- R + 10` |

Each server heartbeats a member into a Redis sorted set and reads its cardinality. It divides the
floor and the two additive steps by that count, and leaves the multiplicative branch per instance.

| Branch | Per instance, with N coordinating | Aggregate |
|---|---|---|
| `R <- R / 2` | unchanged | halves |
| `R <- R - 1000` | `-max(1, 1000/N)` | one 1,000 permits/s step |
| `R <- R + 10` | `+max(1, 10/N)` | one 10 permits/s step, until N > 10 |
| floor | `max(1, floor/N)` | the configured floor |

## Consequences

Queue depth is the signal because it is the only one reflecting the whole downstream. Consumer
concurrency, DynamoDB capacity, throttling and network problems all appear as a changing backlog. CPU
on the API reflects none of them; ingest is a validate-and-publish costing microseconds.

The 100:1 asymmetry is deliberate. One overload tick surrenders half the rate; recovering 1,000
permits/s takes 100 ticks. Backing off too far costs throughput briefly, while ramping up too eagerly
re-overloads a draining consumer into an oscillation that is worse.

Halving is scale-free: the sum of N halved rates is half the sum. The `max(1, ...)` matters most on
the increase branch, since `10/N` is zero at N=11 in integer arithmetic, and a step of zero is a
controller that sheds and never climbs back with every signal reading normal.

Redis can succeed and under-count. It runs with no persistence, so it returns from a restart with an
empty sorted set and the first replica to heartbeat reads a cardinality of 1. A decrease is therefore
applied only after `skier.fleet.shrink-confirmations` consecutive refreshes agree, default 3, while
growth applies on the first sighting. An instance that has never completed a refresh does not guess:
it takes the floor's share of an arbitrarily large fleet and withholds readiness.

`skier.queue-monitor.setpoint-mode` also accepts `latency`. Depth stays the default because the
drain-rate estimate's p5-p95 half-span is +/-48% of its mean at a 10 s window, against a deadband of
+/-33%. The mean is stable at every window length; the spread is what fails.

## Alternatives rejected

- **Fixed rate limit.** Wrong the moment anything downstream changes.
- **No admission control.** RabbitMQ answers memory pressure by blocking every publisher on the node,
  turning a consumer problem into a total ingest outage with no 429 and no signal.
- **A Redis-held shared rate.** The obvious carrier, and the one design that breaks: N instances
  applying `R <- R/2` to one value collapse it by 1/2^N. Correcting that means suppressing the
  multiplicative branch everywhere but one instance, which is leader election with extra steps.
- **Leader election.** Admission depends on a lease, and a partitioned follower either keeps its last
  rate or refuses traffic. Sharing the count degrades continuously instead.
- **Falling back to N=1 when Redis is unreachable.** Fails open into the pathology the coordination
  exists to remove, when an operator can see least.
- **Adaptive concurrency limits (Gradient, Vegas).** Better where no queue exists to observe; here one
  does, and its depth measures the protected thing directly.
- **An integral term.** With a coarse actuator and a delayed measurement a PID would mostly amplify
  the noise.
