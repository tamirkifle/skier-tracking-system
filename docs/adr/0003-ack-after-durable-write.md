# ADR-0003: Acknowledge only after the write is durable

Date: 2026-07-31
Status: accepted

## Context

A consumer that acknowledges a delivery as soon as it lands the event on an in-memory queue has told
the broker to delete a message the datastore has never seen. A crash, an out-of-memory kill, a deploy
or an exhausted retry budget then discards everything in flight, silently, because the acknowledgement
can never fail.

## Decision

Carry the settlement right (`DeliveryAck`) from the listener thread to whichever writer thread
persists the event. Acknowledge only after DynamoDB confirms. Nack with requeue on transient failure,
and nack without requeue once the retry budget is spent.

## Consequences

Delivery is at-least-once. The broker holds each message until it is durable and redelivers otherwise.

Duplicates become normal, which is the cost. They are absorbed structurally rather than by a dedup
store: the primary key `(skier, resort, season, day, minute, lift)` makes a repeated write an
overwrite, and the conditional sentinel makes a repeated cardinality update a no-op.

Settlement now happens on a different thread from the one that received the delivery, and RabbitMQ's
`Channel` is not thread-safe. Every frame is emitted under a lock on the channel instance, and
settlement is guarded to happen at most once. A duplicate delivery tag is a fatal channel error in
RabbitMQ and would stall an entire consumer.

The throughput cost is small. The ack is one frame on an already-open channel, and prefetch means the
consumer is never waiting on a single message's settlement.

## Alternatives rejected

**Ack immediately, accept the loss.** Defensible for genuinely disposable telemetry. Not defensible
while the code advertises `ackMode = MANUAL` and a dead-letter queue.

**Transactional outbox.** Write the event and its outbox row in one transaction, then publish
asynchronously. The standard answer when a write and a publish must not diverge. The divergence here
is between the broker and the datastore rather than within one datastore, so an outbox does not apply
without a second store.

**`basicAck(multiple=true)` per batch.** Fewer frames. It requires delivery tags to be settled in
order on a channel, which a pool of writer threads completing out of order cannot guarantee. Not
worth the coupling for a saving that does not show up in any measurement.
