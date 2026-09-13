# ADR-0008: Answer the ingest POST on the publisher confirm, and add a status for "unknown"

Date: 2026-09-07
Status: accepted

## Context

`RabbitTemplate.convertAndSend` returns when the frames are written to the channel. The broker can
still nack, refuse the message under a disk alarm, or discard a publish that matches no binding. A
handler answering 201 on return has claimed a delivery in all three cases, and enabling confirms does
not change that: the callbacks fire after the response has been sent.

Two protocol details decide the shape of the fix. An unroutable mandatory publish is **returned and
then acked**, so a wait that reads only the confirm reports a discarded message as accepted. And Boot
applies the `spring.rabbitmq.template.*` namespace only to a template it configures, so
`mandatory=true` beside a hand-constructed `new RabbitTemplate(connectionFactory)` is dead text.

## Decision

`LiftRidePublisher` publishes with a `CorrelationData` and blocks on its future for up to
`skier.ingest.confirm-timeout-ms`, default 5 s. It checks `getReturned()` before the confirm, and
reports one of three outcomes.

| Outcome | Status | What it says to a client |
|---|---|---|
| Acked, not returned | 201 | The broker owns the event. Stop retrying. |
| Nacked, returned, or the send threw | 503 | The event is not queued. Retrying cannot duplicate it. |
| No confirm before the timeout | 504 | Nobody knows. Retry with the returned `x-event-id`. |

The 504 is the substance of this decision. A timeout is not evidence of non-delivery, since the
publish may be confirmed a millisecond later. Folding it into the 503 invites a retry that duplicates
the event; folding it into the 201 claims a delivery nobody observed. For the retry to be the same
event, the identity has to be the client's: `x-event-id` is accepted inbound, bounded to 64
characters of `[A-Za-z0-9._:-]`, and generated when absent.

The template is built through Boot's `RabbitTemplateConfigurer`, which makes that property namespace
live. The publisher refuses to start when the connection factory has confirms or returns disabled:
without correlated confirms the future never completes and every publish times out into a 504.

## Consequences

The endpoint's latency now includes a broker round trip, bounded below by one round trip per request
and not otherwise known.

`skier.ingest.publish.unknown` is a separate counter from `skier.ingest.publish.failed` and must stay
separate. Summed into one error rate they hide the only case in which a client retry can duplicate an
event. A client that retries a 504 without reusing `x-event-id` still duplicates; the server can only
make the correct behaviour available and say so in the response body.

## Alternatives rejected

**Keep 201-on-send and document the gap.** A client that cannot tell "queued" from "silently
discarded" has no correct retry policy, whatever the docs say.

**Return 202 instead of 201.** It reads more accurately and fixes the wrong thing. The problem was
never which 2xx; it was a 2xx for a message the broker rejected.

**Use `waitForConfirms`, the simple confirm mode.** Less code. It confirms the channel rather than a
message, so under concurrency a request would block on other requests' publishes and could not
attribute a nack to its own event.

**Publish asynchronously and reconcile later, through an outbox or a status endpoint.** The right
answer where holding a request thread for a broker round trip is unaffordable. Here every in-flight
publish holds a Tomcat thread for up to the confirm timeout, which is why it is 5 s and not 30.

**Alert on the unknown counter.** No run has yet produced a 504, so any threshold would be invented.
