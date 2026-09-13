# Architecture decision records

One file per decision that was not obvious, dated, with the alternatives that were rejected and why.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-message-broker-between-api-and-storage.md) | Put a broker between ingest and storage | Accepted |
| [0002](0002-skier-as-partition-key.md) | Partition `LiftRides` by skier, not resort | Accepted |
| [0003](0003-ack-after-durable-write.md) | Acknowledge only after the write is durable | Accepted |
| [0004](0004-batch-write-item.md) | Coalesce writes into `BatchWriteItem` | Accepted |
| [0005](0005-queue-depth-admission-control.md) | Steer admission from queue depth | Accepted |
| [0006](0006-cardinality-strategy.md) | Keep exact cardinality as the default, and make it one transaction | Accepted |
| [0007](0007-no-exactly-once.md) | Do not attempt exactly-once delivery | Accepted |
| [0008](0008-confirmed-acceptance.md) | Answer the ingest POST on the publisher confirm | Accepted |
