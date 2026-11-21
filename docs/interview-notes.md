# Interview Notes

## Project Summary

I built a local ecommerce data engineering practice with two pipelines:

1. An offline order warehouse using SQL and SQLite.
2. A realtime Kafka + Flink metric pipeline.

The project focuses on data modeling, deterministic deduplication, reconciliation, event-time processing, Checkpoint and Savepoint behavior.

## Offline Questions

### What does one DWD fact row represent?

One row represents one order item in a valid paid or completed order. Both the order header and order detail are deduplicated before joining.

### How is deduplication made deterministic?

The order header is deduplicated by `order_id` with `ROW_NUMBER()` ordered by update time and a unique identifier. Order details are deduplicated by their detail primary key.

### How are metrics reconciled?

The cleaned source totals are compared with DWD totals, and the ADS GMV and paid order count are checked against the cleaned source aggregation.

## Realtime Questions

### Why use event time?

Event time reflects when the order event occurred. It is stable when events arrive late or out of order.

### How does the Watermark work?

The job allows five seconds of bounded disorder. A later event advances the Watermark and closes eligible windows.

### What does Checkpoint store?

Checkpoint stores operator state, window state and Kafka source progress. The job uses a five-second Checkpoint interval.

### What is the difference between Checkpoint and Savepoint?

Checkpoint is automatic and periodic. Savepoint is user-triggered and is used for controlled stop, restore or upgrade.

### What was verified with Savepoint?

A CANONICAL Savepoint was generated, the original job stopped, and a new job restored from the Savepoint. The JobManager log confirms restoration of the Kafka source split enumerator. The restored job continued consuming new events and producing metrics.

### Why not claim Exactly-Once?

The Kafka sink uses `AT_LEAST_ONCE`. Savepoint restores source state, but output retries may still create duplicate metrics. End-to-end Exactly-Once requires coordination or idempotent downstream writes.

## Limitations

The project uses synthetic, small-scale data and a local single-node environment. It does not include database sinks, production monitoring, high availability or performance tuning at scale.

