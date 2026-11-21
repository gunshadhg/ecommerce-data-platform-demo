# Run Summary

Validation date: 2026-11-19

## Environment

- JDK: Amazon Corretto 17.0.21.1
- Kafka: 3.9.1 in KRaft mode
- Flink: 1.20.1
- Input topic: `ecommerce_events`
- Output topic: `realtime_gmv_metrics`
- Topic partitions: 3
- Replication factor: 1

## Results

- Kafka Producer and Consumer worked.
- Flink consumed events from the input topic.
- The job emitted five-minute event-time metrics.
- Metrics were written to the output topic as JSON.
- 16 result records parsed successfully as JSON.
- Invalid JSON records: 0.
- Flink standard error log length: 0 bytes.
- Successful Checkpoint count observed: 124.
- Savepoint type: CANONICAL.
- The job restored from Savepoint and continued producing metrics.

## Sample Metrics

```json
{"windowStart":"2026-11-19T16:55:00Z","windowEnd":"2026-11-19T17:00:00Z","paidOrders":2,"gmv":598.00,"buyers":2}
{"windowStart":"2026-11-19T17:00:00Z","windowEnd":"2026-11-19T17:05:00Z","paidOrders":2,"gmv":798.00,"buyers":2}
```

## Not Validated

- End-to-end Exactly-Once semantics
- Database or Redis sinks
- Production high availability
- Large-scale performance tuning

