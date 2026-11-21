# Project Architecture

## Offline SQL Warehouse

```mermaid
flowchart LR
    SRC["Orders, details, users, products"] --> ODS["ODS<br/>raw records"]
    ODS --> DWD["DWD<br/>deduplicated order-item facts"]
    DWD --> DWS["DWS<br/>daily user trade summary"]
    DWS --> ADS["ADS<br/>GMV, paid orders, AOV, buyers"]
    ODS -. "quality checks" .-> DQ["Data quality results"]
    ADS -. "count and amount checks" .-> REC["Reconciliation"]
```

## Realtime Metrics

```mermaid
flowchart LR
    EVT["Synthetic order events"] --> K1["Kafka ecommerce_events<br/>3 partitions"]
    K1 --> F["Flink event-time window<br/>5-minute tumbling window<br/>5-second Watermark"]
    F --> K2["Kafka realtime_gmv_metrics<br/>JSON metrics"]
    K2 --> C["Result consumer"]
    F -. "Checkpoint every 5 seconds" .-> CP["Durable checkpoint"]
    F -. "CANONICAL Savepoint" .-> SP["Stop and restore"]
```

## Data Flow

1. Synthetic payment events are produced into `ecommerce_events`.
2. Flink reads the topic and assigns event-time timestamps.
3. A bounded-out-of-orderness Watermark tolerates five seconds of disorder.
4. Payment events are aggregated into five-minute tumbling windows.
5. Each window emits `windowStart`, `windowEnd`, `paidOrders`, `gmv` and `buyers`.
6. KafkaSink writes the JSON result to `realtime_gmv_metrics` with At-Least-Once delivery.
7. Checkpoints store job and source state.
8. A CANONICAL Savepoint was generated, the original job was stopped, and a new job restored the Kafka source state.

