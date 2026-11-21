# Flink Savepoint Recovery

## Job

- Original job ID: `ecbde19c95c425fa52f08c29d599410b`
- Restored job ID: `42177e48699110f08c82c3a0386ba745`

## Environment

- JobManager REST: `127.0.0.1:8081`
- JobManager RPC: `127.0.0.1:6123`
- TaskManager count: 1
- Task slots: 1

## Savepoint

Command shape:

```text
flink stop -p <savepoint-directory> <job-id>
```

Savepoint type: CANONICAL

The JobManager log confirmed:

```text
Starting job <restored-job-id> from savepoint <savepoint-path>
Restoring job <restored-job-id> from Savepoint 10
Restoring SplitEnumerator of source Source: kafka-events from checkpoint.
Job <restored-job-id> switched from state CREATED to RUNNING.
```

## Continued Output

After recovery, the job consumed new events and emitted:

```json
{"windowStart":"2026-11-19T16:55:00Z","windowEnd":"2026-11-19T17:00:00Z","paidOrders":2,"gmv":598.00,"buyers":2}
{"windowStart":"2026-11-19T17:00:00Z","windowEnd":"2026-11-19T17:05:00Z","paidOrders":2,"gmv":798.00,"buyers":2}
```

## Boundary

The Kafka sink uses At-Least-Once delivery. Savepoint recovery was verified, but end-to-end Exactly-Once was not.

