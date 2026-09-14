# Recommended setup

For the side-by-side tradeoffs and timestamp-start continuous reading, see
[kafka-flink-recovery-methods-comparison](kafka-flink-recovery-methods-comparison.md).

Recommendation only; corporate production configuration and ClickHouse sink code
have not been inspected or changed. Apply to every job: router, downstream writer,
and direct writer. Goal: at-least-once delivery with duplicate-safe output.
Checkpoint mode alone does not make an arbitrary sink exactly-once.

## 1. Keep enough Kafka history on both clusters

Starting budget: **7 days**, assuming detection + downtime + catch-up + safety margin
fit within seven days. Increase it when this budget is insufficient. Catch-up must
consume faster than new data arrives; otherwise no finite retention fixes the backlog.
These are topic settings, applied to source AND routed topics:

```properties
cleanup.policy=delete
retention.ms=604800000
retention.bytes=-1
```

`retention.bytes=-1` removes the independent size limit which could delete data before
seven days. Provision disk for measured retained bytes × replication factor, across
all topics, plus operational headroom; alert before disk fills. Seven days is a sizing
choice, not a measured capacity claim. Raising retention cannot recover deleted data.
Do not use compaction when every intermediate event must survive.

Broker setting on both clusters:

```properties
offsets.retention.minutes=20160
```

This keeps inactive group offsets for 14 days; it does not retain event data.
For broker failure durability use replication factor 3, `min.insync.replicas=2`, and
producer `acks=all`, `enable.idempotence=true`, and broker
`unclean.leader.election.enable=false`. Three replicas require three brokers;
the lab's two-broker destination cannot use that configuration without expansion.
Do not lower the durability requirement to keep writes green during lost quorum.

## 2. Enable checkpoints in every Flink job

Configuration baseline for the existing 1.17 API, not an upgrade recommendation:

```yaml
execution.checkpointing.interval: 60 s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.min-pause: 10 s
execution.checkpointing.timeout: 5 min
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.unaligned.enabled: false
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
state.backend: hashmap
state.checkpoint-storage: filesystem
state.checkpoints.dir: <durable-shared-storage-URI>
```

Replace the URI with persistent storage accessible to all Flink processes, with the
required filesystem plugin and credentials. A container-local directory is insufficient.
Preserve stable operator UIDs and compatible state across deployments. The existing
Flink 1.17.0 / Java 17 combination also needs a separately tested supported upgrade;
the lab's JVM workarounds are not a production compatibility guarantee.

KafkaSource builder fragment:

```java
.setGroupId("stable-unique-id-for-this-logical-job")
.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
.setProperty("enable.auto.commit", "false")
.setProperty("commit.offsets.on.checkpoint", "true")
.setProperty("partition.discovery.interval.ms", "60000")
```

A missing group offset replays retained history. During state restoration the saved
per-partition positions take precedence over this startup initializer. Monitor for
retention overtaking required offsets: EARLIEST cannot reconstruct expired records.

Configure and test a restart policy with retries covering the intended outage budget;
a short finite retry policy can leave the job permanently failed. Alert on failure
and exhausted retries. Flink-managed recovery uses
completed checkpoints. A fresh manual submission must explicitly restore:

```text
flink run -s <completed-checkpoint-or-savepoint-URI> <job.jar>
```

This is a template, not a command tested against the corporate cluster. Keep the full
checkpoint directory and shared state. Retaining a checkpoint does not make a fresh
submission automatically discover it. For unattended JobManager/cluster loss recovery,
configure deployment-appropriate Flink HA (ZooKeeper or Kubernetes), durable HA
metadata, and a supervisor/operator that recreates failed processes. Checkpoint storage
alone is not HA. Test total JobManager loss as well as TaskManager loss.
Never use allow-non-restored-state to hide an
unexpected state mapping error.

## 3. Make successful checkpoints mean durable output

Router KafkaSink: use `DeliveryGuarantee.AT_LEAST_ONCE` with checkpoints and durable
producer acknowledgements. It flushes on checkpoints; downstream output must tolerate
replays. Transactional EXACTLY_ONCE is an optional separate design with unique
transaction IDs, timeout sizing and `read_committed` consumers.

ClickHouse sink: fail the task on insert failure. Before acknowledging a checkpoint,
flush and await durable writes, or snapshot every pending record for replay. Never drop
rows on full queues; use backpressure. With async inserts use
`async_insert=1, wait_for_async_insert=1`; waiting for a response alone does not make
an arbitrary asynchronous Flink sink checkpoint-safe. Audit its implementation.

ClickHouse insert acknowledgement does not by itself promise survival of every disk,
power or replica failure. Match replicated-table quorum and storage durability to the
required failure model; these settings need the actual deployment topology.

Carry a stable event ID from the original source through the router. Retries must keep
that ID. Use a duplicate-safe schema and read path. `ReplacingMergeTree` removes equal
sorting keys during background merges, not immediately; use `FINAL` or an equivalent
correct deduplicating query where exact results matter. Keep duplicates in the same
partition and use immutable sorting-key values. Plain counts and incremental materialized
aggregates can still double-count; design them explicitly for replay. Kafka block-insert
deduplication alone is not enough when retries have different batch boundaries.

## 4. Timestamp recovery is a repair tool

For future append-time lookup, optional topic setting:

```properties
message.timestamp.type=LogAppendTime
```

Keep business event time in the payload and configure watermarks from that field if
needed. This changes future writes, not timestamps already retained. It is not required
for checkpoint recovery and does not make timestamp replay exactly-once.

Use a separate bounded repair job when no trustworthy checkpoint remains. Choose a
start before the earliest possibly undelivered event, not merely before process death:
a stalled sink may predate the outage. Resolve and save a map of starting and ending
offsets for every partition, then replay through the same duplicate-safe sink.

In Flink 1.17 timestamp initialization uses the end offset when lookup returns no match.
An end offset is a **review flag, not proof of loss**: empty or quiet partitions can
legitimately have no newer record. For a nonempty ambiguous partition use its earliest
retained offset for conservative recovery. CLI dry-run and Flink initialization are
separate paths: validate the explicit map actually supplied to the replay job.
`--shift-by` and `--to-datetime` also need per-partition validation; neither is inherently
safe. Explicit numeric offsets are valid when supplied as the intended partition map.

## Evidence and corrections

- Existing S02/S02L: 900 outage events recovered in graceful-cancel lab cases.
  This proves that a five-second commit interval does not limit outage recovery to
  five seconds. It does not prove crash safety or that cancel always drains a job.
- Existing S21: 900 outage events recovered with database uniqueness enabled.
  Zero stored duplicates does not prove zero repeated deliveries.
- 2026-09-14 read-only inspection: S02K and S21N have **no verdict.json** and only
  reset logs. Logs show stopped containers and missing topic/group errors. Neither
  experiment produced a crash-loss or no-dedup measurement; no run process was found.
- Auto-commit may skip fetched-but-undelivered records or replay delivered-but-uncommitted
  records after a crash. Loss is not bounded by one commit interval: buffering and sink
  delay matter. No auto-commit setting supplies the requested end-to-end guarantee.
- The original production cause remains unproven without actual startup offsets and
  source/sink evidence. A successful reproduction demonstrates a possible cause only.

## Monitoring

Alert on job termination, checkpoint failures and age of the last completed checkpoint
(start with five minutes for this one-minute schedule), sink insert errors, Kafka disk
pressure, and per-partition lag growth. Alert on missing destination events only when
source traffic is expected. Track the age of the oldest required record against the
retention budget; committed consumer lag alone does not prove destination delivery.

The seven-day budget, checkpoint timings and staging duration are proposed starting
values, not measured production limits. Size them from traffic, recovery throughput
and failure objectives before deployment.

## Acceptance before rollout

In an isolated staging deployment of the actual job and sink: produce known event IDs,
kill a TaskManager under load, keep producing for four hours, restore, and compare IDs
at the final read path. Repeat with sink failures, checkpoint failures and a router
restart. Require zero missing IDs, correct duplicate handling, and backlog clearance
inside the retention budget. Record source offsets, checkpoint IDs and sink outcomes.
The PostgreSQL lab cannot certify the corporate ClickHouse connector.

## Sources

- [Flink 1.17 Kafka connector](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/)
- [Kafka topic configuration](https://kafka.apache.org/38/configuration/topic-configs/)
- [Flink timestamp initializer source](https://raw.githubusercontent.com/apache/flink/release-1.17/flink-connectors/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/source/enumerator/initializer/TimestampOffsetsInitializer.java)
- [ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/guides/replacing-merge-tree)

- [Kafka broker configuration](https://kafka.apache.org/38/configuration/broker-configs/)
- [ClickHouse asynchronous inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts)
