# No-checkpoint setup

For the side-by-side tradeoffs and timestamp-start continuous reading, see
[kafka-flink-recovery-methods-comparison](kafka-flink-recovery-methods-comparison.md).

Operator constraint: no checkpoints. These are configuration and operational
instructions, not deployed changes. Actual sink code remains uninspected.
High checkpoint latency/resource cost has not been measured; cost depends on state,
backpressure and sink flushing. This guide respects the constraint regardless.

**Recommendation: committed-offset normal resume plus mandatory conservative repair
replay after failures, through a duplicate-safe sink.** This recovers retained data
without checkpoints, provided the replay covers every possibly undelivered record.
Auto-commit alone is best-effort, not an end-to-end no-loss guarantee.
This assumes the described stateless filter/router jobs. Stateful windows, joins,
timers or aggregations require a separate state-reconstruction design.

## 1. Kafka settings on both source and routed topics

```properties
cleanup.policy=delete
retention.ms=604800000
retention.bytes=-1
```

Seven days is a starting capacity budget: detection + outage + replay + margin must
fit inside it. Catch-up must exceed arrival rate. Provision disk for all retained
replicas and alert on space; no size limit does not mean infinite storage. Increasing
retention does not restore deleted events.

Broker setting on both clusters:

```properties
offsets.retention.minutes=20160
```

Fourteen-day group offset retention is independent of record retention.
Keep stable group IDs unique to each logical job; avoid unrelated consumers using them.
For broker-failure protection use replication factor 3 (requires three brokers),
`min.insync.replicas=2`, `unclean.leader.election.enable=false`, and producer
`acks=all`, `enable.idempotence=true`. Producer idempotence does not deduplicate
application replay after a fresh producer starts. Verify effective topic replica
assignment and min-ISR settings, and check every producer send result. Only records
actually accepted and retained by Kafka can be recovered.

## 2. Flink source for normal resume

Do not call `env.enableCheckpointing(...)`; remove any inherited
`execution.checkpointing.interval` that enables it. Verify runtime checkpointing is
disabled, not merely absent from one configuration file.

KafkaSource builder fragment, with existing bootstrap servers, topic and deserializer:

```java
.setGroupId("stable-unique-id-for-this-logical-job")
.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
.setProperty("enable.auto.commit", "true")
.setProperty("auto.commit.interval.ms", "5000")
.setProperty("commit.offsets.on.checkpoint", "false")
.setProperty("partition.discovery.interval.ms", "60000")
```

The five-second timer stores consumer progress; it does not certify ClickHouse delivery.
It does not limit recovery to five seconds of downtime. A crash can skip fetched but
undelivered records or repeat delivered but uncommitted records. Buffering and sink
stall duration determine possible loss, not just this timer. Reducing the interval
cannot remove this gap.

Use supervised restarts and alerts, with retries sized for the expected outage.
Do not set startup to `latest`. Missing offsets fall back to earliest retained data,
which can replay much more than the outage. Inspect actual startup offsets per partition.

## 3. Sink rules on every hop

ClickHouse inserts must return success before the application treats them as delivered;
fail the task on errors, retry with the same event IDs, and apply backpressure instead
of dropping full queues. Prefer synchronous inserts (`async_insert=0`) for a simple
baseline, with client-side batching to avoid excessive small parts. Tune batch size
and flush delay against measured latency. If async inserts are needed, use `wait_for_async_insert=1`. Neither choice
couples Kafka auto-commits to database writes.

Database acknowledgements alone do not guarantee survival of replica or disk loss.
For that failure scope, use ReplicatedMergeTree-family tables and size insert quorum
for the actual replica topology; fail/retry if quorum is unavailable. Audit distributed
write forwarding and storage durability separately. Topology is unknown here.

Use original globally unique stable event IDs across the router and database. For
fan-out, use a stable composite key including output identity so distinct outputs
are not accidentally deduplicated. `ReplacingMergeTree`
requires identical sorting keys and partition placement for replayed rows. Deduplication
is eventual; correctness-sensitive reads require `FINAL` or equivalent logic.
Incremental materialized aggregates can still count duplicates; handle that separately.

Without checkpoints, Flink KafkaSink `AT_LEAST_ONCE`/`EXACTLY_ONCE` do not supply their
checkpoint-backed guarantees. For a no-checkpoint router, use nontransactional
`DeliveryGuarantee.NONE` with `acks=all` and idempotent producer settings, surface send
failures, and accept that acknowledged source offsets can still outrun destination
writes. Source-topic replay is required after router failures. Preserve the original
ID; a destination Kafka offset is not the original event identity.

## 4. Required recovery procedure

1. Capture per-partition log start and end offsets for each affected topic. Preserve
   these maps with the incident, including cluster/topic identity. Retention must not
   expire the needed records before replay finishes.
2. Resume normal jobs from group offsets. Also run a separate bounded repair job with
   an independent group and explicit starting/ending offset maps; do not reset an active
   normal consumer group. End offsets are exclusive. With no trustworthy delivered
   frontier, replay **all retained records** up to the captured end offsets.
3. Feed replay through the same filters and duplicate-safe writes. Reconcile expected
   original event IDs against the final destination, including expected output multiplicity and content, not just counts or consumer lag.
   If a replay fails, repeat its range; its auto-commits are not proof it finished safely.
4. For router outages, replay the upstream topic through the router first. Allow normal
   downstream consumption, then capture downstream bounds and repair downstream delivery
   as needed. Final reconciliation must cover the whole path and preserve original IDs.
5. Record successful reconciliation and alert if the retention boundary threatens
   unfinished recovery. Also alert on sink errors and missing output while source
   traffic continues, so an alive-but-broken sink triggers repair too.

This is an operational requirement, not an automation already installed in the job.
Full retained-history replay trades recovery CPU/network/database work for avoiding
checkpoint overhead. If that cost is unacceptable, implement a durable delivered
frontier per source partition, advancing only past contiguous completed outputs for
all branches. A highest-seen offset is unsafe. Filtered records and multiple outputs
must be accounted for. Stock auto-commit does not provide this mechanism.

## Optional narrower timestamp repair

Set `message.timestamp.type=LogAppendTime` for future writes only, retaining business
event time in payload. Choose a start before the earliest possibly missed delivery,
including any pre-crash sink stall. This requires evidence; an arbitrary five-minute
lookback is not safe. Existing CreateTime records are unchanged by the topic setting.
Resolve an explicit per-partition map and validate it; Flink 1.17's timestamp initializer
uses log-end for a missing match. Quiet partitions can legitimately have no match.
For ambiguous nonempty partitions, replay earliest retained data. Never assume that
LogAppendTime or a CLI dry-run proves complete delivery.

## Verification and evidence limits

The previous graceful-cancel cases recovered 900/900 outage records. Crash and
no-dedup variants S02K/S21N stopped during setup and have no verdicts. The above is
upstream-supported design guidance, not a measured crash-safe configuration.
Before deployment, kill actual staging TaskManagers under load, inject sink and router
failures, resume plus repair, and compare original event IDs through the final read
path. Require no missing retained events and correct duplicate handling.

## Sources

- [Flink 1.17 Kafka source and sink guarantees](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/)
- [Kafka consumer position and manual offset control](https://kafka.apache.org/38/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [Kafka topic settings](https://kafka.apache.org/38/configuration/topic-configs/)
- [ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/guides/replacing-merge-tree)
- [ClickHouse asynchronous inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts)
