# Three recovery approaches, plus timestamp-start continuous reading

For the described Flink 1.17 Java jobs: Kafka → filter/router → Kafka or ClickHouse.
Examples are setup templates, not corporate deployments or compiled application code.
Use existing bootstrap servers, authentication and deserialization alongside the fragments.
The actual sink and production failure cause remain unverified.

**Yes: set only a starting timestamp and the job can consume history, catch up, and
continue processing new events.** See method 4. A timestamp is translated into an
offset for each partition; an offset itself is not a time.

These are not mutually exclusive mechanisms. Timestamp and committed-offset initializers
choose where a fresh source starts. Checkpoints retain progress and operator state for
recovery. A timestamp-start job can also checkpoint; a checkpointed job can use
committed-earliest when launched without restored state.

## Tradeoffs

Here “committed-earliest” means **no checkpoints + auto-commit + committed offsets,
with EARLIEST fallback**, matching the operator's proposed setup.

| Dimension | 1. Bounded interval replay | 2. Checkpoint recovery | 3. Committed-earliest, no checkpoints |
|---|---|---|---|
| Purpose | Repair or reprocess a selected historical range | Resume processing and operator state after failure | Resume near the consumer's last stored position |
| Starting point | Explicit partition offsets resolved from chosen times | Completed checkpoint; initializer applies without restored state | Each partition's committed offset; earliest retained if missing/invalid under fallback |
| Stopping point | Fixed exclusive end offset per partition | Continues with new events | Continues with new events |
| Four-hour outage | Select a range covering all possibly missed output, then repair | Restore and consume retained backlog automatically under configured recovery | Reads four-hour backlog if commits stayed at the pre-outage position |
| Crash loss | Can repair retained events if range and sink are correct; a failed replay must resume safely or repeat | Consistent source/state recovery; output guarantee depends on sink integration | Can skip fetched-but-undelivered records; no fixed five-second loss bound |
| Duplicates | Expected when overlapping previous deliveries; use duplicate-safe output | At-least-once sinks may replay; exactly-once output requires compatible sink protocol | Possible when output outruns commits; use duplicate-safe output |
| Steady resource cost | Little extra until replay; concurrent repair adds broker, network and sink load | Snapshot CPU/I/O/storage; aligned barriers can wait under backpressure | Periodic offset commit requests; no state snapshots |
| Latency | Main job can remain live, but shared-resource contention may slow it | Depends on state, alignment and sink; transactional visibility may wait for commit | No checkpoint barrier overhead, but batching/network/sink still govern latency |
| Recovery cost | Reads selected history, plus overlap; repeated repair costs again | Restores state and reprocesses since last completed checkpoint | Usually small replay; repair can require all retained history |
| Operations | Choose and validate range, limit repair load, reconcile output | Durable storage, recovery/HA configuration, checkpoint monitoring | Simple startup; stronger monitoring and separate repair procedure needed |
| Stateful jobs | Historical range alone may lack earlier window/join context | Restores supported operator state | Kafka positions do not reconstruct Flink state |
| Best fit | Targeted repair alongside a running main job | Automatic consistent recovery | Stateless workloads accepting best-effort resume plus repair |

**No method recovers Kafka records already deleted.** A zero-lag consumer group does
not prove ClickHouse received every event. Checkpoint cost is not proven “huge” here:
measure state bytes, checkpoint duration, alignment, CPU and end-to-end latency.
A one-minute checkpoint interval does not generally delay every record by one minute;
transactional output visibility is a separate consideration.

## Shared prerequisites

Apply retention to both original and routed topics. Starting budget, not measured sizing:

```properties
# Topic settings
cleanup.policy=delete
retention.ms=604800000
retention.bytes=-1
# Broker setting, separately
offsets.retention.minutes=20160
```

Seven days of records and fourteen days of inactive group offsets. Provision disk for
all replicas and headroom. Detection + outage + catch-up + safety margin must fit
within record retention; processing must eventually exceed the incoming rate.
Removing the byte limit prevents earlier size-based deletion but cannot prevent full disks.

Require acknowledged writes, surfaced sink errors, backpressure and stable original
event IDs through both hops. For fan-out include output identity in the dedup key.
ClickHouse `ReplacingMergeTree` is eventual: correctness-sensitive queries need `FINAL`
or equivalent deduplication. Incremental materialized aggregates need separate replay
handling. Quorum/storage durability depends on the actual deployment. Full sink and
Kafka replication requirements: [kafka-flink-recovery-settings](kafka-flink-recovery-settings.md).

## How much history does each partition contain?

A partition is an ongoing log, not a fixed time bucket. Partition 0 does not hold
Monday while partition 1 holds Tuesday. Both can receive events throughout the day;
producer routing and traffic determine their individual volumes. No setting assigns
a fixed number of events or hours to each partition.

### Settings and defaults

These are **Apache Kafka 3.8 defaults**, not verified values from your cluster.
Topic overrides inherit broker defaults when absent. Managed services can differ.

| Topic setting | Default | Effect |
|---|---|---|
| `retention.ms` | `604800000` (7 days) | Time retention for deletion |
| `retention.bytes` | `-1` (unlimited) | Size retention **per partition**; may shorten history |
| `cleanup.policy` | `delete` | `compact` can remove older values for a key |
| `segment.ms` | `604800000` (7 days) | Segment rolling by time |
| `segment.bytes` | `1073741824` (1 GiB) | Segment rolling by size |
| `message.timestamp.type` | `CreateTime` | Producer timestamp; `LogAppendTime` uses broker time |

Deletion works on segments, not exact per-record expiry. Retention is not a promise
that every partition has exactly seven days of records. Segment rolling and cleanup
scheduling affect deletion timing. `segment.ms` is not the replay duration.
Time-based deletion uses segment timestamps: skewed producer `CreateTime` can
shorten or extend retention relative to arrival time. Verify producer clocks and
timestamp validation, or consider `LogAppendTime` for future ingestion-time history.
Changing the setting does not rewrite existing timestamps.
`offsets.retention.minutes` retains consumer progress, not topic events.
These configuration facts come from [Kafka topic settings](https://kafka.apache.org/38/generated/topic_config.html)
and [broker settings](https://kafka.apache.org/38/generated/kafka_config.html).

Keep the seven-day starting budget above, with sufficient disk, then size it against
maximum outage + detection delay + repair duration + margin. For a four-hour outage,
a four-hour retention budget is too tight. Confirm effective topic and inherited
broker configuration through your Kafka admin tooling; a topic with no explicit
overrides still has retention settings.

**Example, steady traffic:** with a 10 GiB per-partition size cap, a partition receiving
2 GiB/hour holds roughly five hours; one receiving 0.01 GiB/hour reaches its seven-day
time limit first. This is a sizing estimate: segment boundaries, compression and
traffic changes affect the actual result. All partitions share the topic policy,
but their available histories can differ.

### Measure offsets, timestamps and counts separately

Use an independent diagnostic consumer with auto-commit disabled and the same
transaction isolation as the reader being assessed. Do not move the live group's commits.

1. Discover all partitions; capture `beginningOffsets` and `endOffsets` for each.
2. Scan each captured `[beginning, end)` range to inspect returned records and count
   them. Keep these bounds fixed; handle retention moving the beginning as a failed
   measurement requiring a new snapshot. Empty polls alone do not prove completion.
3. Record first/last readable offsets and their timestamps, plus minimum/maximum
   timestamps. With producer timestamps, first/last offsets need not contain the
   oldest/newest times. Payload event time needs a separate measurement.

The offset difference is a **span, not an exact event count**: compaction, transaction
markers and transaction visibility can create gaps. `read_committed` end offsets use
the last stable offset; ongoing transactions can hide later data. Timestamp lookup
returns the first offset with timestamp at least the target, or no match. See the
[Kafka consumer API](https://kafka.apache.org/38/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).

Illustrative snapshot, all times UTC on the same date:

| Partition | Beginning | Exclusive end | Offset span | Observed timestamp range |
|---|---:|---:|---:|---|
| 0 | 100 | 160 | 60 | 06:00–10:00 |
| 1 | 800 | 820 | 20 | 09:00–09:55 |
| 2 | 0 | 0 | 0 | Empty at capture |

Partition 1's shorter range alone cannot tell you whether earlier events expired
or producers only started at 09:00. Even a 06:00–10:00 range does not prove every
expected event exists between those times; reconcile producer IDs/counts if needed.

### Count events inside a chosen interval

For `[08:00, 09:00)`, resolve both times **per partition** and apply the missing-match
rules below. The difference between resolved offsets estimates the scan span. For an
exact count of readable matching records, scan the validated bounds and count only
records whose chosen timestamp satisfies `08:00 <= timestamp < 09:00`.
With unordered producer or payload times, widen the scan, potentially to all retained
records; narrow timestamp bounds can miss matching events at later offsets.

Example: readable records at offsets 100, 101 and 104 fall inside the interval;
its exclusive end offset is 105. The offset span is five, but the count is three.
Distinguish Kafka records from business events: one record may contain several events.
Report per-partition counts, the captured bounds, timestamp meaning and isolation mode.
This measures retained readable data, not records already deleted or never produced.
Capturing offsets does not freeze Kafka storage. Concurrent compaction can change
records inside those bounds; the count describes records observed during this scan,
not an atomic snapshot across partitions.

## 1. Read only a historical interval, then stop

Example: **2026-09-14 06:00–10:00 UTC**, interpreted as `[06:00, 10:00)`.
Do not reset or share the main job's consumer group. Keep the already-running main job
as it is; it continues processing live data. Independent groups do not eliminate
competition for broker and sink resources, so limit repair parallelism/rate as needed.

### Resolve timestamps before launching

```java
long fromMs = Instant.parse("2026-09-14T06:00:00Z").toEpochMilli();
long toMs   = Instant.parse("2026-09-14T10:00:00Z").toEpochMilli();
```

Using a separate KafkaConsumer with auto-commit disabled, discover the intended
partitions and call `offsetsForTimes(Map<TopicPartition, Long>)` once for each boundary.
Also read `beginningOffsets(partitions)` and `endOffsets(partitions)`. No subscription
or group-offset reset is required for these lookup calls. Use a timeout and fail the
preflight on errors. Record the topic/cluster identity and partition set.

Build and persist `Map<TopicPartition, Long> startOffsets` and `endOffsets` only after:

1. Every selected partition has both explicit offsets.
2. `logStart <= start <= end <= capturedLogEnd` for each partition.
3. Missing matches and retention truncation are accounted for; bounds alone cannot
   prove the requested time range is still fully retained.
4. The chosen timestamps represent the desired clock: Kafka record time versus payload
   business time. Offset bounds are not a universal event-time predicate.

A missing start match is not automatically an empty partition or permission to skip.
For an ambiguous nonempty partition, widen to the earliest retained offset. A missing
end match can use the captured log end **only for a justified snapshot of available
history**, with timestamp filtering as needed. If the end time is in the future, do
not claim that this snapshot will include future arrivals: wait or use an ongoing job.
For out-of-order event timestamps, end-time lookup can exclude later-arriving older
events; use wider offset bounds and filter decoded records instead.

### Build the finite source

```java
// startOffsets/endOffsets are validated persisted maps with identical key sets.
KafkaSource<String> repair = KafkaSource.<String>builder()
    .setBootstrapServers(bootstrapServers)
    .setPartitions(startOffsets.keySet())
    .setGroupId("events-repair-20260914-0600-1000")
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .setStartingOffsets(OffsetsInitializer.offsets(
        startOffsets, OffsetResetStrategy.NONE))
    .setBounded(OffsetsInitializer.offsets(endOffsets))
    .setProperty("enable.auto.commit", "false")
    .setProperty("commit.offsets.on.checkpoint", "false")
    .build();
```

These API classes are `KafkaSource`, `OffsetsInitializer`, Kafka's `OffsetResetStrategy`,
Flink's `SimpleStringSchema`, Java `Instant`, and Kafka `TopicPartition`.
A fixed partition set makes this repair's scope explicit; newly created partitions
need separate assessment. Source completion occurs after the exclusive ends are reached;
job completion also requires downstream operators/sinks to finish and flush successfully.
`NONE` prevents a start offset disappearing under retention from silently resetting.

No checkpoints are required for this finite repair. If it crashes, retry the same maps
through duplicate-safe output. Do not treat job-green, commits or row counts alone as
proof of recovery: compare expected output IDs, multiplicity and content.
For router failures, repair upstream first, then assess downstream coverage using the
original event IDs. Changing filter logic or external lookups can change replay results.

## 2. Use checkpoints for recovery

Apply to every job whose progress/state must survive: router and downstream writers.
Starting configuration for the existing API:

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

Source builder fragment:

```java
.setGroupId("events-main-v1")
.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
.setProperty("enable.auto.commit", "false")
.setProperty("commit.offsets.on.checkpoint", "true")
.setProperty("partition.discovery.interval.ms", "60000")
```

`hashmap` holds working state on the JVM heap and takes full snapshots. This baseline
fits the described stateless jobs; for large state compare EmbeddedRocksDBStateBackend
and incremental checkpoints, including local disk, CPU and serialization overhead.
There is no universal cheapest backend.

The URI must resolve to supported persistent storage accessible across Flink processes,
with the required plugin and credentials. Preserve operator UIDs/state compatibility.
Configure restart policy and deployment-appropriate HA for automatic recovery, including
JobManager loss. Retention must still cover the oldest required Kafka position.

Flink-managed recovery uses completed state. For a new manual submission, restore it
explicitly; merely retaining checkpoint files does not make a new job find them:

```text
flink run -s <completed-checkpoint-or-savepoint-URI> <job.jar>
```

The sink must flush/await output at checkpoints or preserve pending writes for replay.
Router KafkaSink can use `AT_LEAST_ONCE`; duplicates then need handling downstream.
Transactional `EXACTLY_ONCE` has transaction-ID, timeout and consumer-isolation requirements.
An arbitrary ClickHouse sink does not become exactly-once from the YAML flag.
Full setup and supported-runtime caveat: [kafka-flink-recovery-settings](kafka-flink-recovery-settings.md).

## 3. Use committed-earliest without checkpoints

Disable checkpointing in code and inherited configuration, then verify runtime state.
Source builder fragment:

```java
.setGroupId("events-main-v1")
.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
.setProperty("enable.auto.commit", "true")
.setProperty("auto.commit.interval.ms", "5000")
.setProperty("commit.offsets.on.checkpoint", "false")
.setProperty("partition.discovery.interval.ms", "60000")
```

Without checkpoints, Flink 1.17 defaults to no restart unless a restart strategy is
configured. For example, add the following starting policy to the job configuration:

```yaml
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 1000
restart-strategy.fixed-delay.delay: 30 s
```

This is a proposed retry budget (about 8.3 hours of delays), not infinite recovery or
a delivery guarantee. Size it to the outage objective and alert on exhausted retries.
A supervisor/HA setup must also recover process or cluster loss; task retry settings
alone cannot do that. Apply this policy to method 4 too.

Keep this group ID stable across restarts and unique to the logical job. `EARLIEST` is
only the fallback; it does not rewind a valid committed offset. If the process was dead
four hours and nobody advanced its group, restart reads those four retained hours.
The five-second interval governs commit timing, not the amount of recoverable history.

Auto-commit tracks consumer progress rather than successful final delivery. A crash can
cause loss or duplicates; buffering/sink stalls can exceed one interval. Repair missing
output using method 1. Without a reliable delivered frontier, the conservative repair
range is all retained history. Router KafkaSink guarantees requiring checkpoints do
not apply here; see [kafka-flink-without-checkpoints](kafka-flink-without-checkpoints.md) for producer and sink settings.

## 4. Set a start time only, then keep processing new events

**Yes. Omit stopping bounds. KafkaSource is continuous by default.**

```java
long fromMs = Instant.parse("2026-09-14T06:00:00Z").toEpochMilli();

KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers(bootstrapServers)
    .setTopics("events")
    .setGroupId("events-from-time-v1")
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .setStartingOffsets(OffsetsInitializer.timestamp(fromMs))
    .setProperty("enable.auto.commit", "true")
    .setProperty("auto.commit.interval.ms", "5000")
    .setProperty("commit.offsets.on.checkpoint", "false")
    .setProperty("partition.discovery.interval.ms", "60000")
    .build();
// No setBounded(...), and no setUnbounded(stoppingOffsets).
```

This no-checkpoint example reads from the resolved start, catches up, then follows
arrivals. The timestamp initializer ignores existing group commits when choosing its
start. On a fresh restart without restored state, the same timestamp is evaluated again:
**it replays from that time, not automatically from the last commit.** Auto-commit does
not change that startup rule. With checkpoint restoration, restored positions take
precedence for restored partitions.

For a one-time historical start followed by normal no-checkpoint restarts, use method 3
on subsequent submissions with the same group ID. Merely finding commits for every
partition is not a safe delivery handover: commits can outrun the sink. Before switching,
record the interval that could be undelivered and verify output coverage, or run method 1
over that overlap with deduplication. Without a delivered frontier, repair all retained
history. The switch changes initialization only; future auto-commit failures still need
repair. Missing commits replay earliest. Persist this transition in deployment
configuration rather than relying on memory.

### Timestamp caveats for both time-based methods

Flink 1.17 `timestamp()` falls back to the partition's end when no match exists. A future
start time is therefore **not** a scheduled gate that waits until that time: it may start
at the current end and consume arrivals before the requested time. For recovery, prefer
method 1's validated explicit start map; for continuous operation omit its bounded end.

For future ingestion-time-based recovery, topic `message.timestamp.type=LogAppendTime`
uses broker append time. Keep business event time in payload, configure watermarks from
that field if needed, and remember old records retain their original timestamps. Broker
clock anomalies and incomplete retention still require validation. With `CreateTime`
or payload event time, use conservative offsets and an explicit time filter; arbitrary
out-of-order arrivals mean a fixed offset interval may not contain every event-time match.

Starting a second unbounded job while the main job stays live produces an ongoing
overlap. Deduplicate, or plan a deliberate handover after catch-up. For a one-off gap,
use a bounded repair instead. Do not switch a healthy main job to `latest()` on every
restart: that can discard its own downtime backlog.

## Audit: the supplied ivi-ru ClickHouse sink

Inspected upstream revision `ef4adb9583cd51e22a3bf5b14f681aa8f5fa062f`
on 2026-09-14. This is a source audit, not a crash-test result or confirmation of the
deployed JAR. Its pom declares `1.4.1-SNAPSHOT` and Flink `1.9.0`; do not assume
runtime compatibility with the operator's Flink 1.17 deployment.

**This revision does not provide checkpoint-safe delivery into ClickHouse.**

- [ClickHouseSink](https://github.com/ivi-ru/flink-clickhouse-sink/blob/ef4adb9583cd51e22a3bf5b14f681aa8f5fa062f/src/main/java/ru/ivi/opensource/flinkclickhousesink/ClickHouseSink.java)
  extends `RichSinkFunction`, and `invoke()` hands records to `sink.put()`.
  It has no checkpoint callback to flush writes or snapshot pending records.
- [ClickHouseSinkBuffer](https://github.com/ivi-ru/flink-clickhouse-sink/blob/ef4adb9583cd51e22a3bf5b14f681aa8f5fa062f/src/main/java/ru/ivi/opensource/flinkclickhousesink/applied/ClickHouseSinkBuffer.java)
  keeps records in `localValues`, then submits batches to an asynchronous writer.
  Buffer size/time trigger submission; submission is not a durable insert acknowledgment.
- [ClickHouseWriter](https://github.com/ivi-ru/flink-clickhouse-sink/blob/ef4adb9583cd51e22a3bf5b14f681aa8f5fa062f/src/main/java/ru/ivi/opensource/flinkclickhousesink/applied/ClickHouseWriter.java)
  has an in-memory queue and HTTP requests with retries. Its shutdown waits for work,
  but shutdown handling does not run on SIGKILL and does not protect a checkpoint.

**Concrete inferred failure:** 100 records reach `invoke()`, 80 reach ClickHouse,
and 20 remain in sink memory. A checkpoint completes with the source past all 100.
The TaskManager is killed. Restoring that checkpoint does not replay those 20:
this sink neither included them in managed state nor awaited their delivery.
This counterexample follows the source paths; it has not been reproduced in the lab.

Set `clickhouse.sink.ignoring-clickhouse-sending-exception-enabled=false` when
using this connector so observed failed futures can surface through subsequent
`invoke()` calls. This is necessary error handling, **not a checkpoint-safety fix**.
The true setting permits sending errors to be ignored while failed batches are
written to disk. Such files are not Flink checkpoint state or an automatic replay
mechanism; they also do not cover a sudden kill of still-pending batches.

Checkpointing still restores upstream operator state and coordinated source positions.
It is therefore not identical to auto-commit, but neither alone makes this particular
asynchronous sink loss-free. Lower buffer sizes or more frequent checkpoints do not
supply the missing coordination. Duplicates remain possible on replay or retries;
the connector has no checkpoint-coordinated transaction protocol for exactly-once output.

For reliable delivery, replace or adapt the sink so every pre-checkpoint record is
acknowledged durably or recoverably snapshotted, including queued, in-flight and
retrying batches. Propagate failures and retain backpressure. Add output deduplication
or another verified exactly-once mechanism separately. Verify the deployed artifact,
ClickHouse table/insert settings and a kill-during-insert test with known event IDs
before declaring end-to-end guarantees. No connector code or production settings
were changed by this audit.

## Audit: the official ClickHouse connector

Inspected upstream revision `4fe4e51faa612ec4b01afe27aa8690c999e34ce0` on 2026-09-14,
focusing on its Flink 1.17 module. **Yes: asynchronous writes and checkpoint
integration are implemented. Exactly-once output is explicitly unsupported.**
This is source evidence, not a deployed-JAR audit or a completed crash test.

- [Writer](https://github.com/ClickHouse/flink-connector-clickhouse/blob/4fe4e51faa612ec4b01afe27aa8690c999e34ce0/flink-connector-clickhouse-1.17/src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseAsyncWriter.java):
  submits inserts through client asynchronous operations and handles completion
  through `CompletableFuture`. Batch size, buffer time and maximum in-flight
  requests control batching and concurrency. Client asynchronous execution does
  not itself enable ClickHouse server `async_insert`.
- [Checkpoint handling](https://github.com/ClickHouse/flink-connector-clickhouse/blob/4fe4e51faa612ec4b01afe27aa8690c999e34ce0/flink-connector-clickhouse-1.17/src/main/java/org/apache/flink/connector/clickhouse/sink/writer/ExtendedAsyncSinkWriter.java):
  waits for outstanding requests before checkpoint preparation completes, then
  snapshots unsent buffered records. It does not necessarily send all buffered
  records on every checkpoint. Restored state repopulates the buffer.
- [Sink](https://github.com/ClickHouse/flink-connector-clickhouse/blob/4fe4e51faa612ec4b01afe27aa8690c999e34ce0/flink-connector-clickhouse-1.17/src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseAsyncSink.java):
  provides the writer-state serializer and restores the buffered writer state.
- [Failure configuration](https://github.com/ClickHouse/flink-connector-clickhouse/blob/4fe4e51faa612ec4b01afe27aa8690c999e34ce0/flink-connector-clickhouse-1.17/src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseClientConfig.java):
  `BatchFailureStrategy.STOP_FLINK` is the default. Keep it when records must not
  be discarded. `DROP_BATCH` acknowledges a malformed batch as handled after
  incrementing drop counters; checkpoints cannot undo that intentional loss.

**What this changes:** unlike the audited ivi-ru sink, pending writes participate
in recovery. With checkpointing enabled, durable checkpoint storage, restoration
from a completed checkpoint, retained Kafka data, failure propagation and durable
insert acknowledgments, this provides an at-least-once recovery design. It does
not make Kafka auto-commit coordinated with the sink, and using this connector
without checkpoints does not gain checkpoint recovery automatically.

**Example:** checkpoint C covers source offsets through 100. Events 101–120 are
inserted into ClickHouse, then the job dies before the next checkpoint completes.
Restoring C replays 101–120: those events can be duplicated. A record buffered in
C is restored from sink state; a later record is replayed from Kafka. This fixes
the unmanaged-buffer loss mechanism in the old sink, but requires deduplication
for duplicate-free output. These numbers illustrate the protocol, not measured results.

The [README](https://github.com/ClickHouse/flink-connector-clickhouse/blob/4fe4e51faa612ec4b01afe27aa8690c999e34ce0/README.md) lists Flink 1.17.2 support through
`flink-connector-clickhouse-1.17`, Java 11+, and DataStream API support.
Table API is listed as not yet implemented. Validate the exact published version
against your deployed Flink version before switching.

Source concern to test: an immediate exception inside the writer's `insert()`
submission try-block is logged without a retry callback or fatal-error signal.
That path could leave an in-flight request outstanding and stall checkpoints;
this is an inferred edge case, not a reproduced failure. Run kill-during-insert,
malformed-batch and transport-failure tests before claiming production guarantees.
If server asynchronous inserts are separately enabled, verify durable acknowledgment
settings as well; client futures alone do not establish server durability.

## Choosing for this deployment

Given the operator's no-checkpoint constraint: method 3 for normal running plus method 1
for bounded repair. Method 4 is useful for an intentional historical bootstrap, with an
explicit subsequent restart policy. Method 2 remains the option for consistent state
recovery if the resource/latency tradeoff is acceptable after measurement.

The graceful-cancel lab recovered 900/900 outage records in selected cases. The crash
and no-dedup variants S02K/S21N failed in setup and have no verdicts. None of these notes
certifies the corporate ClickHouse sink. Validate in staging with TaskManager/router
failure, sink errors, retention boundaries, timestamp misses and final output reconciliation.

## Sources

- [Flink Kafka connector: source and sink behavior](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/)
- [KafkaSourceBuilder: starts and stopping bounds](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/connector/kafka/source/KafkaSourceBuilder.html)
- [OffsetsInitializer API](https://nightlies.apache.org/flink/flink-docs-release-1.17/api/java/org/apache/flink/connector/kafka/source/enumerator/initializer/OffsetsInitializer.html)
- [Flink 1.17 timestamp initializer implementation](https://raw.githubusercontent.com/apache/flink/release-1.17/flink-connectors/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/source/enumerator/initializer/TimestampOffsetsInitializer.java)
- [Flink checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Kafka timestamp lookup API](https://kafka.apache.org/38/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

- [Flink restart strategies](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/state/task_failure_recovery/)
