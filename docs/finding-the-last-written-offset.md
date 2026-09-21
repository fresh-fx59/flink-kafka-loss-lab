# Which offset actually reached ClickHouse?

The question this answers: a Flink 1.17 job with **checkpointing off** crashed. Which
Kafka offset was the last one whose record really landed in ClickHouse, so a replay can
start from exactly there instead of guessing a timestamp?

Every claim here is sourced. Where something is community practice rather than
documented, it says so.

## The short answer

**Nothing Flink or Kafka gives you for free answers this question.** Every available
signal sits *upstream* of the sink and is therefore an **upper bound** — resume from it
and you lose whatever was in flight. The only exact answer requires the sink itself to
record what it flushed.

| signal | what it really means | usable as the answer? |
|---|---|---|
| Kafka committed offsets (`__consumer_offsets`) | the consumer's **fetch position** | no — upper bound |
| Flink `currentOffset` metric | last record **emitted** by the source | no — upper bound |
| Flink `committedOffset` metric | **`-1`, always**, when checkpointing is off | no — never set |
| `max(event_id)` in the destination table | a per-branch maximum, not a completed prefix | no — see §5 |
| the sink logging its flushed range after the ack | what actually landed | **yes**, with caveats |
| a side progress table written after the ack | what actually landed, durably | **yes** — the real fix |

## 1. Why Kafka's committed offsets overshoot

With checkpointing disabled, Flink does not commit offsets at all — the Kafka client's
own timer does. Flink 1.17 docs, verbatim:

> "If checkpointing is not enabled, Kafka source relies on Kafka consumer's internal
> automatic periodic offset committing logic, configured by `enable.auto.commit` and
> `auto.commit.interval.ms` in the properties of Kafka consumer.
>
> Note that Kafka source does **NOT** rely on committed offsets for fault tolerance.
> Committing offset is only for exposing the progress of consumer and consuming group
> for monitoring."

The client commits `SubscriptionState.allConsumed()`, whose value is `position.offset`
— **the next offset to fetch**, i.e. everything already returned by `poll()`. In Flink
that position runs ahead twice over: the split-fetcher thread polls on its own thread
and buffers batches in the source reader's element queue, and the sink then buffers its
own batch. So the committed offset can overshoot the true ClickHouse high-water mark by
an entire in-flight buffer **plus** a sink batch.

Practical consequence: **read those offsets as a ceiling, and resume below them.**

Also check whether they exist at all. `KafkaSourceBuilder` defaults
`enable.auto.commit` to `false`, so unless the job set it explicitly there are **no
committed offsets in Kafka** and `--describe` will show none. The lab's S01 reproduced
exactly that: the broker answered `Consumer group 'loss-lab' does not exist`.

## 2. Why the Flink `committedOffset` metric is useless here

Flink 1.17 registers these per-partition gauges (names from
`KafkaSourceReaderMetrics.java` on `release-1.17`):

```
<...>.KafkaSourceReader.topic.<topic>.partition.<partition>.currentOffset
<...>.KafkaSourceReader.topic.<topic>.partition.<partition>.committedOffset
<...>.KafkaSourceReader.commitsSucceeded
<...>.KafkaSourceReader.commitsFailed
```

**The names are singular.** The 1.17 docs table calls them `currentOffsets` /
`committedOffsets` — that is a documentation bug; query the singular form.

- `currentOffset` — set in `KafkaPartitionSplitReader` as each record is handed to the
  emitter. This is "about to be emitted", not "confirmed downstream".
- `committedOffset` — recorded in exactly one place, `KafkaSourceReader
  .notifyCheckpointComplete()`. **With checkpointing off that method never fires, so the
  gauge stays at its initial value `-1` for the entire life of the job**, even while
  `enable.auto.commit=true` is doing real commits inside the Kafka client. Flink's gauge
  does not observe the client's auto-commits.
  (FLINK-16714, closed Won't Fix: *"A negative value means that the offset was not
  fetched yet from Kafka; any non-negative value would be misleading."*)
- `pendingRecords` — consumer lag, i.e. records **not yet fetched**. Not a position.

Metrics also die with the JVM. To retain anything you need a reporter and a TSDB, and
in Flink 1.17 the key is `factory.class` — **`metrics.reporter.<name>.class` is dead**
and silently reports nothing:

```yaml
metrics.reporters: prom
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9250-9260
```

For a crash-looping job, `PrometheusPushGatewayReporterFactory` with
`deleteOnShutdown: false` retains the last push. Either way the resolution is a scrape
interval — at 15 s and a few thousand records/s, the last scraped `currentOffset` can be
tens of thousands of records behind the crash. It bounds the **source**, never the sink.

## 3. Carrying the offset to the sink — no schema change needed

`KafkaRecordDeserializationSchema` hands over the whole `ConsumerRecord`:

```java
void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<T> out);
```

`ConsumerRecord` exposes `topic()`, `partition()`, `offset()`, `timestamp()`, `key()`,
`value()`, `headers()`, `leaderEpoch()`. The emitter passes only your `T` downstream, so
the offset is dropped **unless you attach it to the record object**. Nothing about this
requires a column in the destination table — the INSERT statement keeps only business
columns.

```java
// deserializer: attach the coordinates
row.kafkaPartition = record.partition();
row.kafkaOffset    = record.offset();

// sink: after the insert is ACKNOWLEDGED, not before
LOG.info("clickhouse flush ok partition={} lastOffset={} rows={}", p, o, n);
```

**One trap:** if anything shuffles between source and sink — a `keyBy`, a `rebalance`,
a parallelism change — the record is serialized, and `transient` offset fields become
`null` silently. They must be part of the serialized type. When source and sink are
chained (same parallelism, no shuffle) object reuse means no serialization and plain
fields are fine.

## 4. Where to put that record — three options, ranked

### 4a. A side progress table — RECOMMENDED

The documented pattern, and the primary source is Spark's Kafka integration guide:

> "Kafka delivery semantics in the case of failure depend on how and when offsets are
> stored. Spark output operations are at-least-once. So if you want the equivalent of
> exactly-once semantics, you must either **store offsets after an idempotent output, or
> store offsets in an atomic transaction alongside output**."

and its recipe contains the rule that matters:

```
// update offsets where the end of existing offsets matches the beginning of this batch
```

That clause **is** the anti-gap guard: refuse a progress update that is not adjacent to
the stored one. This lab proved why it is not optional. **S12** resumed from
`MAX(offset)` of the leading table and left the lagging table with 3 of 900 rows.
**S13** was written to fix that with a per-branch progress table and **still lost 450 of
900**, because the row was updated with `GREATEST(existing, new)` — once a starved branch
resumed it recorded the *later* offset and erased the gap it had skipped. A maximum is
still a maximum, wherever you store it. Track **contiguity**: the highest offset below
which nothing is outstanding.

Shape (`etl_offsets` as a separate ReplacingMergeTree table — not a change to the
business tables):

```
etl_offsets(job, topic, partition, next_offset, updated_at)
```

Written **after** the data insert acks, guarded by `new_start == stored_next_offset`.
ClickHouse has no multi-statement transactions, so data and progress cannot be atomic —
a crash between them replays a bounded window. That is at-least-once, which is the
correct target, and it is exactly what Spark prescribes.

### 4b. `log_comment` on the INSERT — a good second record, zero schema change

ClickHouse's `log_comment` setting is an arbitrary per-query string that lands in
`system.query_log.log_comment`. Stamp the range on every insert:

```sql
SET log_comment = '{"job":"router","p":3,"from":41230,"to":41329}';
INSERT INTO events_table (...) VALUES (...);
```

and read it back after a crash:

```sql
SELECT log_comment, event_time, written_rows
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment != ''
ORDER BY event_time DESC LIMIT 50;
```

`system.query_log` has no automatic TTL, so it persists. Caveat: it is flushed on an
interval, so the final pre-crash INSERT may not be there. Good corroboration, not a
replacement for 4a.

### 4c. A log line — forensic only, NOT the durability record

No Apache or vendor documentation, and no named-company write-up, endorses recovering a
resume point by querying logs. The failure modes are documented and they are all
concentrated exactly where it hurts:

- Log4j2 async appenders keep events in a ring buffer that is **lost on JVM crash**, and
  the queue-full policy *discards* events at or below `discardThreshold` (default
  `INFO`). Upstream: *"if a problem happens during the logging process and an exception
  is thrown, it is less easy for an asynchronous setting to signal this problem to the
  application."*
- Log shippers drop lines: Loki rejects out-of-order timestamps and oversize lines
  (`promtail_dropped_entries{reason="max_line_size_limited"}`), and rate limits apply.
- Rotation and shipping lag truncate the tail.

The pathology is structural: **the crash you are recovering from is the event most
likely to truncate the log**, so the log tail is biased against the exact moment you
care about. The same weakness applies to the skipped-row log line — a row skipped just
before a crash may exist nowhere at all.

Use it as a cross-check. If it is the only record, make the appender synchronous with
immediate flush and accept that the last lines may be missing.

## 5. Do not derive the resume point from `max(event_id)`

Three ways it breaks:

1. **A maximum is not a prefix.** With parallel or buffered flushing, rows with lower
   ids from a slower partition may never have landed. Measured: S12, S13.
2. **Offsets are per partition; an event id is one scalar.** Kafka guarantees order only
   *within* a partition, so a single number cannot name a resume point — you need one
   offset per partition. (Flink never conflates them: `committedOffsets` reads each
   partition's own commit, `timestamp(ms)` calls `offsetsForTimes` per partition, and
   `offsets(Map<TopicPartition, Long>)` takes an explicit map. The place the hazard is
   real is the CLI — `--reset-offsets --to-offset N` applies one number to **every**
   partition.)
3. **Ids and offsets are different sequences.** Producer retries, compaction, other
   record types and multiple producers all break any id≈offset arithmetic. Mapping one
   to the other means scanning Kafka, not computing.

As a **one-off forensic for a crash that already happened** it is fine: bracket the
window, pick a deliberately early per-partition resume point, replay, and dedup. Over-
replay plus dedup beats under-replay plus silent loss.

## 6. ClickHouse system tables — what helps and what does not

| table | carries | verdict |
|---|---|---|
| `system.query_log` | `query`, `written_rows`, `query_id`, **`log_comment`**, `event_time`. Not the VALUES payload. | **useful via `log_comment`** (§4b) |
| `system.part_log` | `event_type`, `part_name`, `partition_id`, `rows`, `event_time` | row counts and timing only — no offsets |
| `system.asynchronous_insert_log` | `query`, `rows`, `flush_query_id`, `flush_time`, status | async inserts only; no payload |
| `_part`, `_part_offset`, `_block_number` | physical storage positions, rewritten by merges | **unrelated to Kafka offsets** |
| `system.kafka_consumers` | `assignments.current_offset`, `num_commits`, … | **not applicable** — scoped to ClickHouse's own Kafka **table engine**. A Flink job writing over HTTP/native is not a Kafka consumer to ClickHouse, so this table is empty. The Kafka-engine virtual columns `_topic/_partition/_offset` are likewise unavailable. |

Replay idempotency without schema change: `insert_deduplication_token`. Note that plain
`MergeTree` deduplicates **nothing** by default — `non_replicated_deduplication_window`
is `0`; block-hash dedup is on by default only for `Replicated*MergeTree` via
`replicated_deduplication_window`. And a token only matches an identical block, so a
replay with shifted batch boundaries misses it. Row-level dedup
(`ReplacingMergeTree` + `FINAL`/`argMax`) is the dependable form.

## 7. Right now, for the crash that already happened

```bash
# 1. Capture the ceiling BEFORE it expires. offsets.retention.minutes defaults to
#    10080 (7 days) and the clock started when the group lost its last member.
kafka-consumer-groups.sh --bootstrap-server <b> --describe --group <g> \
  | tee /tmp/offsets-ceiling.txt
kafka-consumer-groups.sh --bootstrap-server <b> --describe --group <g> --state

# 2. The commit HISTORY with timestamps, not just the latest value (Kafka >= 3.7):
kafka-console-consumer.sh --bootstrap-server <b> \
  --topic __consumer_offsets --from-beginning \
  --formatter org.apache.kafka.tools.consumer.OffsetsMessageFormatter
#    Kafka < 3.7:
#    --formatter 'kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter'

# 3. Do NOT run --delete on the group or the topic: that drops the offsets
#    immediately, with no retention period.

# 4. Resume BELOW the ceiling, per partition, then dedup:
#    OffsetsInitializer.offsets(Map<TopicPartition, Long>)
```

## 8. And the caveat that outranks all of it

For this operator the sink **skips individual rows it cannot write** and logs the event
id. Knowing the exact right offset does not recover those rows: a replay feeds the same
row to the same skip. Offset recovery controls only what is **re-read**. Fix the sink's
reaction first — measured, S08 (swallow) lost 900 of 900 with the job green, S09
(fail the task) lost none.

## Sources

- Flink 1.17 Kafka connector — https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/datastream/kafka/
- `KafkaSourceReaderMetrics.java`, release-1.17 — https://github.com/apache/flink/blob/release-1.17/flink-connectors/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/source/metrics/KafkaSourceReaderMetrics.java
- Flink 1.17 metric reporters — https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/metric_reporters/
- Flink 1.17 metrics / system scope — https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/ops/metrics/
- FLINK-16714 (committedOffsets < 0) — https://issues.apache.org/jira/browse/FLINK-16714
- FLINK-8410 (commitedOffsets gauge prematurely set, reopened) — https://issues.apache.org/jira/browse/FLINK-8410
- Kafka consumer configs — https://kafka.apache.org/32/generated/consumer_config.html
- KIP-186 (offsets retention 7 days) — https://cwiki.apache.org/confluence/display/KAFKA/KIP-186%3A+Increase+offsets+retention+default+to+7+days
- Confluent broker configs (`offsets.retention.minutes` semantics) — https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html
- Spark Kafka 0.10 integration, "Storing Offsets" — https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
- Cloudera, offset management for Kafka with Spark Streaming — https://www.cloudera.com/blog/technical/offset-management-for-apache-kafka-with-apache-spark-streaming.html
- Spring Kafka, out-of-order commits — https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/ooo-commits.html
- ClickHouse `system.query_log` — https://clickhouse.com/docs/operations/system-tables/query_log
- ClickHouse `system.kafka_consumers` — https://clickhouse.com/docs/operations/system-tables/kafka_consumers
- ClickHouse deduplicating inserts on retries — https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries
- Log4j2 asynchronous loggers — https://logging.apache.org/log4j/2.x/manual/async.html
- Grafana, dropped logs from out-of-order timestamps — https://grafana.com/blog/2021/09/16/avoid-dropped-logs-due-to-out-of-order-timestamps-with-a-new-loki-feature/
