# Two ways to record "this offset reached ClickHouse" without touching the business schema

Constraint: the INSERT statement carries only real business columns. No synthetic
`partition` / `offset` columns on the destination tables. Checkpointing stays off.

Both records below satisfy that. They are not alternatives of equal weight — **A is the
mechanism, B is the cross-check.** Run both; they fail in different ways, which is the
point.

---

# A. A side progress table

A separate small table, written by the sink **after** the data insert is acknowledged.
It is a new table, not a change to any existing one, so the constraint holds.

## A.1 The table

```sql
CREATE TABLE etl_offsets
(
    job          LowCardinality(String),   -- 'router', 'writer-direct', ...
    topic        LowCardinality(String),
    partition    UInt32,
    next_offset  UInt64,                   -- the offset to RESUME FROM
    rows_written UInt64,                   -- forensics only
    updated_at   DateTime64(3)             -- ReplacingMergeTree version column
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (job, topic, partition);
```

`next_offset` is the **resume point**, not the last written offset. Storing
"resume from here" removes an off-by-one that will otherwise bite during an incident at
3am.

Always read it with `FINAL`, because `ReplacingMergeTree` collapses on a background
merge, not on write:

```sql
SELECT partition, next_offset
FROM etl_offsets FINAL
WHERE job = 'writer-direct' AND topic = 'events';
```

## A.2 The write order — this is the whole correctness argument

```
1. INSERT the business rows
2. wait for the acknowledgement          <-- not optional
3. INSERT the progress row
```

ClickHouse has no multi-statement transactions, so steps 1 and 3 cannot be atomic. That
is fine, and the asymmetry is deliberate:

| crash point | stored progress vs data | on restart | result |
|---|---|---|---|
| between 1 and 3 | progress **behind** data | re-reads rows already written | duplicates → absorbed by dedup. **Safe.** |
| (if you wrote 3 first) | progress **ahead** of data | skips rows never written | **silent permanent loss** |

So: **never write progress before the ack, and never in a parallel branch.** The
ordering is the guarantee. Delivery is at-least-once, which is the correct target —
this is exactly what Spark's Kafka guide prescribes: *"store offsets after an idempotent
output, or store offsets in an atomic transaction alongside output."*

## A.3 The contiguity guard — the part that is not optional

**A maximum is not a completed prefix.** This is the single most expensive lesson in
this lab, and it cost two failed scenarios to learn:

- **S12** resumed from `MAX(src_offset)` of the leading table. The lagging table ended
  with **3 of 900** rows. 897 events lost permanently.
- **S13** was written to fix S12 with a per-branch progress table, and **still lost 450
  of 900** — because the progress row was updated with `GREATEST(existing, new)`. Once
  the starved branch resumed, it recorded the *later* offset and **erased the gap it had
  skipped**. Moving a maximum into a progress table leaves it a maximum.

The fix is to refuse any update that is not adjacent to what is stored. Spark's
documented recipe contains exactly this clause:

```
// update offsets where the end of existing offsets matches the beginning of this batch
```

In the sink, keeping `next_offset` in memory per partition:

```java
// after the data insert is acknowledged
long expected = inMemoryNextOffset.get(partition);       // loaded at startup from etl_offsets FINAL
if (batchFirstOffset != expected) {
    // A gap. Something was skipped, reordered, or a second writer is active.
    // Do NOT advance. Fail the task and let the replay close the gap.
    throw new IllegalStateException(
        "offset gap on " + topic + "-" + partition +
        ": batch starts at " + batchFirstOffset + ", expected " + expected);
}
inMemoryNextOffset.put(partition, batchLastOffset + 1);
// then INSERT the progress row
```

Two practical consequences:

- **Flush per partition, not across partitions.** A batch mixing partitions has no
  single `first`/`last`, and the guard cannot be evaluated. Key the buffer by partition.
- **A skipped row breaks contiguity by definition.** With a sink that drops individual
  bad rows, offset *N* is never written but *N+1* is. Decide explicitly which you mean:
  advance past a deliberately-skipped row (it is a rejected record, not a lost one) but
  record it — see A.5.

## A.4 Startup

```java
// one query, at open(), per job
SELECT partition, next_offset FROM etl_offsets FINAL
WHERE job = ? AND topic = ?
```

then feed it to the source as an explicit per-partition map:

```java
Map<TopicPartition, Long> starts = /* from the query */;
KafkaSource.<Row>builder()
    .setStartingOffsets(OffsetsInitializer.offsets(starts))   // one offset PER PARTITION
```

Rules for the gaps in that map:

- **A partition with no row** means nothing was ever written from it. Start at
  `earliest()`. Falling back to `latest()` here reintroduces the silent skip this whole
  design exists to prevent — measured in S03 (0 of 900 saved) and S22 (0 of 900).
- **A partition discovered later** (`partition.discovery.interval.ms`) has no row
  either. Same rule.
- **If ClickHouse is unreachable at startup, fail the job.** Do not "fall back to
  earliest" silently on a query error — that turns a transient outage into a full replay
  of the retained log, which under a plain-insert sink means mass duplicates.

Read the progress table with `select_sequential_consistency = 1` on a replicated setup,
so a lagging replica does not hand back a stale offset. Stale is safe in direction
(over-replay, not skip) but the setting bounds how much.

## A.5 Rejected rows belong in this record too

Since the sink skips rows ClickHouse refuses, the progress row alone says "we got past
offset N" without saying "row N is not in the table". Add the second table:

```sql
CREATE TABLE etl_rejects
(
    job         LowCardinality(String),
    topic       LowCardinality(String),
    partition   UInt32,
    offset      UInt64,
    event_id    String,
    reason      String,
    rejected_at DateTime64(3)
)
ENGINE = MergeTree ORDER BY (job, topic, partition, offset);
```

This is the durable version of the log line already being emitted, and it is the only
thing that makes the rejected rows findable later. Measured: **S23** — with every 10th
event unwritable, the run lost exactly those rows (900 produced during the outage, 810
saved, 90 lost) and **no offset strategy recovered them**, because a replay feeds the
same row to the same skip. A reject table turns "gone" into "queued for repair".

## A.6 Cost and failure modes

- One extra INSERT per flush per partition. At a 1-second flush interval and 3
  partitions that is 3 small inserts/second — noticeable as ClickHouse parts, so keep
  the flush interval at seconds, not milliseconds, and let `ReplacingMergeTree` merge.
- **Two writers on the same `(job, topic, partition)` corrupt the record.** If two
  deployments run at once, both advance it. The contiguity guard catches this (the
  second writer's batch will not be adjacent) — which is a reason to fail loudly rather
  than log and continue.
- The record is only as good as the ack. If the sink acknowledges before the write is
  durable, the progress row is a lie. Measured: **S10** — ack-before-commit lost 900 of
  900 with no error anywhere. Use `async_insert=0`, or
  `async_insert=1, wait_for_async_insert=1`.

---

# B. `log_comment` on the INSERT — the cross-check

ClickHouse has a per-query setting, `log_comment`, whose value is stored in
`system.query_log.log_comment`. It costs no schema change at all, because it is
attached to the *query*, not the data.

```sql
SET log_comment = '{"job":"writer-direct","topic":"events","p":3,"from":41230,"to":41329}';
INSERT INTO events_table (...) VALUES (...);
```

or per statement via the HTTP interface:

```
POST /?log_comment=%7B%22p%22%3A3%2C%22from%22%3A41230%2C%22to%22%3A41329%7D
```

Reading it back after a crash:

```sql
SELECT
    event_time,
    written_rows,
    JSONExtractString(log_comment, 'topic')  AS topic,
    JSONExtractUInt(log_comment, 'p')        AS partition,
    max(JSONExtractUInt(log_comment, 'to'))  AS last_offset
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND log_comment != ''
  AND event_time > now() - INTERVAL 1 DAY
GROUP BY event_time, written_rows, topic, partition
ORDER BY event_time DESC
LIMIT 50;
```

**Why it is a cross-check and not the mechanism:**

- `system.query_log` is flushed on an interval (`flush_interval_milliseconds`, default
  7500 ms), so the last inserts before a crash may never be written.
- It logs `QueryFinish` — the query succeeded — but says nothing about whether *your
  sink* then treated it as delivered.
- It is an operations table. Someone will `TRUNCATE` it, or a TTL will be added.

**Why it is still worth doing:** it is nearly free, it survives the process dying (it
lives in ClickHouse, not in the JVM), and it independently corroborates the progress
table. When A and B disagree, something is wrong with the sink, and you want to know.

---

# Why not just the log line

The sink can log `flushed partition=3 offsets=41230..41329` after the ack. Do it — it is
useful. But do not make it the record of truth:

- Log4j2 async appenders hold events in a ring buffer that is **lost on JVM crash**, and
  the queue-full policy *discards* events at or below `discardThreshold` (default
  `INFO`).
- Log shippers drop lines — Loki rejects out-of-order timestamps and oversize lines, and
  rate limits apply. Rotation and shipping lag truncate the tail.
- No Apache or vendor documentation endorses recovering a resume point from logs.

The pathology is structural: **the crash you are recovering from is the event most
likely to truncate the log.** The tail is biased against the exact moment you need. The
existing skipped-row log line has the same weakness — a row skipped seconds before a
crash may exist nowhere.

---

# Summary

| record | where it lives | survives a crash | authoritative | cost |
|---|---|---|---|---|
| `etl_offsets` + `etl_rejects` | ClickHouse, new tables | yes | **yes** | 1–2 small inserts per flush |
| `log_comment` → `system.query_log` | ClickHouse, existing system table | mostly (interval flush) | no — corroboration | ~free |
| sink log line | TaskManager log / Loki | **often not, exactly when it matters** | no — forensics | free |

Three rules that make A correct, each one paid for by a failed scenario in this lab:

1. Write progress **after** the ack, never before. (S10 — ack before commit: 900 lost.)
2. Guard **contiguity**, not the maximum. (S12: 897 lost. S13: 450 lost even with a
   progress table.)
3. A missing partition row means **earliest**, never latest. (S03, S22: 900 lost each.)
