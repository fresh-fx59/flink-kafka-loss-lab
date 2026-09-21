# What to do, in order

Written for one specific production setup: **Flink 1.17.0 / Java 17, Kafka in ZooKeeper
mode, a custom ClickHouse sink that skips individual rows it cannot write, checkpointing
off and staying off.** Two job shapes: a router (`kafka1 → filter → kafka2`, then
`kafka2 → ClickHouse`) and a direct writer (`kafka1 → ClickHouse`).

Every step names the scenario that justifies it. Nothing here is advice the lab did not
measure.

---

## Step 1 — Fix the sink's reaction to a failed insert. Nothing else matters first.

**Measured:** S08 vs S09 — the same injected failure during catch-up.

| sink behaviour | events saved from the outage | job status |
|---|---|---|
| catch the exception, log it, drop the batch | **0 of 900** | green, no alarm |
| let it propagate and fail the task | **900 of 900** | restarts, replays, recovers |

**Do:**
- An insert failure must **propagate and fail the task**. Flink restarts the job and
  replays from the last committed position. That replay *is* the recovery.
- A full internal queue must **backpressure**, never drop. (S11 lost 100 rows this way.)
- Never acknowledge a batch before the write is durable. Use `async_insert=0`, or
  `async_insert=1` with `wait_for_async_insert=1`. (S10: ack-before-commit lost **900 of
  900** with no error anywhere.)
- Configure a restart strategy with enough retries to cover a realistic outage, and
  alert when they are exhausted. A short finite policy leaves the job permanently
  failed, which is the outage you are trying to prevent.

**Why first:** every later step only controls what gets **re-read**. If the sink eats
the replay, the rest is theatre.

---

## Step 2 — Make skipped rows findable. They are not recoverable.

**Measured:** S23 and S24 — a per-row skip (every 10th row unwritable), once without
checkpoints and once **with checkpoints, restarted from a retained checkpoint**.

```
S23  produced=900  saved=810  lost=90
S24  produced=900  saved=810  lost=90     <-- identical
```

**Checkpoints changed nothing.** A replay feeds the same row to the same skip. This is
the hard boundary:

> Offset configuration decides only what is **re-read**. It can never recover a row the
> sink chose to drop.

**Do:** at the moment a row is skipped, write it to a durable table, not only a log line.

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

This is a **new** table — the business tables are untouched. It turns "gone" into
"queued for repair", and it gives the one number a log line cannot be trusted to keep:
the offset.

Alert on a non-zero rejection rate. A per-row skip is invisible otherwise — batches keep
committing, lag stays at zero, throughput and row counts look normal. **Nothing turns
red.** That is what makes it worse than a whole-batch failure.

---

## Step 3 — Set the source so a restart resumes where it stopped.

**Measured:** S01 (auto-commit at the builder default), S02, S02L, S03, S04.

The trap: `KafkaSourceBuilder` defaults `enable.auto.commit` to **`false`**. With
checkpointing off and that untouched, **nothing is ever committed** and
`committedOffsets(EARLIEST)` silently degenerates into `earliest()`. S01 reproduced it:
the broker answered `Consumer group 'loss-lab' does not exist` and the run replayed the
entire log — 0 missing, **600 duplicates**.

```java
KafkaSource.<Row>builder()
    .setGroupId("stable-unique-id-for-this-logical-job")   // never changes between deploys
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
    .setProperty("enable.auto.commit", "true")
    .setProperty("auto.commit.interval.ms", "5000")
    .setProperty("commit.offsets.on.checkpoint", "false")
    .setProperty("partition.discovery.interval.ms", "60000")
```

**Results:** S02 recovered 900/900 with 0 duplicates. S02L held the outage open for
**three minutes** against a five-second interval and still recovered 900/900 — the
commit interval does **not** bound what a restart can reach.

**Do not:**
- `latest()` — S03 lost the entire window, 0 of 900.
- A `group.id` that changes between deploys — S04 **skipped 600 events already in the
  topic**. Note it skips; it does not duplicate.

---

## Step 4 — Know what Step 3 does *not* give you.

**Measured:** S02K — S02 repeated with the TaskManager **SIGKILLed** instead of cancelled.

```
S02   (graceful cancel)  lost 0
S02K  (hard kill)        lost 98
```

Auto-commit commits the consumer's **fetch position** — everything `poll()` returned.
That runs ahead of what the source emitted, which runs ahead of what the sink flushed.
So a crash loses the in-flight window.

Two things follow, and they matter for how you talk about this internally:

- S02's "zero loss" is **true for a clean stop**, not a guarantee.
- Flink's own `committedOffset` metric will not help you find the gap: with checkpointing
  off it is **never written at all** (it is set only inside
  `KafkaSourceReader.notifyCheckpointComplete()`, which never fires) and stays at `-1`
  for the life of the job — even while `enable.auto.commit=true` is really committing
  inside the Kafka client.

If losing up to one commit interval on a crash is unacceptable, Step 6 is the answer,
not a smaller interval. Shrinking the interval narrows the window; it never closes it.

---

## Step 5 — Set retention so recovery is physically possible.

No offset design survives a deleted record.

On **both** clusters, per topic:

```properties
cleanup.policy=delete
retention.ms=604800000      # 7 days
retention.bytes=-1          # no independent size limit
message.timestamp.type=LogAppendTime
```

Broker, both clusters:

```properties
offsets.retention.minutes=20160   # 14 days
```

Sizing, not a slogan: **detection + outage + replay + margin must fit inside
`retention.ms`.** Seven days is a starting budget — raise it if your detection time is
slower. Provision disk for retained bytes × replication factor across all topics and
alert before it fills. `retention.bytes=-1` removes a size cap that would otherwise
delete data before the time limit.

`offsets.retention.minutes` is separate and counts from when the group loses its **last
member** — i.e. the clock starts at the crash. Default is 7 days; 14 gives room to
investigate over a holiday.

`message.timestamp.type=LogAppendTime` is Step 7's prerequisite. Set it now, because it
only affects records written after the change.

---

## Step 6 — Put the resume point where the data is.

**Measured:** S12 and S13, both failures, and they define the design.

- **S12** resumed from `MAX(src_offset)` of the leading table. The lagging table ended
  with **3 of 900** rows.
- **S13** was written to fix that with a per-branch progress table and **still lost 450
  of 900**, because the row was updated with `GREATEST(existing, new)` — once the starved
  branch resumed it recorded the *later* offset and erased the gap it had skipped.

> A maximum is not a completed prefix, wherever you store it.

The deserializer already receives the whole `ConsumerRecord`, so partition and offset
reach the sink for free. **No synthetic column on the business tables** — put the record
in a new table:

```sql
CREATE TABLE etl_offsets
(
    job          LowCardinality(String),
    topic        LowCardinality(String),
    partition    UInt32,
    next_offset  UInt64,        -- the offset to RESUME FROM
    rows_written UInt64,
    updated_at   DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (job, topic, partition);
```

Three rules, each paid for by a failed scenario:

1. **Write it after the ack, never before.** Progress behind data → replay (safe).
   Progress ahead of data → silent loss. (S10.)
2. **Guard contiguity, not the maximum.** Refuse a batch whose first offset is not
   adjacent to the stored `next_offset`, and fail the task on a gap. Buffer per
   partition, or there is no single first/last to check. (S12, S13.)
3. **A partition with no stored row means `earliest()`, never `latest()`.** (S03, S22.)

At startup, read it once (`SELECT … FROM etl_offsets FINAL`, with
`select_sequential_consistency = 1` on a replicated setup) and pass an explicit
per-partition map:

```java
.setStartingOffsets(OffsetsInitializer.offsets(startsPerPartition))
```

If ClickHouse is unreachable at startup, **fail the job** — do not fall back to earliest
silently, or a transient outage becomes a full replay of a week of data.

Cross-check, nearly free: stamp `log_comment` on every INSERT
(`{"p":3,"from":41230,"to":41329}`) and read it back from `system.query_log`. Not
authoritative — that table flushes on an interval, so the last inserts before a crash may
be missing — but it survives the JVM and it disagrees with `etl_offsets` exactly when
something is wrong.

**Do not** make the sink's log line the record of truth. Log4j2 async appenders lose
their ring buffer on JVM crash and discard at/below INFO when full; log shippers drop
out-of-order and oversize lines. The crash you are recovering from is the event most
likely to truncate the log — the tail is biased against the exact moment you need.

---

## Step 7 — Have a repair procedure for a gap that already happened.

**Measured:** S21 and S22.

Restart **once** with a timestamp just before the outage began, so only the gap replays
instead of the whole retained log:

```java
.setStartingOffsets(OffsetsInitializer.timestamp(outageStartMillis))
```

or without touching code, with the job stopped:

```bash
kafka-consumer-groups.sh --bootstrap-server <b> --group <g> --topic <t> \
  --reset-offsets --to-datetime 2026-09-21T04:00:00.000 --dry-run
# inspect the per-partition offsets it prints, then re-run with --execute
```

**S21** (topic on `LogAppendTime`): 900/900, only the gap read.

**S22** (topic on `CreateTime`, producer clocks skewed): **0 of 900 saved.** The
time-index lookup found nothing at or after the target and Flink 1.17's
`TimestampOffsetsInitializer` fell back to the partition's **end offset** with
`OffsetResetStrategy.LATEST`. The restart splits read:

```
StartingOffset: 499   (events-1)
StartingOffset: 520   (events-0)
StartingOffset: 481   (events-2)
```

499 + 520 + 481 = 1500 = every record in the topic. It jumped to the head and dropped
the backlog **silently** — no exception, no warning.

So the rules are:

1. `message.timestamp.type=LogAppendTime` first (Step 5). Without it this is unsafe.
2. Aim **earlier** than the outage start, never at it.
3. Always `--dry-run` and confirm **no partition resolves to its log-end offset** — that
   is the skip-to-head signature.
4. Expect duplicates across the overlap and have Step 8 in place.

Note the failure directions: `committedOffsets(EARLIEST)` fails **safe** (replays too
much); `timestamp()` fails **unsafe** (skips). Never use `--reset-offsets --to-offset N`
— that applies one number to **every** partition. Offsets are per partition.

---

## Step 8 — Make the replay idempotent.

Every recovery above is at-least-once, so duplicates are not a bug to avoid — they are
the price of not losing data. They have to be absorbed.

**Use `ReplacingMergeTree` keyed on your event id**, with a version column, and read via
`FINAL` or `argMax`. It is eventual: duplicates stay visible until a background merge, so
every query and every downstream aggregate must be written for it.

**Do not rely on `insert_deduplication_token`.** It only matches an identical block, and
any replay shifts batch boundaries. Also note plain `MergeTree` deduplicates **nothing**
by default — `non_replicated_deduplication_window` is `0`, and block-hash dedup is on by
default only for `Replicated*MergeTree`.

This is the one place Postgres and ClickHouse genuinely differ: S15's
`ON CONFLICT DO NOTHING` is immediate and exact; ClickHouse's equivalent is eventual.
Plan queries accordingly.

---

## Step 9 — The router hop, if you keep it.

**Measured:** S17, S18, S20 (and S19, which failed its own prediction — see
`FINDINGS.md`).

- Each job's own offset configuration decides its own gap. **A router's checkpoint gives
  the downstream job no resume point** (S17, S18) — so "checkpoint only the routers" does
  not work as a plan.
- `KafkaSink`'s default guarantee is `NONE`. Without checkpointing that is all you can
  have, and the router becomes its own loss point.
- If the router matters, S20 is the shape that works: checkpoints on plus
  `DeliveryGuarantee.AT_LEAST_ONCE`, with duplicates absorbed downstream by Step 8.
- Do **not** add routers in front of the direct writers for durability. `kafka2` already
  buffers because it is a Kafka topic; an uncheckpointed router in front just moves the
  loss upstream.

---

## Step 10 — Alerts, because every failure here was silent.

Not one scenario in this lab announced itself. Add:

- **Consumer-group lag** per job, per partition.
- **"No rows in ClickHouse for N minutes"** per destination table — this is what catches
  a swallowed sink (S08) and an ack-before-commit (S10), where lag looks perfectly
  healthy.
- **Rejection rate** from `etl_rejects` — the only signal for a per-row skip (S23/S24).
- **`etl_offsets` staleness** — `updated_at` not advancing means the pipeline is stalled
  even if the job is "running".
- **Retention headroom** — alert before the disk that holds your recovery window fills.

---

## The order, in one line each

1. Sink: fail the task on an insert error; backpressure, never drop; never ack early.
2. `etl_rejects` table + a rejection-rate alert — skipped rows are unrecoverable by replay.
3. Source: stable `group.id`, `committedOffsets(EARLIEST)`, `enable.auto.commit=true`, 5 s.
4. Accept that a crash still loses the in-flight window — or go to 6.
5. Retention: 7 days data, 14 days offsets, `LogAppendTime`, on both clusters.
6. `etl_offsets` written after the ack, contiguity-guarded; missing row ⇒ earliest.
7. Repair procedure: `timestamp()` aimed early, `--dry-run` checked, on `LogAppendTime`.
8. `ReplacingMergeTree` on the event id, read with `FINAL`/`argMax`.
9. Router: `AT_LEAST_ONCE` + checkpoints, or accept it as a loss point.
10. Alerts on lag, on row-arrival, on rejects, on `etl_offsets` staleness, on retention.

Steps 1, 2 and 3 are the ones that would have changed the outcome of the incident that
started this. Steps 6 and 7 are what let you answer "which events were missed?" the next
time instead of guessing.
