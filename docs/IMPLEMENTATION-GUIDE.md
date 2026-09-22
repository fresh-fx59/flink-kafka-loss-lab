# Implementation guide — WHAT-TO-DO.md, step by step

`WHAT-TO-DO.md` says **what** to change and which scenario proves it. This file says
**how**: the rollout order, the code, the test that proves each change before prod, and
the rollback.

Target: Flink 1.17.0 / Java 17, Kafka (ZooKeeper mode), a custom ClickHouse sink,
checkpointing **off**. Two job shapes: router (`kafka1 → filter → kafka2 → ClickHouse`)
and direct writer (`kafka1 → ClickHouse`).

Code below is a template. Names like `Event`, `ChSink`, `eventId` stand in for your own
classes. The lab's `job/src/main/java/lab/PgSink.java` is the working reference for the
same mechanics against Postgres.

---

## Rollout plan at a glance

| Phase | Steps | Changes code? | Risk | Can ship alone? |
|---|---|---|---|---|
| 0 | Prepare: inventory, staging lab, baseline alerts | no | none | yes |
| 1 | Retention + `LogAppendTime` (step 5) | no, config | low | yes — do it first, it only helps later |
| 2 | Sink fails the task; no drop; no early ack (step 1) | yes | medium | yes |
| 3 | `etl_rejects` + rejection alert (step 2) | yes | low | yes |
| 4 | Source config (step 3) | yes, 3 lines | low | yes — ship with phase 2 |
| 5 | Idempotent tables (step 8) | schema + queries | medium | **before** phase 6/7 |
| 6 | `etl_offsets` resume point (steps 4, 6) | yes | high | after 2–5 are stable |
| 7 | Repair runbook (step 7) | no, procedure | — | after phase 1 has aged ≥ 1 retention |
| 8 | Router decision (step 9) | maybe | medium | any time after phase 5 |
| 9 | Alerts (step 10) | no, monitoring | none | grow it phase by phase |

Why this order and not the doc's numbering: retention is pure config and only protects
records written *after* the change, so start its clock first. Idempotency (phase 5)
must exist before anything that replays more (phases 6–7), or each replay creates
visible duplicates.

---

## Phase 0 — Prepare (½ day)

1. **Inventory.** For every job write down: job name, source topic(s), partition count,
   current `group.id`, `setStartingOffsets(...)` value, sink class, target table(s),
   ClickHouse engine, parallelism of source and sink. Put it in a table in your repo.
2. **Find every place the sink catches an exception.** `grep -n "catch" ChSink*.java`.
   Classify each: *rethrows* / *logs and continues per batch* / *logs and continues per
   row*. The last two are steps 1 and 2.
3. **Stand up a staging copy** of one direct-writer job against a throwaway ClickHouse
   and a throwaway topic. You will replay the lab scenarios on it (commands per phase).
4. **Baseline alert** you can add today, before any code: "no new rows in table X for
   N minutes" (see phase 9). It would have caught S08 and S10.

Done when: inventory table committed, catch sites classified, staging job runs.

---

## Phase 1 — Retention and timestamps (step 5)

Config only. Do it on **both** clusters.

```bash
# per topic, both clusters
kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name $T --alter \
  --add-config cleanup.policy=delete,retention.ms=604800000,retention.bytes=-1,message.timestamp.type=LogAppendTime

# verify
kafka-configs.sh --bootstrap-server $B --entity-type topics --entity-name $T --describe
```

Broker setting (`server.properties`, rolling restart; it is a static broker config):

```properties
offsets.retention.minutes=20160
```

**Size the disk first:** `bytes-in per day × 7 × replication factor`, summed over all
topics, plus 30 % headroom. Check with `kafka-log-dirs.sh --describe`.

**Check the consumers are not affected:** `LogAppendTime` replaces the record timestamp
with the broker's clock. If any job uses the Kafka record timestamp as *event time*
(watermarks, windows), move it to a timestamp field inside the payload **before** this
change. Grep for `WatermarkStrategy` and `record.timestamp()`.

Verify: `--describe` shows the 4 settings on every topic; one new record, read with
`kafka-console-consumer.sh --property print.timestamp=true`, shows `LogAppendTime:`.

Rollback: `--delete-config message.timestamp.type`. Retention can be lowered any time.

---

## Phase 2 — The sink fails the task (step 1)

The single most important change. Four rules, in code.

### 2.1 An insert error propagates

```java
private void flush() throws Exception {
    if (buffer.isEmpty()) return;
    List<Event> batch = new ArrayList<>(buffer);
    try {
        insertBatch(batch);          // synchronous; returns only after ClickHouse acked
        buffer.clear();              // clear ONLY after the ack
    } catch (Exception e) {
        // No "log and continue". Throwing fails the task; Flink restarts and the
        // source re-reads from the committed position. That replay is the recovery.
        throw new IOException("ClickHouse insert failed, " + batch.size() + " rows", e);
    }
}
```

Delete every `catch (...) { LOG.error(...); }` that does not rethrow on the write path.
The only exception allowed is the per-row reject in phase 3, and only because it
records the row durably.

### 2.2 Backpressure, never drop

If the sink has an internal queue or a background writer thread, the producer side must
**block** when full:

```java
queue = new ArrayBlockingQueue<>(capacity);
queue.put(event);        // blocks → Flink backpressure reaches the source
// never: if (!queue.offer(event)) dropped++;
```

A background thread's failure must reach `invoke()`: store it in a
`volatile Throwable asyncError` and throw it at the top of the next `invoke()`,
`flush()` and `close()`. Simplest correct design: **no background thread**, flush
synchronously from `invoke()` on size/time.

### 2.3 No ack before the write is durable

ClickHouse insert settings:

```java
// either
settings.put("async_insert", "0");
// or
settings.put("async_insert", "1");
settings.put("wait_for_async_insert", "1");
```

Also: flush on `finish()` **and** `close()`, and let a flush error in `close()`
propagate (the lab's `PgSink.close()` does this).

### 2.4 Restart strategy with a real budget

`flink-conf.yaml` (or per job):

```yaml
restart-strategy: exponential-delay
restart-strategy.exponential-delay.initial-backoff: 1 s
restart-strategy.exponential-delay.max-backoff: 2 min
restart-strategy.exponential-delay.backoff-multiplier: 2.0
restart-strategy.exponential-delay.reset-backoff-threshold: 10 min
restart-strategy.exponential-delay.jitter-factor: 0.1
```

In 1.17 exponential-delay retries forever — that is the point: a ClickHouse outage of
hours becomes a delay, not a loss. Pair it with an alert on `numRestarts` rising
(phase 9) so "retrying forever" is visible.

### 2.5 Prove it on staging

Replay S08 → S09: stop ClickHouse for 60 s while producing, start it again.

```
expected: rows in table == rows produced; job restarted ≥ 1 time; 0 missing
```

Count check (use your event id):

```sql
SELECT count(), uniqExact(event_id) FROM t WHERE produced_at >= {test_start};
```

Rollback: redeploy the previous jar. No state to migrate.

---

## Phase 3 — `etl_rejects`: skipped rows become findable (step 2)

Some rows really are unwritable (bad type, too long). Failing the task on them would
restart forever. So: reject them **durably**, then carry on.

### 3.1 Table

Create exactly the DDL in `WHAT-TO-DO.md` step 2, plus the raw payload so a row can be
repaired without Kafka:

```sql
ALTER TABLE etl_rejects ADD COLUMN payload String CODEC(ZSTD);
```

### 3.2 Sink code

ClickHouse rejects a whole INSERT, not one row. So you need the bad row isolated:

```java
private void insertBatch(List<Event> batch) throws Exception {
    try {
        insert(batch);
    } catch (SQLException e) {
        if (!isDataError(e)) throw e;          // network, timeout, auth → fail the task
        // A data error: split to find the bad rows. Bisect keeps it O(k log n).
        bisect(batch, e);
    }
}

private void bisect(List<Event> rows, SQLException cause) throws Exception {
    if (rows.size() == 1) {
        rejects.write(rows.get(0), cause);     // synchronous insert into etl_rejects
        return;                                // throws if etl_rejects itself fails
    }
    int mid = rows.size() / 2;
    for (List<Event> half : List.of(rows.subList(0, mid), rows.subList(mid, rows.size()))) {
        try { insert(half); } catch (SQLException e) {
            if (!isDataError(e)) throw e;
            bisect(half, e);
        }
    }
}
```

Rules:
- `isDataError` is a **whitelist** of ClickHouse error codes that mean "this row is bad"
  (e.g. 6 `CANNOT_PARSE_TEXT`, 27 `CANNOT_PARSE_INPUT_ASSERTION_FAILED`, 53
  `TYPE_MISMATCH`, 70 `CANNOT_CONVERT_TYPE`, 131 `TOO_LARGE_STRING_SIZE`). Everything
  else fails the task. Unknown ⇒ fail, never skip.
- If writing to `etl_rejects` fails, **throw**. A reject that is not recorded is a
  silent loss again.
- Rows rejected *before* the sink (deserializer, filter) go to the same table. A
  deserializer that throws on bad bytes should instead emit a reject record.
- The event needs `topic`, `partition`, `offset`: take them from the `ConsumerRecord`
  in your `KafkaRecordDeserializationSchema` (the lab's `EventDeserializer` does this).

### 3.3 Prove it

Replay S23 on staging: make every 10th row unwritable.

```sql
-- expected: saved + rejected == produced, and no offset in both
SELECT (SELECT count() FROM t          WHERE produced_at >= {s}) AS saved,
       (SELECT count() FROM etl_rejects WHERE rejected_at >= {s}) AS rejected;
```

Repair path to document: fix the data or the schema, then re-insert from
`etl_rejects.payload` (or re-read that exact offset from Kafka while it is retained).

---

## Phase 4 — Source config (step 3)

Ship together with phase 2.

```java
KafkaSource.<Event>builder()
    .setBootstrapServers(bootstrap)
    .setTopics(topic)
    .setGroupId("etl-" + jobName)                 // a constant; never a timestamp or UUID
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
    .setDeserializer(new EventDeserializer())     // keeps topic/partition/offset
    .setProperty("enable.auto.commit", "true")
    .setProperty("auto.commit.interval.ms", "5000")
    .setProperty("commit.offsets.on.checkpoint", "false")
    .setProperty("partition.discovery.interval.ms", "60000")
    .build();
```

Checks:
- `grep -rn "setGroupId\|OffsetsInitializer" src/` — no `latest()`, no generated ids.
- **One group id per logical job.** Two jobs sharing a group id overwrite each other's
  commits.
- **First deploy with a new group id** starts from the beginning of the retained log
  (EARLIEST). If that replay is too big, seed the new group first, with the job
  stopped: `kafka-consumer-groups.sh --group etl-X --topic T --reset-offsets
  --to-datetime <just before the old job stopped> --dry-run`, then `--execute`
  (phase 7 rules apply).

Prove it: replay S02 on staging (cancel job, produce 900, start job).

```bash
kafka-consumer-groups.sh --bootstrap-server $B --describe --group etl-X
# expected: the group exists, CURRENT-OFFSET advances, LAG returns to 0
```

Then 900/900 in ClickHouse, 0 duplicates.

---

## Phase 5 — Idempotent tables (step 8)

Every recovery is at-least-once. Make duplicates harmless **before** phases 6–7.

```sql
CREATE TABLE t_new
(
    event_id    String,
    ...business columns...,
    _version    UInt64          -- e.g. the Kafka offset, or produced_at in ms
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (event_id);           -- ORDER BY must contain the dedup key
```

Migration without downtime:
1. Create `t_new`. Point the sink at **both** tables (dual-write) for one deploy.
2. `INSERT INTO t_new SELECT ... FROM t` for history.
3. Switch readers to `t_new`, with `FINAL` or `argMax(col, _version) ... GROUP BY event_id`.
4. Stop writing `t`. Keep it for one retention period, then drop.

Every query and downstream aggregate must be rewritten for eventual dedup — a
materialized view on top of a ReplacingMergeTree still counts duplicates. List these
readers in the phase-0 inventory.

Prove it: insert the same 900 rows twice, then
`SELECT count() FROM t_new FINAL` = 900.

---

## Phase 6 — `etl_offsets`: the resume point lives with the data (steps 4, 6)

Only if losing one commit interval on a crash (S02K: 98 events) is not acceptable.
This is the biggest change; do it on one job first.

### 6.1 Precondition: one partition → one sink subtask, in order

The contiguity guard needs every record of a partition to reach the **same** sink
subtask **in order**. So:
- source and sink have the **same parallelism** and are **chained** (no `keyBy`,
  `rebalance`, or `shuffle` between them). Check the Flink UI job graph: one box.
- a `filter` in the chain is fine **only if** filtered records still advance progress.
  Do not drop them: map them to a "skip marker" that the sink counts but does not insert.

### 6.2 Table

Exactly the `etl_offsets` DDL in `WHAT-TO-DO.md` step 6.

### 6.3 Sink code: per-partition buffers + contiguity guard

```java
// state per partition this subtask owns
private final Map<TopicPartition, Long> nextOffset = new HashMap<>(); // loaded at open()
private final Map<TopicPartition, List<Event>> buf = new HashMap<>();

public void invoke(Event e, Context ctx) throws Exception {
    TopicPartition tp = new TopicPartition(e.topic, e.partition);
    Long expected = nextOffset.get(tp);
    long last = lastSeen(tp);                  // offset of the last record buffered, or expected-1
    if (e.offset <= last) return;              // replay overlap: already buffered or done
    if (expected != null && e.offset != last + 1 && !gapsAllowed) {
        throw new IllegalStateException("gap on " + tp + ": expected " + (last + 1)
                + ", got " + e.offset);        // fail the task; never write past a hole
    }
    buf.computeIfAbsent(tp, k -> new ArrayList<>()).add(e);
    if (due()) flushAll();
}

private void flushAll() throws Exception {
    for (var entry : buf.entrySet()) {
        List<Event> rows = entry.getValue();
        if (rows.isEmpty()) continue;
        insertBatch(realRows(rows));                           // phase 2 + 3: acked, rejects recorded
        long next = rows.get(rows.size() - 1).offset + 1;
        writeProgress(entry.getKey(), next, rows.size());      // AFTER the ack, never before
        nextOffset.put(entry.getKey(), next);
        rows.clear();
    }
}
```

The three rules from the doc, where they live:
1. **After the ack:** `writeProgress` runs only after `insertBatch` returned.
2. **Contiguity, not maximum:** the `IllegalStateException` above. Never
   `GREATEST(old, new)` (S13).
3. **No row ⇒ earliest:** see 6.4.

**Offset gaps that are not loss.** Transactional or idempotent producers write control
records, so offsets can legally jump by 1–2. If any producer to this topic uses
transactions, set `gapsAllowed = true` and instead guard that the first offset after a
restart is `<= nextOffset` (never ahead of the stored resume point). Check with the
producer owners; record the answer in the inventory.

### 6.4 Startup: read the resume point

Adapt the lab's `PgOffsetsInitializer` (FRONTIER mode) to ClickHouse:

```java
public Map<TopicPartition, Long> getPartitionOffsets(Collection<TopicPartition> parts,
                                                     PartitionOffsetsRetriever r) {
    Map<TopicPartition, Long> earliest = r.beginningOffsets(parts);
    Map<Integer, Long> stored = query(
        "SELECT partition, next_offset FROM etl_offsets FINAL " +
        "WHERE job = ? AND topic = ? SETTINGS select_sequential_consistency = 1");
    // ClickHouse down → this throws → job fails to start. Do NOT catch and fall back.
    Map<TopicPartition, Long> out = new HashMap<>();
    for (TopicPartition tp : parts) {
        Long s = stored.get(tp.partition());
        long start = (s == null) ? earliest.get(tp) : Math.max(s, earliest.get(tp));
        if (s != null && s < earliest.get(tp)) {
            LOG.error("resume point {} for {} is below log start {}: data expired", s, tp, earliest.get(tp));
            // decide: fail the job (safer) or continue and alert. Default: fail.
            throw new IllegalStateException("resume point expired for " + tp);
        }
        out.put(tp, start);
    }
    return out;
}
public OffsetResetStrategy getAutoOffsetResetStrategy() { return OffsetResetStrategy.EARLIEST; }
```

Keep `enable.auto.commit=true` from phase 4: the broker offsets stay useful for lag
monitoring, even though they are no longer the resume point.

### 6.5 Cross-check (optional, cheap)

Add `log_comment` to each insert: `{"p":3,"from":41230,"to":41329}`. Compare with
`system.query_log` after an incident.

### 6.6 Prove it

On staging, all three must pass:
- **S02K:** `kill -9` the TaskManager mid-stream → restart → 900/900, 0 missing
  (the thing phase 4 alone cannot do).
- **S12/S13 shape:** starve one partition's inserts for 60 s → the job must **fail**,
  not advance.
- **ClickHouse down at startup** → the job refuses to start; no replay from earliest.

Rollback: switch `setStartingOffsets` back to `committedOffsets(EARLIEST)`. Because
auto-commit stayed on, the broker offsets are still current.

---

## Phase 7 — Repair runbook (step 7)

A procedure, not code. Commit it next to the job as `RUNBOOK-replay.md`:

1. Confirm the topic has `LogAppendTime` and that the phase-1 change is older than the
   outage. If not: **stop** — use offsets from `etl_offsets`/`etl_rejects`, not time.
2. Stop the job (savepoint not needed; checkpointing is off).
3. Pick `T = outage start − 15 min`.
4. Dry run:
   ```bash
   kafka-consumer-groups.sh --bootstrap-server $B --group etl-X --topic $T \
     --reset-offsets --to-datetime 2026-09-21T03:45:00.000 --dry-run
   kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server $B --topic $T --time -1
   ```
5. Compare: **if any partition's new offset equals its log-end offset, abort** — that is
   the S22 skip-to-head signature.
6. Re-run with `--execute`. Start the job. With phase 6 in place, instead delete or
   lower the `etl_offsets` rows for the affected partitions.
7. Verify with the phase-2 count query; duplicates are absorbed by phase 5.

Never `--to-offset N` for the whole topic: it applies one number to every partition.

Rehearse it once on staging (replay S21) before you need it.

---

## Phase 8 — The router (step 9)

Decide per router, and write the decision down:

- **Keep and protect:** enable checkpointing on the router job only
  (`execution.checkpointing.interval: 30 s`, `externalized-checkpoint-retention:
  RETAIN_ON_CANCELLATION`) and
  `KafkaSink.builder().setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)`.
  Downstream dedup comes from phase 5. This is S20.
- **Remove:** if the filter is cheap, move it into the direct writer
  (`kafka1 → filter → ClickHouse`) and delete the hop. One loss point fewer.
- **Leave as is:** only with a written acceptance that the router can lose data.

Each downstream job keeps its **own** source config from phase 4; a router checkpoint
never helps the downstream job (S17, S18).

---

## Phase 9 — Alerts (step 10)

Add each one in the phase that makes it possible:

| Alert | Source | Added in | Catches |
|---|---|---|---|
| No new rows in table X for N min | ClickHouse `max(inserted_at)` | 0 | S08, S10 |
| Job restarting | Flink `numRestarts` rate | 2 | sink failing in a loop |
| Consumer lag per partition | `kafka-consumer-groups` / exporter | 4 | stalls, crash loops |
| Rejection rate > 0 | `etl_rejects` | 3 | S23/S24 |
| `etl_offsets.updated_at` stale | ClickHouse | 6 | stalled pipeline |
| Disk headroom for retention | broker disk | 1 | lost recovery window |

Do **not** alert on Flink's `committedOffset` metric: with checkpointing off it stays
`-1` forever (step 4 of `WHAT-TO-DO.md`).

Example ClickHouse checks:

```sql
-- no rows in 10 min
SELECT now() - max(inserted_at) > INTERVAL 10 MINUTE FROM t;
-- rejects in last 5 min
SELECT count() FROM etl_rejects WHERE rejected_at > now() - INTERVAL 5 MINUTE;
-- stalled progress
SELECT job, partition FROM etl_offsets FINAL WHERE updated_at < now() - INTERVAL 10 MINUTE;
```

---

## Definition of done, per job

- [ ] No non-rethrowing `catch` on the write path; bounded queues block.
- [ ] `async_insert=0` or `wait_for_async_insert=1`; restart strategy + alert.
- [ ] `etl_rejects` written for every skipped row; alert on it.
- [ ] Stable `group.id`, `committedOffsets(EARLIEST)`, auto-commit 5 s; group visible in `--describe`.
- [ ] Topic: 7 d retention, `LogAppendTime`; broker: 14 d offsets.
- [ ] Target table is ReplacingMergeTree on the event id; readers use `FINAL`/`argMax`.
- [ ] (if needed) `etl_offsets` with contiguity guard; S02K passes on staging.
- [ ] Repair runbook rehearsed once.
- [ ] Router decision written down.
- [ ] Staging replay of S08/S09, S02, S23 passes; results saved with the job.
