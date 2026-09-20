# Findings

What the lab actually established, including the scenarios that refuted their own
prediction and the ones that never produced a number. Every claim below points at a
row in [`../RESULTS.md`](../RESULTS.md); anything without a row is marked as not
measured.

The recommended configurations live in
[kafka-flink-recovery-settings.md](kafka-flink-recovery-settings.md) (with checkpoints),
[kafka-flink-without-checkpoints.md](kafka-flink-without-checkpoints.md) (without), and
[kafka-flink-recovery-methods-comparison.md](kafka-flink-recovery-methods-comparison.md).

## The question

A Flink job stops for hours. Kafka still holds the events. Which single setting decides
whether those hours are re-read and saved, or lost forever?

Every scenario is the same shape: produce → run → **kill the job** → keep producing →
restart → ask the database what survived. `outage.produced` vs `outage.saved` is the
answer; row counts and consumer lag are not evidence and the harness refuses to use them.

## 1. What recovers an outage

| configuration | evidence | saved | duplicates |
|---|---|---|---|
| `committedOffsets(EARLIEST)` + `enable.auto.commit=true` + `auto.commit.interval.ms=5000`, **no checkpoints** | S02 | 900/900 | 0 |
| the same, with a **3-minute** outage against a 5 s interval | S02L | 900/900 | 0 |
| checkpoints on, restart **without** `-s` | S05 | 900/900 | 398 |
| checkpoints on, restart **with** `-s <checkpoint>` | S06 | 900/900 | 0 |
| one-off repair: restart from `OffsetsInitializer.timestamp(outage start)` | S21 | 900/900 | 0 (a unique index absorbed them — see §5) |

**The commit interval does not bound what a restart can recover.** S02L held the outage
open for three minutes against a five-second interval and still recovered every event.
The interval governs how often the bookmark is written *while the job runs*; the Kafka
log is what makes the data recoverable afterwards.

## 2. What loses the outage

| configuration | evidence | saved | note |
|---|---|---|---|
| `latest()` | S03 | **0/900** | the whole window skipped |
| `committedOffsets(LATEST)` with a `group.id` that changes between deploys | S04 | 900/900 but **600 missing** | a fresh group has no offsets, so the LATEST fallback skips what is already in the topic. It does **not** duplicate. |
| `timestamp()` on a `CreateTime` topic with skewed clocks | S22 | **0/900** | §5 |
| sink swallows its insert exception | S08 | **0/900** | §3 |
| sink acknowledges before the commit | S10 | **0/900** | §3 |
| bounded sink queue drops instead of back-pressuring | S11 | 900/900, 100 missing elsewhere | §3 |

## 3. The sink decides whether recovery is worth anything

S08 and S09 inject the **same** failure during the catch-up flood and differ only in
what the sink does with the exception.

| | evidence | saved | job status |
|---|---|---|---|
| catch, log, drop the batch | S08 | **0 of 900** | green, no alarm |
| let it propagate and fail the task | S09 | **900 of 900** | restarts, replays, recovers |

This is the single highest-value result in the lab. Offset configuration only controls
what gets **re-read**; if the sink eats the replay, the fix is theatre. Fix the sink
first.

## 4. The two-hop (router) topology

- **S17 / S18** — killing only the downstream job, or only the router, both recover
  900/900. Each job's own offset configuration decides its own gap; the other job's
  state is irrelevant to it. A router's checkpoint gives the downstream job no resume
  point.
- **S20** — router with checkpoints and `DeliveryGuarantee.AT_LEAST_ONCE`: 900/900,
  and the duplicates it pushes into the second cluster are absorbed downstream.
- **S19 — REFUTED ITS OWN PREDICTION.** An uncheckpointed router with
  `DeliveryGuarantee.NONE` was predicted to lose records. It lost none, for two reasons
  the scenario had not accounted for: `flink cancel` is **graceful**, so Flink closes
  the sink and flushes the Kafka producer; and with auto-commit off the router had no
  resume point at all and replayed from earliest. `NONE` is still unsafe — but this
  scenario did not demonstrate it, and the honest record is that it failed.
  **S19H** (hard SIGKILL, auto-commit on a 1 s timer) was written to test it properly
  and has **not been run**.

## 5. Timestamp positioning — the right repair tool, with one trap

`OffsetsInitializer.timestamp(ms)` is not a durability mechanism. It is a **one-off
repair**: restart once, aimed just before the outage began, so only the gap replays
instead of the entire retained log.

**S21** (topic on `message.timestamp.type=LogAppendTime`): 900/900, only the gap read.

**S22** (topic on `CreateTime`, outage records back-dated 24 h — producer clock skew):
**0 of 900 saved**. The time-index lookup found nothing at or after the target, and
Flink 1.17's `TimestampOffsetsInitializer` falls back to the partition's **end offset**
with `OffsetResetStrategy.LATEST`. The restart splits read:

```
StartingOffset: 499   (events-1)
StartingOffset: 520   (events-0)
StartingOffset: 481   (events-2)
```

499 + 520 + 481 = 1500 = every record in the topic. It jumped to the head and dropped
the backlog **silently** — no exception, no warning.

Compare the failure directions: `committedOffsets(EARLIEST)` fails *safe* (replays too
much); `timestamp()` fails *unsafe* (skips). So:

1. set `message.timestamp.type=LogAppendTime` on any topic you intend to replay by time;
2. aim earlier than the outage start, never at it;
3. have row-level deduplication downstream, because the overlap **will** duplicate;
4. before executing, run `kafka-consumer-groups --reset-offsets --to-datetime … --dry-run`
   and check that **no partition resolves to its log-end offset** — that is the
   skip-to-head signature.

**Offsets are per partition, and Flink never conflates them.** Every startup mode
resolves to a vector: `committedOffsets` reads each partition's own commit,
`timestamp(ms)` calls `offsetsForTimes` per partition, `offsets(Map<TopicPartition,
Long>)` takes an explicit map. Offset 3 in partition 1 and offset 3 in partition 2 are
unrelated and are treated as such. The one place the worry is real is the CLI:
`--reset-offsets --to-offset 3` applies that one number to **every** partition. Use
`--to-datetime` or `--shift-by` instead.

## 6. Fan-out: a maximum is not a completed prefix

One topic writing to several tables, with one table deliberately starved so the
branches diverge.

- **S12** — resume from `MAX(src_offset)` of the **leading** table. `t_a` ended with
  897 of 897; `t_b` ended with **3 of 900** — 897 events lost permanently. The maximum
  says nothing about whether earlier offsets were completed.
- **S13 — REFUTED ITS OWN PREDICTION.** Resuming from `MIN` over a per-branch progress
  table was supposed to fix S12. It lost 450 of 900 anyway. The reason is instructive:
  the progress row was updated with `GREATEST(existing, new)`, so once the starved
  branch resumed it recorded the *later* offset and **erased the gap it had skipped**.
  A frontier built from a per-branch maximum is still a maximum. A correct frontier has
  to track **contiguity** — the highest offset below which nothing is outstanding — not
  the highest offset seen. This is a genuine hazard for any "store the resume point in
  the sink store" design, and it is the second scenario in this lab that refuted the
  design it was written to endorse.

## 7. Hard limits nothing can beat

- Topic `retention.ms`. An offset cannot address a deleted record. Recovery is bounded
  by retention, not by any Flink setting.
- Broker `offsets.retention.minutes` — committed offsets are discarded this long after
  the group loses its **last member** (default 7 days). After that the group looks brand
  new and falls back to its reset strategy.
- Catch-up throughput. If replay cannot consume faster than new data arrives, no finite
  retention fixes the backlog.

## 8. Not measured — stated so the gap is visible

- **S07** — committed-offset expiry (`offsets.retention.minutes=1` plus a held-open
  outage). Never produced a verdict.
- **S16** — XA exactly-once. **Deliberately not built.** It requires a JDBC driver with
  XA support and `max_prepared_transactions > 0`; ClickHouse has neither, so the result
  could not inform the target deployment.
- **S19H** — the honest re-run of S19 (hard kill, auto-commit on). Not run.
- **S02K** — does auto-commit really produce neither loss nor duplicates, or did the
  graceful `flink cancel` hide the window? **Not run**, and this matters: S02's and
  S02L's zero-duplicate results were both obtained with a *graceful* shutdown, which
  drains the pipeline. Auto-commit commits the **fetcher's** position, which normally
  sits ahead of what the sink wrote, so a crash is expected to **lose** up to one commit
  interval. Treat S02's "0 duplicates, 0 lost" as *"true for a graceful stop"* and not
  as a general guarantee.
- **S21N** — how many duplicates `timestamp()` produces with no deduplication. Not run.
  S21's zero-duplicate figure was obtained with a unique index absorbing them, so it is
  not a measurement of the strategy itself.

## 9. Lab bugs worth knowing about

Nine plumbing bugs were fixed before the first verdict. Three are production traps in
their own right:

1. **Flink's tarball binds RPC to localhost** (`jobmanager.bind-host`,
   `taskmanager.host`, `rest.bind-address`). In containers the TaskManager never
   registers. The official images strip these; an image built from the tarball must too.
2. **JDBC via `DriverManager` fails only on restart.** `DriverManager` lives on the
   system classloader while the driver arrives on Flink's child-first user classloader.
   The first submission works; every restart dies with `No suitable driver found` — a
   restart-only failure that looks exactly like a data bug. Instantiate the driver
   directly.
3. **A cgroup memory cap can apply to nothing.** podman with the systemd cgroup manager
   always parents a pod under `machine.slice`, so `--cgroup-parent` on the pod is
   silently ignored. A cap that silently applies to nothing reads exactly like a working
   one; assert `MemoryCurrent` on the slice that actually holds the containers.
