# flink-kafka-loss-lab

A reproduction lab for one question:

> A Flink job stops for a few hours. Kafka keeps the events. When the job comes back,
> **which single setting decides whether those hours are re-read and saved, or lost
> forever?**

Production shape being reproduced: Apache Flink **1.17.0 on Java 17**, Kafka in
**ZooKeeper mode** (3 brokers for the source cluster, 2 for the router target), a
stateless filter job, and a database sink. Checkpointing off. The lab runs the same
outage every time — produce, run, **kill the job**, keep producing, restart — and then
asks the database what survived.

## What it measures

For every destination table:

```
missing   = expected_ids - actual_ids     MUST be empty
duplicate = ids seen more than once       reported
extra     = actual_ids - expected_ids     MUST be empty
```

and the headline number:

```
events produced while the job was down   vs   how many of those are now in the database
```

**Row counts and Kafka consumer lag are not evidence** and the harness does not use
them. The producer's own ledger, not Kafka, is the source of truth for what should
have arrived.

Each scenario carries a written **prediction**. A run where the observation disagrees
with the prediction is the most valuable result the lab can produce, and is recorded
as such.

## Recovery guides

- [Recovery methods and ClickHouse connector audits](docs/kafka-flink-recovery-methods-comparison.md)
- [Kafka and Flink recovery settings](docs/kafka-flink-recovery-settings.md)
- [Recovery without checkpoints](docs/kafka-flink-without-checkpoints.md)

These guides include the 2026-09-14 evidence corrections. Graceful-cancel results
are not crash guarantees; S02K/S21N stopped during setup and have no verdicts.
ClickHouse connector findings are source audits, not lab crash-test results.

## Quick start

```bash
make build      # compile the Flink job (Maven runs in a container)
make up         # bring up ZooKeeper, 5 Kafka brokers, Postgres, Flink JM+TM, harness
make topics     # create the topics explicitly (auto-create is off on purpose)

./harness/run.sh harness/scenarios/S06.env     # the recommended setup
./harness/run.sh harness/scenarios/S03.env     # the control that loses data
```

Results land in `harness/out/<scenario>/verdict.json`, with the resolved per-partition
starting offsets and the consumer-group state saved alongside.

## Scenarios

| # | What it sets up | Prediction |
|---|---|---|
| S01 | `committed-earliest`, no checkpoints, auto-commit left at its default | Nothing ever commits, so this degenerates into `earliest()` — full replay every restart |
| S02 | + `enable.auto.commit=true`, 5 s interval | Resume near where the source stopped; outage re-read |
| S03 | `latest()` | Outage skipped and lost — the control |
| S04 | `committed-latest` with a rotating group id | Silent loss, indistinguishable from S03 in the logs |
| S05 | Checkpoints on, restart **without** `-s` | Do checkpoint-committed offsets suffice on their own? |
| S06 | Checkpoints on, restart **with** `-s` | Exact resume, zero loss — the recommendation |
| S08 | Sink **swallows** its failure during catch-up | Offsets advance, rows never land, nothing turns red |
| S09 | Same failure, but it **fails the task** | Flink replays, rows land — the one-line fix |
| S10 | Sink acks **before** the commit | Silent loss with no error anywhere |
| S11 | Bounded queue that **drops** instead of back-pressuring | Loss while the job reports healthy |
| S14 | Replay with plain `INSERT` | Duplicates land |
| S15 | Replay with a unique index + `ON CONFLICT DO NOTHING` | Exact row set after replay |

Observed results: [`RESULTS.md`](RESULTS.md).

## Why Flink 1.17.0 on Java 17 needs a custom image

Docker Hub publishes **no `flink:1.17.*-java17`** image — the 1.17 line ships `-java8`
and `-java11` only, and `java17` tags start at 1.18, where Java 17 support landed as
*experimental*. `flink-image/` builds one on `eclipse-temurin:17` and injects the exact
`--add-exports` / `--add-opens` list that Flink 1.18 ships as its default. Flink's docs
are explicit that this list "must not be shortened, but only extended".

Related: unaligned checkpoints are **disabled on purpose**. FLINK-31963 breaks
unaligned-checkpoint rescaling on 1.17.0 (fixed in 1.17.1).

## Footprint

Ten containers, capped at **3.8 GiB** total, everything bound to `127.0.0.1`. Sized to
run beside other workloads under a 4 GiB cgroup slice.

## Licence

MIT.

## What transfers to a ClickHouse deployment with a custom sink

The lab sinks to Postgres because the subject under test is **offset durability**, not
the database. That choice is honest only if it is clear which results carry over.

**Carries over unchanged — these are source and offset mechanics, sink-agnostic:**
S01, S02, S02L, S03, S04, S05, S06, S07, S17, S18, S19, S20, S21, S22. Nothing in them
depends on what the sink is.

**Carries over *more* strongly with a custom sink — S08–S11 are about sink code, and a
custom sink is exactly where these bugs live:**

| lab result | the rule for a custom ClickHouse sink |
|---|---|
| S08 (swallow) loses everything, job stays green | an insert exception must **propagate and fail the task**, never be caught and logged |
| S09 (fail-task) loses nothing | failing fast is the fix; Flink replays from the last committed position |
| S10 (ack before commit) loses silently | never acknowledge a batch before the write is durable — `async_insert=1` with `wait_for_async_insert=0` is this bug |
| S11 (bounded queue drops) loses under load | a full queue must **backpressure**, never drop |
| S12 vs S13 (fan-out) | if the resume point lives in the sink store, it must be a **frontier** (min over branches, written after the ack), never a `max()` |

**Does NOT carry over:**

- **S15** — `ON CONFLICT DO NOTHING` on a unique index. ClickHouse has no unique
  constraint. The nearest equivalent is `ReplacingMergeTree` keyed on the event id, and
  it is **eventual**: duplicates stay visible until a background merge, so every read
  must use `FINAL` or `argMax`. Postgres dedups at write time; ClickHouse does not.
  `insert_deduplication_token` is not a substitute — it only matches identical blocks,
  and any replay shifts batch boundaries.
- **S16 (XA exactly-once)** — deliberately **not built**. It relies on the JDBC driver
  supporting the XA standard and on `max_prepared_transactions > 0`. ClickHouse has
  neither, so the scenario could only produce a result that cannot inform a ClickHouse
  deployment. Exactly-once ends at Kafka; into ClickHouse the target is at-least-once
  plus row-level dedup.

## Evidence

`RESULTS.md` is generated by `harness/report.py` from what each run left on disk —
never written by hand. For every scenario it carries the case definition verbatim, the
initial data the producer emitted (per phase, with id ranges), where the restarted
source actually began reading, the consumer-group state, the per-table outcome and the
raw verdict.

The underlying artefacts are committed under `harness/out/<scenario>/`:

| file | what it is |
|---|---|
| `produced.jsonl` | the producer's ledger — the source of truth for what should have arrived |
| `starting-offsets.log` | the resolved per-partition starting offset on the restart |
| `consumer-group.log` | `kafka-consumer-groups --describe` after the run |
| `submit.log`, `submit-restore.log` | the actual job submissions |
| `verdict.json` | missing / duplicate / extra per table, and the outage window count |

Regenerate with `python3 harness/report.py`.
