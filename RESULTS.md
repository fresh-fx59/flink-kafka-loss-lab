# Results

One row per scenario run. `outage.produced` is how many events were produced while the
job was down; `outage.saved` is how many of those are in the database afterwards. Those
two columns are the whole question.

A row is only meaningful once it has been run. Nothing below is filled in from
reasoning.

| scenario | outage.produced | outage.saved | missing | duplicates | prediction | observed | verdict |
|---|---|---|---|---|---|---|---|
| S01 | — | — | — | — | full replay every restart | _not run yet_ | — |
| S02 | — | — | — | — | outage re-read | _not run yet_ | — |
| S03 | — | — | — | — | outage lost | _not run yet_ | — |
| S04 | — | — | — | — | outage lost | _not run yet_ | — |
| S05 | — | — | — | — | outage re-read | _not run yet_ | — |
| S06 | — | — | — | — | outage re-read, exact | _not run yet_ | — |
| S08 | — | — | — | — | outage lost at the sink | _not run yet_ | — |
| S09 | — | — | — | — | outage saved after replay | _not run yet_ | — |
| S10 | — | — | — | — | outage lost, no error | _not run yet_ | — |
| S11 | — | — | — | — | outage partly lost | _not run yet_ | — |
| S14 | — | — | — | — | duplicates present | _not run yet_ | — |
| S15 | — | — | — | — | no duplicates | _not run yet_ | — |

## Not yet implemented

These are specified but the harness does not run them yet. Listed so the gap is
visible rather than implied:

- **S07** — committed-offset expiry (`offsets.retention.minutes=1`); needs a broker
  restart between scenarios.
- **S12 / S13** — fan-out resume from `max(offset)` vs a completion frontier; needs the
  custom `OffsetsInitializer` that reads `ingest_progress`.
- **S16** — XA exactly-once (`JdbcSink.exactlyOnceSink`, `max_prepared_transactions>0`,
  `withTransactionPerConnection(true)`).
- **S17 / S18 / S19 / S20** — the two-hop router shape; needs two jobs running together.
- **S21 / S22** — timestamp positioning, including the Flink 1.17 case where a failed
  time-index lookup starts a partition at its END offset and silently skips its backlog.
