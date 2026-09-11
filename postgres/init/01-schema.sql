-- Loss-lab schema.
-- Three destination tables so fan-out (one topic -> several tables) is real:
--   t_a, t_b   : routes 'a' and 'b', roughly half the stream each
--   t_rare     : route 'rare', once per 500 events, so one table is legitimately
--                idle for long stretches (the lagging-table case)
--
-- event_id is NOT a primary key by default. Scenarios that test idempotency add the
-- unique index themselves (harness/run.sh, PK_ON_EVENT_ID=true), because the
-- plain-insert baseline must be allowed to produce duplicates.

CREATE TABLE IF NOT EXISTS t_a (
    id          BIGSERIAL PRIMARY KEY,
    event_id    BIGINT      NOT NULL,
    produced_at BIGINT      NOT NULL,
    route       TEXT        NOT NULL,
    src_topic   TEXT        NOT NULL,
    src_part    INT         NOT NULL,
    src_offset  BIGINT      NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS t_b    (LIKE t_a INCLUDING ALL);
CREATE TABLE IF NOT EXISTS t_rare (LIKE t_a INCLUDING ALL);

-- Approach A's completion frontier: one row per (job, topic, sink_table, partition).
-- Written only AFTER that branch's data insert is acknowledged.
CREATE TABLE IF NOT EXISTS ingest_progress (
    job         TEXT   NOT NULL,
    topic       TEXT   NOT NULL,
    sink_table  TEXT   NOT NULL,
    partition   INT    NOT NULL,
    max_offset  BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job, topic, sink_table, partition)
);

CREATE INDEX IF NOT EXISTS ix_t_a_event_id    ON t_a    (event_id);
CREATE INDEX IF NOT EXISTS ix_t_b_event_id    ON t_b    (event_id);
CREATE INDEX IF NOT EXISTS ix_t_rare_event_id ON t_rare (event_id);
