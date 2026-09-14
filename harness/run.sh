#!/usr/bin/env bash
# Run one scenario end to end and write a verdict.
#
#   ./harness/run.sh harness/scenarios/S06.env
#
# Shape of every scenario, matching the production incident:
#   produce -> job runs and catches up -> KILL the job -> keep producing
#   -> RESTART the job -> did the events from the outage window get saved?
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RUNNER=${RUNNER:-podman}

[ $# -ge 1 ] || { echo "usage: $0 <scenario.env>" >&2; exit 2; }
SCENARIO_FILE="$1"
SCENARIO="$(basename "$SCENARIO_FILE" .env)"

# ---- defaults, overridden by the scenario file -------------------------------
SHAPE=direct
STARTING_OFFSETS=committed-earliest
# Some scenarios restart with a DIFFERENT startup mode than they started with - that is
# what a real recovery looks like. The literal OUTAGE_START is substituted with the
# wall-clock millisecond at which the job was killed.
RESTART_STARTING_OFFSETS=""
TS_TYPE=LogAppendTime
OUTAGE_TIMESTAMP_SHIFT_MS=0
# S07: shrink the broker's committed-offset retention so a short outage outlives it.
# This is NOT a dynamic broker config, so the kafka1 brokers must be recreated.
OFFSETS_RETENTION_MINUTES=""
OUTAGE_SLEEP_SECONDS=0
SINK_FAIL_TABLE=t_b
GROUP_ID=loss-lab
ENABLE_AUTO_COMMIT=false
AUTO_COMMIT_INTERVAL_MS=5000
CHECKPOINTING_MS=0
RESTART_FROM_CHECKPOINT=false
SINK_MODE=plain-insert
SINK_FAILURE_MODE=none
SINK_FAIL_SECONDS=0
PK_ON_EVENT_ID=false
WRITE_PROGRESS=false
KAFKA_SINK_GUARANTEE=NONE
PARALLELISM=2
EXPECT=no-loss
UNIQUE_REQUIRED=false
BEFORE_COUNT=600
OUTAGE_COUNT=900
AFTER_COUNT=300
RATE=60
TOPIC1=events
TOPIC2=events-routed
TABLES=t_a,t_b,t_rare
DESCRIPTION=""
# shellcheck disable=SC1090
source "$SCENARIO_FILE"

RUN_DIR="$ROOT/harness/out/$SCENARIO"
rm -rf "$RUN_DIR"; mkdir -p "$RUN_DIR"
LEDGER="$RUN_DIR/produced.jsonl"

say() { printf '\n=== %s\n' "$*"; }

pgexec() { $RUNNER exec lab-postgres psql -U lab -d lab -qtAc "$1"; }

harness_py() {
  $RUNNER exec -w /work lab-harness python3 "$@"
}

flink() { $RUNNER exec "$@" lab-jobmanager flink "${@:2}"; }

job_env_args() {
  cat <<EOF
-e SCENARIO=$SCENARIO
-e SHAPE=$SHAPE
-e STARTING_OFFSETS=${EFFECTIVE_STARTING_OFFSETS:-$STARTING_OFFSETS}
-e GROUP_ID=$GROUP_ID
-e ENABLE_AUTO_COMMIT=$ENABLE_AUTO_COMMIT
-e AUTO_COMMIT_INTERVAL_MS=$AUTO_COMMIT_INTERVAL_MS
-e CHECKPOINTING_MS=$CHECKPOINTING_MS
-e SINK_MODE=$SINK_MODE
-e SINK_FAILURE_MODE=$SINK_FAILURE_MODE
-e WRITE_PROGRESS=$WRITE_PROGRESS
-e SINK_FAIL_TABLE=$SINK_FAIL_TABLE
-e KAFKA_SINK_GUARANTEE=$KAFKA_SINK_GUARANTEE
-e PARALLELISM=$PARALLELISM
-e TOPIC1=$TOPIC1
-e TOPIC2=$TOPIC2
EOF
}

submit() {  # $1 = extra flink args (e.g. -s <checkpoint>)
  local extra="${1:-}"
  local failwindow="off"
  if [ "$SINK_FAIL_SECONDS" -gt 0 ]; then
    local now_ms; now_ms=$(( $(date +%s) * 1000 ))
    failwindow="${now_ms}-$(( now_ms + SINK_FAIL_SECONDS * 1000 ))"
  fi
  # shellcheck disable=SC2046
  $RUNNER exec $(job_env_args | tr '\n' ' ') -e "SINK_FAIL_WINDOW=$failwindow" \
    lab-jobmanager flink run -d $extra /opt/job/target/loss-lab-job.jar \
    | tee "$RUN_DIR/submit$( [ -n "$extra" ] && echo "-restore" ).log"
}

job_id() {
  $RUNNER exec lab-jobmanager flink list -r 2>/dev/null \
    | grep -oE '[0-9a-f]{32}' | head -1 || true
}

wait_for_running() {
  for _ in $(seq 1 60); do
    [ -n "$(job_id)" ] && return 0
    sleep 2
  done
  echo "job never reached RUNNING" >&2; return 1
}

wait_for_rows_stable() {   # wait until the row count stops moving
  local last=-1 same=0
  for _ in $(seq 1 90); do
    local n; n=$(pgexec "SELECT count(*) FROM t_a")
    if [ "$n" = "$last" ]; then same=$((same+1)); else same=0; fi
    last=$n
    [ "$same" -ge 4 ] && return 0
    sleep 2
  done
}

cancel_job() {
  local jid; jid=$(job_id)
  if [ -n "$jid" ]; then
    $RUNNER exec lab-jobmanager flink cancel "$jid" >/dev/null 2>&1 || true
  fi
  for _ in $(seq 1 30); do
    [ -z "$(job_id)" ] && return 0
    sleep 2
  done
}

latest_checkpoint() {
  $RUNNER exec lab-jobmanager sh -lc \
    'ls -1dt /checkpoints/*/chk-* 2>/dev/null | head -1' || true
}

# ------------------------------------------------------------------ run -------
# Never submit into a cluster with no slots: the run would "complete" and report a
# result that means nothing.
require_taskmanager() {
  for _ in $(seq 1 30); do
    local tm
    tm=$(curl -s --max-time 5 http://127.0.0.1:18081/overview \
         | sed -n 's/.*"taskmanagers":\([0-9]*\).*/\1/p')
    [ "${tm:-0}" -ge 1 ] && return 0
    $RUNNER start lab-taskmanager >/dev/null 2>&1 || true
    sleep 5
  done
  echo "ABORT: no TaskManager registered - the cluster has no slots" >&2
  exit 5
}

say "$SCENARIO — $DESCRIPTION"
echo "expect=$EXPECT startingOffsets=$STARTING_OFFSETS autoCommit=$ENABLE_AUTO_COMMIT checkpointMs=$CHECKPOINTING_MS sinkFailure=$SINK_FAILURE_MODE"

require_taskmanager

if [ -n "$OFFSETS_RETENTION_MINUTES" ]; then
  say "recreating the kafka1 brokers with offsets.retention.minutes=$OFFSETS_RETENTION_MINUTES"
  ( cd "$ROOT" && KAFKA1_OFFSETS_RETENTION_MINUTES="$OFFSETS_RETENTION_MINUTES" \
      nix run nixpkgs#podman-compose -- up -d --force-recreate \
      kafka1-1 kafka1-2 kafka1-3 ) >"$RUN_DIR/broker-recreate.log" 2>&1
  sleep 35
fi

say "reset"
GROUP_ID="$GROUP_ID" TOPIC1="$TOPIC1" TOPIC2="$TOPIC2" TS_TYPE="$TS_TYPE" \
  "$HERE/reset.sh" >"$RUN_DIR/reset.log" 2>&1

if [ "$PK_ON_EVENT_ID" = "true" ]; then
  for t in ${TABLES//,/ }; do
    pgexec "CREATE UNIQUE INDEX IF NOT EXISTS ux_${t}_event_id ON ${t}(event_id);" >/dev/null
  done
  echo "unique index on event_id: ON"
else
  for t in ${TABLES//,/ }; do
    pgexec "DROP INDEX IF EXISTS ux_${t}_event_id;" >/dev/null
  done
  echo "unique index on event_id: OFF"
fi

say "phase 1 — produce $BEFORE_COUNT events BEFORE the outage"
harness_py harness/produce.py --bootstrap kafka1-1:9092 --topic "$TOPIC1" \
  --start-id 1 --count "$BEFORE_COUNT" --rate "$RATE" \
  --ledger "/work/harness/out/$SCENARIO/produced.jsonl" --phase before-outage

say "phase 2 — start the job and let it catch up"
submit ""
wait_for_running
wait_for_rows_stable
echo "rows after phase 2: t_a=$(pgexec 'SELECT count(*) FROM t_a') t_b=$(pgexec 'SELECT count(*) FROM t_b')"

say "phase 3 — KILL the job (this is the outage)"
OUTAGE_START_MS=$(( $(date +%s) * 1000 ))
echo "outage starts at epoch ms $OUTAGE_START_MS"
cancel_job
echo "job list after cancel: $($RUNNER exec lab-jobmanager flink list -r 2>&1 | tail -2)"

say "phase 4 — produce $OUTAGE_COUNT events WHILE THE JOB IS DOWN"
harness_py harness/produce.py --bootstrap kafka1-1:9092 --topic "$TOPIC1" \
  --start-id $((BEFORE_COUNT + 1)) --count "$OUTAGE_COUNT" --rate "$RATE" \
  --timestamp-shift-ms "$OUTAGE_TIMESTAMP_SHIFT_MS" \
  --ledger "/work/harness/out/$SCENARIO/produced.jsonl" --phase during-outage

if [ "$OUTAGE_SLEEP_SECONDS" -gt 0 ]; then
  say "holding the outage open for ${OUTAGE_SLEEP_SECONDS}s (long enough for the committed offsets to expire)"
  sleep "$OUTAGE_SLEEP_SECONDS"
  $RUNNER exec lab-kafka1-1 kafka-consumer-groups --bootstrap-server kafka1-1:9092 \
    --describe --group "$GROUP_ID" 2>&1 | tee "$RUN_DIR/group-after-expiry.log" || true
fi

say "phase 5 — restart the job"
if [ -n "$RESTART_STARTING_OFFSETS" ]; then
  EFFECTIVE_STARTING_OFFSETS="${RESTART_STARTING_OFFSETS/OUTAGE_START/$OUTAGE_START_MS}"
  echo "restart startup mode: $EFFECTIVE_STARTING_OFFSETS"
fi
RESTORE_ARG=""
if [ "$RESTART_FROM_CHECKPOINT" = "true" ]; then
  CP="$(latest_checkpoint)"
  if [ -z "$CP" ]; then
    echo "RESTART_FROM_CHECKPOINT=true but no retained checkpoint found" >&2
    exit 3
  fi
  echo "restoring from $CP"
  RESTORE_ARG="-s $CP"
fi
submit "$RESTORE_ARG"
wait_for_running
wait_for_rows_stable

say "phase 6 — produce $AFTER_COUNT events after the restart"
harness_py harness/produce.py --bootstrap kafka1-1:9092 --topic "$TOPIC1" \
  --start-id $((BEFORE_COUNT + OUTAGE_COUNT + 1)) --count "$AFTER_COUNT" --rate "$RATE" \
  --ledger "/work/harness/out/$SCENARIO/produced.jsonl" --phase after-restart
wait_for_rows_stable

say "evidence — what the source actually started from"
$RUNNER logs lab-taskmanager 2>&1 | grep -iE "Adding split|startingOffset|KafkaPartitionSplit" \
  | tail -20 | tee "$RUN_DIR/starting-offsets.log" || true
$RUNNER exec lab-kafka1-1 kafka-consumer-groups --bootstrap-server kafka1-1:9092 \
  --describe --group "$GROUP_ID" 2>&1 | tee "$RUN_DIR/consumer-group.log" || true

say "verdict"
cancel_job
UNIQ_FLAG=""
[ "$UNIQUE_REQUIRED" = "true" ] && UNIQ_FLAG="--unique-required"
set +e
harness_py harness/verify.py \
  --ledger "/work/harness/out/$SCENARIO/produced.jsonl" \
  --dsn "postgresql://lab:lab@postgres:5432/lab" \
  --tables "$TABLES" \
  --out "/work/harness/out/$SCENARIO/verdict.json" \
  --scenario "$SCENARIO" --expect "$EXPECT" $UNIQ_FLAG
RC=$?
set -e
echo
if [ $RC -eq 0 ]; then echo "SCENARIO $SCENARIO: PASS (observed matched the prediction)"
else echo "SCENARIO $SCENARIO: FAIL (observed did NOT match the prediction — this is a finding, record it)"; fi
exit $RC
