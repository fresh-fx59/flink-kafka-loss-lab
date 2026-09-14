#!/usr/bin/env bash
# Run one TWO-HOP scenario: the operator's Type 1 shape.
#
#   kafka1.events --[job R: filter]--> kafka2.events-routed --[job S]--> Postgres
#
# Both jobs run at once. The scenario kills exactly one of them, keeps producing into
# kafka1, and then restarts it. The question is the same as always: did the events from
# the outage window reach Postgres?
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RUNNER=${RUNNER:-podman}

[ $# -ge 1 ] || { echo "usage: $0 <scenario.env>" >&2; exit 2; }
SCENARIO_FILE="$1"
SCENARIO="$(basename "$SCENARIO_FILE" .env)"

KILL_JOB=router                # router | sink
KAFKA_SINK_GUARANTEE=NONE
ROUTER_CHECKPOINTING_MS=0
SINK_CHECKPOINTING_MS=0
ROUTER_STARTING_OFFSETS=committed-earliest
SINK_STARTING_OFFSETS=committed-earliest
ENABLE_AUTO_COMMIT=true
AUTO_COMMIT_INTERVAL_MS=5000
SINK_MODE=on-conflict-ignore
PK_ON_EVENT_ID=true
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
TS_TYPE=LogAppendTime
DESCRIPTION=""
# shellcheck disable=SC1090
source "$SCENARIO_FILE"

RUN_DIR="$ROOT/harness/out/$SCENARIO"
rm -rf "$RUN_DIR"; mkdir -p "$RUN_DIR"

say() { printf '\n=== %s\n' "$*"; }
pgexec() { $RUNNER exec lab-postgres psql -U lab -d lab -qtAc "$1"; }
harness_py() { $RUNNER exec -w /work lab-harness python3 "$@"; }

job_id_named() {  # $1 = job name substring
  $RUNNER exec lab-jobmanager flink list -r 2>/dev/null \
    | grep -F "$1" | grep -oE '[0-9a-f]{32}' | head -1 || true
}

require_taskmanager() {
  for _ in $(seq 1 30); do
    local tm
    tm=$(curl -s --max-time 5 http://127.0.0.1:18081/overview \
         | sed -n 's/.*"taskmanagers":\([0-9]*\).*/\1/p')
    [ "${tm:-0}" -ge 1 ] && return 0
    $RUNNER start lab-taskmanager >/dev/null 2>&1 || true
    sleep 5
  done
  echo "ABORT: no TaskManager registered" >&2; exit 5
}

submit_router() {
  $RUNNER exec \
    -e "SCENARIO=$SCENARIO" -e SHAPE=router \
    -e "STARTING_OFFSETS=$ROUTER_STARTING_OFFSETS" \
    -e "GROUP_ID=${SCENARIO}-router" \
    -e "ENABLE_AUTO_COMMIT=$ENABLE_AUTO_COMMIT" \
    -e "AUTO_COMMIT_INTERVAL_MS=$AUTO_COMMIT_INTERVAL_MS" \
    -e "CHECKPOINTING_MS=$ROUTER_CHECKPOINTING_MS" \
    -e "KAFKA_SINK_GUARANTEE=$KAFKA_SINK_GUARANTEE" \
    -e "TXN_ID_PREFIX=${SCENARIO}-router" \
    -e "PARALLELISM=$PARALLELISM" -e "TOPIC1=$TOPIC1" -e "TOPIC2=$TOPIC2" \
    lab-jobmanager flink run -d /opt/job/target/loss-lab-job.jar \
    | tee -a "$RUN_DIR/submit-router.log"
}

submit_sink() {
  $RUNNER exec \
    -e "SCENARIO=$SCENARIO" -e SHAPE=sink \
    -e "STARTING_OFFSETS=$SINK_STARTING_OFFSETS" \
    -e "GROUP_ID=${SCENARIO}-sink" \
    -e "ENABLE_AUTO_COMMIT=$ENABLE_AUTO_COMMIT" \
    -e "AUTO_COMMIT_INTERVAL_MS=$AUTO_COMMIT_INTERVAL_MS" \
    -e "CHECKPOINTING_MS=$SINK_CHECKPOINTING_MS" \
    -e "SINK_MODE=$SINK_MODE" -e SINK_FAILURE_MODE=none -e SINK_FAIL_WINDOW=off \
    -e "PARALLELISM=$PARALLELISM" -e "TOPIC1=$TOPIC1" -e "TOPIC2=$TOPIC2" \
    lab-jobmanager flink run -d /opt/job/target/loss-lab-job.jar \
    | tee -a "$RUN_DIR/submit-sink.log"
}

cancel_named() {
  local jid; jid=$(job_id_named "$1")
  [ -n "$jid" ] && $RUNNER exec lab-jobmanager flink cancel "$jid" >/dev/null 2>&1 || true
  for _ in $(seq 1 20); do
    [ -z "$(job_id_named "$1")" ] && return 0
    sleep 2
  done
}

wait_rows_stable() {
  local last=-1 same=0 n
  for _ in $(seq 1 90); do
    n=$(pgexec "SELECT count(*) FROM t_a")
    if [ "$n" = "$last" ]; then same=$((same+1)); else same=0; fi
    last=$n
    [ "$same" -ge 4 ] && return 0
    sleep 2
  done
}

say "$SCENARIO — $DESCRIPTION"
echo "kill=$KILL_JOB routerGuarantee=$KAFKA_SINK_GUARANTEE routerCheckpointMs=$ROUTER_CHECKPOINTING_MS sinkCheckpointMs=$SINK_CHECKPOINTING_MS expect=$EXPECT"
require_taskmanager

say "reset"
GROUP_ID="${SCENARIO}-router" TOPIC1="$TOPIC1" TOPIC2="$TOPIC2" TS_TYPE="$TS_TYPE" \
  "$HERE/reset.sh" >"$RUN_DIR/reset.log" 2>&1
$RUNNER exec lab-kafka2-1 kafka-consumer-groups --bootstrap-server kafka2-1:9092 \
  --delete --group "${SCENARIO}-sink" >/dev/null 2>&1 || true
if [ "$PK_ON_EVENT_ID" = "true" ]; then
  for t in ${TABLES//,/ }; do
    pgexec "CREATE UNIQUE INDEX IF NOT EXISTS ux_${t}_event_id ON ${t}(event_id);" >/dev/null
  done
else
  for t in ${TABLES//,/ }; do pgexec "DROP INDEX IF EXISTS ux_${t}_event_id;" >/dev/null; done
fi

say "phase 1 — produce $BEFORE_COUNT events into kafka1"
harness_py harness/produce.py --bootstrap kafka1-1:9092 --topic "$TOPIC1" \
  --start-id 1 --count "$BEFORE_COUNT" --rate "$RATE" \
  --ledger "/work/harness/out/$SCENARIO/produced.jsonl" --phase before-outage

say "phase 2 — start BOTH jobs (router + sink) and let them catch up"
submit_router; sleep 8; submit_sink
wait_rows_stable
echo "rows after phase 2: t_a=$(pgexec 'SELECT count(*) FROM t_a') t_b=$(pgexec 'SELECT count(*) FROM t_b')"

say "phase 3 — KILL the $KILL_JOB job"
if [ "$KILL_JOB" = "router" ]; then cancel_named "loss-lab-router"; else cancel_named "loss-lab-sink"; fi
$RUNNER exec lab-jobmanager flink list -r 2>&1 | tail -3

say "phase 4 — produce $OUTAGE_COUNT events into kafka1 while it is down"
harness_py harness/produce.py --bootstrap kafka1-1:9092 --topic "$TOPIC1" \
  --start-id $((BEFORE_COUNT + 1)) --count "$OUTAGE_COUNT" --rate "$RATE" \
  --ledger "/work/harness/out/$SCENARIO/produced.jsonl" --phase during-outage

say "phase 5 — restart the $KILL_JOB job"
if [ "$KILL_JOB" = "router" ]; then submit_router; else submit_sink; fi
wait_rows_stable

say "phase 6 — produce $AFTER_COUNT more"
harness_py harness/produce.py --bootstrap kafka1-1:9092 --topic "$TOPIC1" \
  --start-id $((BEFORE_COUNT + OUTAGE_COUNT + 1)) --count "$AFTER_COUNT" --rate "$RATE" \
  --ledger "/work/harness/out/$SCENARIO/produced.jsonl" --phase after-restart
wait_rows_stable

say "evidence"
$RUNNER logs lab-taskmanager 2>&1 | grep -iE "Adding split" | tail -10 \
  | tee "$RUN_DIR/starting-offsets.log" || true
{
  echo "--- kafka1 group ${SCENARIO}-router"
  $RUNNER exec lab-kafka1-1 kafka-consumer-groups --bootstrap-server kafka1-1:9092 \
    --describe --group "${SCENARIO}-router" 2>&1
  echo "--- kafka2 group ${SCENARIO}-sink"
  $RUNNER exec lab-kafka2-1 kafka-consumer-groups --bootstrap-server kafka2-1:9092 \
    --describe --group "${SCENARIO}-sink" 2>&1
  echo "--- how many records actually reached kafka2"
  $RUNNER exec lab-kafka2-1 kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka2-1:9092 --topic "$TOPIC2" 2>&1
} | tee "$RUN_DIR/consumer-group.log"

say "verdict"
cancel_named "loss-lab-router"; cancel_named "loss-lab-sink"
UNIQ_FLAG=""; [ "$UNIQUE_REQUIRED" = "true" ] && UNIQ_FLAG="--unique-required"
set +e
harness_py harness/verify.py \
  --ledger "/work/harness/out/$SCENARIO/produced.jsonl" \
  --dsn "postgresql://lab:lab@postgres:5432/lab" \
  --tables "$TABLES" --out "/work/harness/out/$SCENARIO/verdict.json" \
  --scenario "$SCENARIO" --expect "$EXPECT" $UNIQ_FLAG
RC=$?
set -e
echo
[ $RC -eq 0 ] && echo "SCENARIO $SCENARIO: PASS (observed matched the prediction)" \
              || echo "SCENARIO $SCENARIO: FAIL (observed did NOT match the prediction — record it)"
exit $RC
