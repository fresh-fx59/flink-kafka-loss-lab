#!/usr/bin/env bash
set -euo pipefail

CONF="$FLINK_HOME/conf/flink-conf.yaml"
JAVA17_OPTS="$(cat /opt/java17-opts.txt)"

# The stock Flink 1.17 distribution binds its RPC to localhost:
#   jobmanager.bind-host: localhost
#   taskmanager.bind-host: localhost
#   taskmanager.host: localhost
#   rest.bind-address: localhost
# That is correct for a laptop tarball and fatal in containers - the TaskManager gets
# "Connection refused" to jobmanager:6123 forever. The official images strip these;
# because this image is built from the tarball, we strip them here.
sed -i -E 's/^[[:space:]]*(jobmanager\.bind-host|taskmanager\.bind-host|taskmanager\.host|rest\.bind-address):.*/# &/' "$CONF"

# Flink 1.17 has no env.java.opts default; without these, Java 17 fails on JDK
# module access. Written once, before any Flink process starts.
if ! grep -q '^env.java.opts:' "$CONF"; then
  {
    echo ""
    echo "# --- added by the loss lab: Java 17 module access (verbatim from Flink 1.18) ---"
    echo "env.java.opts: ${JAVA17_OPTS}"
  } >> "$CONF"
fi

# FLINK_PROPERTIES is the standard override channel of the official images.
if [ -n "${FLINK_PROPERTIES:-}" ]; then
  echo "" >> "$CONF"
  echo "# --- from FLINK_PROPERTIES ---" >> "$CONF"
  echo "${FLINK_PROPERTIES}" >> "$CONF"
fi

# The checkpoint volume is created root-owned, but Flink runs as `flink`. Without this
# the JobManager dies at submit with "Failed to create directory for shared state",
# which only affects the checkpointing scenarios - i.e. exactly the ones that are
# supposed to prove the fix works.
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/checkpoints}"
if [ -d "$CHECKPOINT_ROOT" ]; then
  chown -R flink:flink "$CHECKPOINT_ROOT" || true
fi

case "${1:-help}" in
  jobmanager)
    {
      echo "jobmanager.bind-host: 0.0.0.0"
      echo "rest.bind-address: 0.0.0.0"
    } >> "$CONF"
    exec gosu flink "$FLINK_HOME/bin/jobmanager.sh" start-foreground
    ;;
  taskmanager)
    {
      echo "taskmanager.bind-host: 0.0.0.0"
      echo "taskmanager.host: $(hostname)"
    } >> "$CONF"
    exec gosu flink "$FLINK_HOME/bin/taskmanager.sh" start-foreground
    ;;
  help)
    echo "usage: jobmanager | taskmanager | <any command>"
    exit 0
    ;;
  *)
    exec "$@"
    ;;
esac
