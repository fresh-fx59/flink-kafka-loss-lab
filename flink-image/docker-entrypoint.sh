#!/usr/bin/env bash
set -euo pipefail

CONF="$FLINK_HOME/conf/flink-conf.yaml"
JAVA17_OPTS="$(cat /opt/java17-opts.txt)"

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

case "${1:-help}" in
  jobmanager)
    exec gosu flink "$FLINK_HOME/bin/jobmanager.sh" start-foreground
    ;;
  taskmanager)
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
