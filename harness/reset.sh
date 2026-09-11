#!/usr/bin/env bash
# Between scenarios: wipe the destination tables, delete the topics and the consumer
# group, and recreate the topics. Leaves the containers running.
set -euo pipefail
RUNNER=${RUNNER:-podman}
TOPIC1=${TOPIC1:-events}
TOPIC2=${TOPIC2:-events-routed}
GROUP_ID=${GROUP_ID:-loss-lab}

$RUNNER exec lab-postgres psql -U lab -d lab -c \
  "TRUNCATE t_a, t_b, t_rare, ingest_progress;"

for t in "$TOPIC1"; do
  $RUNNER exec lab-kafka1-1 kafka-topics --bootstrap-server kafka1-1:9092 --delete --topic "$t" || true
done
$RUNNER exec lab-kafka2-1 kafka-topics --bootstrap-server kafka2-1:9092 --delete --topic "$TOPIC2" || true
$RUNNER exec lab-kafka1-1 kafka-consumer-groups --bootstrap-server kafka1-1:9092 --delete --group "$GROUP_ID" || true
$RUNNER exec lab-kafka2-1 kafka-consumer-groups --bootstrap-server kafka2-1:9092 --delete --group "$GROUP_ID" || true
sleep 3
"$(dirname "$0")/create-topics.sh"
echo "reset done"
