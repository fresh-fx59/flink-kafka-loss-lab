#!/usr/bin/env bash
# Topics are created explicitly (auto-create is off) so partition count and replication
# factor are part of the experiment, not an accident.
set -euo pipefail
RUNNER=${RUNNER:-podman}
PARTITIONS=${PARTITIONS:-3}
TOPIC1=${TOPIC1:-events}
TOPIC2=${TOPIC2:-events-routed}
# LogAppendTime makes the broker stamp the timestamp, so the time index is monotonic
# and OffsetsInitializer.timestamp() is exact. CreateTime (the default) is the
# producer's clock and can make that lookup return a wrong offset.
TS_TYPE=${TS_TYPE:-LogAppendTime}

$RUNNER exec lab-kafka1-1 kafka-topics --bootstrap-server kafka1-1:9092 \
  --create --if-not-exists --topic "$TOPIC1" \
  --partitions "$PARTITIONS" --replication-factor 3 \
  --config min.insync.replicas=2 --config message.timestamp.type="$TS_TYPE"

$RUNNER exec lab-kafka2-1 kafka-topics --bootstrap-server kafka2-1:9092 \
  --create --if-not-exists --topic "$TOPIC2" \
  --partitions "$PARTITIONS" --replication-factor 2 \
  --config message.timestamp.type="$TS_TYPE"

echo "--- kafka1 ---"
$RUNNER exec lab-kafka1-1 kafka-topics --bootstrap-server kafka1-1:9092 --describe --topic "$TOPIC1"
echo "--- kafka2 ---"
$RUNNER exec lab-kafka2-1 kafka-topics --bootstrap-server kafka2-1:9092 --describe --topic "$TOPIC2"
