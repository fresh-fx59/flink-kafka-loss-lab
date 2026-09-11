#!/usr/bin/env bash
# Bring the whole lab up from a clean checkout.
set -euxo pipefail
cd "$(dirname "$0")"
export PATH=/root/.nix-profile/bin:$PATH
SLICE=${SLICE:-loss-lab.slice}

echo "== 1. build the Flink job jar"
podman run --rm --cgroup-parent="$SLICE" \
  -v "$PWD/job:/src:z" \
  -v loss-lab-m2:/root/.m2 \
  -w /src maven:3.9-eclipse-temurin-17 \
  mvn -B -q clean package
ls -la "$PWD"/job/target/*.jar

echo "== 2. build images + start the stack"
# podman-compose puts every container in a POD, and the pod's cgroup wins over any
# per-container --cgroup-parent. The cap has to be set on the pod.
nix run nixpkgs#podman-compose -- \
  --podman-pod-args="--cgroup-parent=$SLICE" \
  --podman-run-args="--cgroup-parent=$SLICE" \
  up -d --build

echo "== 3. wait for brokers"
sleep 45
podman ps --format "{{.Names}}\t{{.Status}}"

echo "== 4. topics"
./harness/create-topics.sh

echo "== 5. wait for the TaskManager to register"
for i in $(seq 1 40); do
  tm=$(curl -s --max-time 5 http://127.0.0.1:18081/overview | sed -n 's/.*"taskmanagers":\([0-9]*\).*/\1/p')
  [ "${tm:-0}" -ge 1 ] && break
  sleep 5
done
curl -s http://127.0.0.1:18081/overview; echo

echo "== 6. cgroup check"
systemctl show "$SLICE" -p MemoryCurrent -p MemoryMax

echo "BOOTSTRAP OK"
