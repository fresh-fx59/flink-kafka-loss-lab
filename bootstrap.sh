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
  mvn -B -q clean package  # target/ is recreated; compose mounts ./job, not ./job/target
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

echo "== 6. apply the memory cap to the pod's real cgroup"
# podman-compose puts every container in a pod, and podman with the systemd cgroup
# manager always parents that pod under machine.slice - --cgroup-parent on the pod is
# silently ignored. The only cgroup that actually holds the containers is the pod slice,
# so the cap goes there. Verified, not assumed: MemoryCurrent below must be non-trivial.
PODID=$(podman pod inspect pod_loss-lab --format "{{.ID}}")
POD_SLICE="machine-libpod_pod_${PODID}.slice"
systemctl set-property --runtime "$POD_SLICE" \
  MemoryMax=4G MemoryHigh=3800M CPUQuota=250%
systemctl show "$POD_SLICE" -p MemoryCurrent -p MemoryMax -p CPUQuotaPerSecUSec

CUR=$(systemctl show "$POD_SLICE" -p MemoryCurrent --value)
if [ "${CUR:-0}" -lt 104857600 ]; then
  echo "REFUSING: the cap is not on the cgroup that holds the containers" >&2
  exit 4
fi

echo "BOOTSTRAP OK"
