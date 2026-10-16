#!/usr/bin/env bash
# Owner-only: validate the real-GPU path on a machine with an NVIDIA GPU
# (written for the RTX 4060). Needs: NVIDIA driver, Docker, and the NVIDIA
# Container Toolkit (`docker run --rm --gpus all ubuntu nvidia-smi` must work).
#
#   cargo build --release && scripts/owner_gpu_check.sh
#
# Saves everything to docs/evidence/owner-gpu-<date>/. Not run by the author:
# the build machine has no GPU.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=docs/evidence/owner-gpu-$(date +%F)
IMAGE=${IMAGE:-nvidia/cuda:12.4.1-base-ubuntu22.04}
PORT=${PORT:-7171}
mkdir -p "$OUT"
WORK=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$WORK"' EXIT

echo "== 1. nvidia-smi output (add it to the parser fixture in src/gpu.rs)"
nvidia-smi --query-gpu=index,name,memory.total,uuid --format=csv,noheader,nounits | tee "$OUT/nvidia-smi-query.csv"
nvidia-smi > "$OUT/nvidia-smi.txt"
MODEL=$(head -1 "$OUT/nvidia-smi-query.csv" | cut -d, -f2 | sed 's/^ *//;s/ *$//')
docker pull "$IMAGE" > /dev/null

cat > "$WORK/nebulad.json" <<JSON
{"listen": "127.0.0.1:$PORT", "data_dir": "$WORK/state", "admin_token": "owner-admin",
 "node_token": "owner-node", "heartbeat_timeout_s": 10, "tick_ms": 200,
 "tenants": [{"name": "research", "api_key": "owner-research", "weight": 1.0}]}
JSON
./target/release/nebulad --config "$WORK/nebulad.json" > "$OUT/nebulad.log" 2>&1 &
sleep 1
./target/release/nebula-agent --server "http://127.0.0.1:$PORT" --token owner-node --id gpu-node \
  --gpu-source nvidia --runtime docker --image "$IMAGE" --default-memory-mb 2048 \
  --work-dir "$WORK/agent" > "$OUT/agent.log" 2>&1 &
sleep 3

echo "== 2. registered GPUs (expect the real name, memory_mb and uuid)"
curl -sf -H 'Authorization: Bearer owner-admin' "http://127.0.0.1:$PORT/v1/admin/cluster" | tee "$OUT/cluster.json"; echo

echo "== 3. one single-GPU job in a container"
ID=$(curl -sf -H 'Authorization: Bearer owner-research' -H 'content-type: application/json' \
  -d "{\"gpus\": 1, \"gpu_model\": \"$MODEL\", \"est_runtime\": 60, \"memory_mb\": 2048,
       \"command\": [\"sh\", \"-c\", \"nvidia-smi -L; echo CUDA_VISIBLE_DEVICES=\$CUDA_VISIBLE_DEVICES NEBULA_HOST_GPUS=\$NEBULA_HOST_GPUS\"]}" \
  "http://127.0.0.1:$PORT/v1/jobs" | sed 's/[^0-9]//g')
for _ in $(seq 60); do
  S=$(curl -sf -H 'Authorization: Bearer owner-research' "http://127.0.0.1:$PORT/v1/jobs/$ID")
  echo "$S" | grep -q '"state":"\(succeeded\|failed\)"' && break
  sleep 1
done
echo "$S" | tee "$OUT/job.json"; echo
cat "$WORK"/agent/job-"$ID"-a0.log | tee "$OUT/job.log"
echo "$S" | grep -q '"state":"succeeded"' && grep -q "GPU 0:" "$OUT/job.log" \
  && echo "PASS: real GPU job ran in a container" | tee "$OUT/RESULT" \
  || { echo "FAIL" | tee "$OUT/RESULT"; exit 1; }
