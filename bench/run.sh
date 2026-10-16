#!/bin/sh
# Reproduce docs/benchmarks.md: bench/run.sh [DATE]
set -eu
D=${1:-$(date +%Y-%m-%d)}
OUT=bench/results/$D
mkdir -p "$OUT"
cargo build --release --bins
S=target/release/nebula-sim
for load in 0.8 0.9 1.0 1.1; do
  $S --jobs 4000 --seeds 3 --load $load --out "$OUT/sim_load$load.json"
done
$S --jobs 4000 --seeds 3 --load 0.9 --mtbf-h 100 --out "$OUT/sim_failures.json"
$S --jobs 4000 --seeds 3 --load 1.0 --no-penalty --out "$OUT/sim_nopenalty.json"
$S --jobs 20000 --seeds 1 --load 1.0 --nodes 256 --out "$OUT/sim_large.json"
B=target/release/nebula-bench
$B --nodes 64 --clients 16 --seconds 20 --rate 50 --out "$OUT/api_steady.json"
$B --nodes 256 --clients 16 --seconds 20 --rate 100 --out "$OUT/api_256nodes.json"
$B --nodes 64 --clients 16 --seconds 20 --out "$OUT/api_flood.json"
python3 bench/report.py "$D" > docs/benchmarks.md
