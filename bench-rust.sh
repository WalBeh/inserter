#!/bin/bash
# Run Rust benchmark from k8s pod, save results locally
# Usage: ./bench-rust.sh <connection-string> [shards] [replicas] [output_file]
# Example: ./bench-rust.sh http://crate@localhost:4200 5 0 superbench.json

if [ -z "$1" ]; then
  echo "Usage: $0 <connection-string> [shards] [replicas] [output_file]" >&2
  echo "Example: $0 http://crate@localhost:4200 5 0 results.json" >&2
  exit 1
fi

CONN=$1
SHARDS=${2:-5}
REPLICAS=${3:-0}
OUTFILE=${4:-superbench.json}
TABLE="bench_rs_s${SHARDS}_r${REPLICAS}_$(date +%H%M%S)"

echo "Running: cluster=$CONN shards=$SHARDS replicas=$REPLICAS table=$TABLE → $OUTFILE" >&2

kubectl exec -n default rust -- bash -c \
  "cd /inserter/rust && cargo run --release -- \
  --benchmark \
  --no-compression \
  --connection-string '$CONN' \
  --table-name $TABLE \
  --duration 5 \
  --threads 128 \
  --batch-size 1000 \
  --batch-interval 0 \
  --shards $SHARDS \
  --replicas $REPLICAS" >> "$OUTFILE" 2>&1

echo "Done. Results appended to $OUTFILE" >&2
