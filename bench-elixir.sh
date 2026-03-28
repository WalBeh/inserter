#!/bin/bash
# Run Elixir benchmark from k8s pod, save results locally
# Usage: ./bench-elixir.sh [shards] [replicas] [output_file]
# Example: ./bench-elixir.sh 5 0 superbench.json

SHARDS=${1:-5}
REPLICAS=${2:-0}
OUTFILE=${3:-superbench.json}
TABLE="bench_s${SHARDS}_r${REPLICAS}_$(date +%H%M%S)"

echo "Running: shards=$SHARDS replicas=$REPLICAS table=$TABLE → $OUTFILE" >&2

kubectl exec -n default elixir -- bash -c \
  "cd /inserter/elixir && mix run -e 'CrateWrite.main()' -- \
  --benchmark \
  --auto-tune \
  --auto-tune-mode rejections \
  --no-compression \
  --table-name $TABLE \
  --duration 5 \
  --threads 128 \
  --batch-size 1000 \
  --batch-interval 0 \
  --shards $SHARDS \
  --replicas $REPLICAS" >> "$OUTFILE" 2>&1

echo "Done. Results appended to $OUTFILE" >&2
