# CrateDB Record Generator — Rust Implementation

High-performance CrateDB record generator using tokio async runtime, reqwest HTTP client, and gzip compression.

## Prerequisites

Rust 1.70+ and a C compiler with OpenSSL headers:

```bash
# macOS (usually pre-installed)
xcode-select --install

# Amazon Linux / RHEL / Fedora
sudo yum install -y gcc openssl-devel

# Ubuntu / Debian
sudo apt install -y build-essential libssl-dev pkg-config

# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
```

### Kubernetes / Docker
```bash
# Run a pod with Rust pre-installed
kubectl run rust --image rust:slim -- sleep infinity
kubectl exec -it rust -- bash

# Inside the pod:
apt update && apt install -y git gcc libssl-dev pkg-config
git clone https://github.com/WalBeh/inserter.git && cd inserter/rust
cargo build --release

# Run benchmark
./target/release/crate-write --benchmark --no-compression \
  --connection-string "http://crate@your-cratedb-host:4200" \
  --table-name bench --duration 5 --threads 128 --batch-size 1000 \
  --batch-interval 0 --shards 5 --replicas 0

# When done:
# kubectl delete pod rust
```

## Build & Run

```bash
cd rust

# Set up connection
echo 'CRATE_CONNECTION_STRING=https://admin:password@your-cluster:4200' > .env

# Create config (optional — auto-detected if present)
cat > config.toml << 'EOF'
table_name = "performance_test"
duration = 10
batch_size = 1000
batch_interval = 0
threads = 128
objects = 0
dashboard = false
log_level = "info"
EOF

# Run with config.toml defaults
cargo run

# Or specify everything on the CLI
cargo run -- --table-name stress_test --duration 5 --threads 128 --batch-size 1000 --batch-interval 0

# Release build for max performance
cargo build --release
./target/release/crate-write
```

## CLI Options

```
--table-name <NAME>           Table to create/insert into
--duration <MINUTES>          Minutes to run
--connection-string <URL>     CrateDB URL (overrides .env)
--batch-size <SIZE>           Records per bulk insert
--batch-interval <MS>         Milliseconds between batches (0 = none)
--threads <COUNT>             Concurrent async worker tasks
--objects <COUNT>             Extra low-cardinality TEXT columns
--test-loadbalancer           Run 5-tuple load balancer test and exit
--benchmark                   Minimal output, JSONL result to stdout
--log-level <LEVEL>           error, warn, info, debug, trace
--config <FILE>               Config file path (.toml or .json)
```

Config file values are defaults — CLI args override only when explicitly provided. Connection string comes from `.env` or `--connection-string`.

## Architecture

### Worker Loop

```
spawn_blocking(generate_batch + into_params)  →  spawn_blocking(serialize + gzip)  →  await HTTP POST  →  repeat
```

- **CPU work** (record generation, JSON serialization, gzip compression) runs on tokio's blocking thread pool via `spawn_blocking`
- **HTTP I/O** runs on tokio's async thread pool, free to multiplex many concurrent requests
- **Monitor** uses `AtomicU64` counters — zero lock contention between workers
- **Payloads** are gzip-compressed (flate2 fast / level 1, ~88% size reduction)

This separation is critical for scaling: without `spawn_blocking`, CPU work blocks the tokio I/O threads and throughput drops at high concurrency (18K → 31K rec/sec improvement at 64 threads).

### Connection Pool

Sized to `max(threads + 2, 10)` with 60s keepalive and 60s request timeout.

## Benchmark Mode

```bash
cargo run -- --benchmark --table-name bench --duration 2 --threads 128 --batch-size 1000 --batch-interval 0 >> results.json
```

- Suppresses progress logs and load balancer test
- Queries cluster info from `sys.nodes` (CPUs, memory, disk, heap, version)
- Collects rate samples every 10s for percentile stats
- Outputs single-line JSON to stdout (`client: "rust-http"`)
- Prints summary to stderr: `CrateDB 6.2.1 | rec/s per CPU: avg=2478 p95=3583 max=3583`

Multiple runs append as JSONL (one JSON object per line).

## Performance

Tested against a 3-node CrateDB Cloud cluster (~150ms latency):

| Threads | Batch Size | Interval | Throughput |
|---------|-----------|----------|------------|
| 32 | 1000 | 0 | ~31,000 rec/sec |
| 64 | 1000 | 0 | ~31,000 rec/sec |
| **128** | **1000** | **0** | **~34,000 rec/sec** |

At high concurrency the bottleneck is CrateDB ingestion, not the client.

## Tests

```bash
cargo test    # 19 tests
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| tokio | Async runtime + spawn_blocking |
| reqwest | HTTP client with connection pooling |
| flate2 | Gzip compression |
| clap | CLI argument parsing |
| serde / serde_json | JSON serialization |
| fake | Random data generation |
| chrono | Timestamps |
| tracing | Structured logging |
| native-tls / tokio-native-tls | TLS for load balancer test |
| dotenvy | .env file loading |
| toml | Config file parsing |

## License

MIT
