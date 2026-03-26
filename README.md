# CrateDB Record Generator

High-performance record generators for CrateDB. Available in Python (async) and Rust, both optimized for maximum insert throughput.

## Implementations

| | Python (async) | Rust |
|---|---|---|
| **Engine** | asyncio + aiohttp | tokio + reqwest + spawn_blocking |
| **Concurrency** | Async tasks (no GIL) | Async I/O + blocking thread pool |
| **Compression** | gzip level 1 | flate2 fast |
| **Throughput** | ~33,000 rec/sec | ~34,000 rec/sec |
| **Memory** | ~30-100MB | ~5-50MB |
| **Setup** | `uv run crate-write` | `cargo run` |

Both share the same CLI interface, table schema, and record format. Throughput numbers from a 3-node CrateDB Cloud cluster (cr2, 2 TiB EBS gp3, AWS NLB, us-east-1). Actual throughput depends on network latency and bandwidth between client and cluster.

## Quick Start

### Python

```bash
# Requires uv (https://astral.sh/uv)
uv venv && source .venv/bin/activate
uv pip install -e .

# Configure connection
echo 'CRATE_CONNECTION_STRING=https://admin:password@your-cluster:4200' > .env

# Run (1 minute, 64 async tasks, batch size 1000, no delay)
uv run crate-write --table-name test_events --duration 1 --threads 64 --batch-size 1000 --batch-interval 0
```

### Rust

```bash
cd rust

# Configure connection and config
echo 'CRATE_CONNECTION_STRING=https://admin:password@your-cluster:4200' > .env
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

# Run (reads config.toml automatically)
cargo run
```

## Configuration

### Environment Variables

Create a `.env` file in the project root (Python) or `rust/` directory (Rust):

```env
CRATE_CONNECTION_STRING=https://admin:password@your-cluster:4200
LOG_LEVEL=INFO
```

### Rust Config File

The Rust implementation auto-detects `rust/config.toml` (gitignored — may contain credentials). CLI arguments override config file values when explicitly provided.

## CLI Options

Both implementations accept the same flags:

```
--table-name TEXT       Table to create/insert into (required)
--duration INTEGER      Minutes to run (required)
--connection-string     CrateDB URL (overrides .env)
--batch-size            Records per bulk insert (default: 100)
--batch-interval        Delay between batches: seconds (Python) / ms (Rust)
--threads               Concurrent async tasks (default: 1)
--objects               Extra low-cardinality TEXT columns (default: 0)
--test-loadbalancer     Run 5-tuple load balancer test and exit
--benchmark             Minimal output, JSONL result to stdout
```

## Benchmark Mode

Both implementations support `--benchmark` for structured, machine-readable output:

```bash
# Run benchmark, append JSON result to file
uv run crate-write --benchmark --table-name bench1 --duration 2 --threads 64 --batch-size 1000 --batch-interval 0 >> results.json
cargo run -- --benchmark --table-name bench2 --duration 2 --threads 128 --batch-size 1000 --batch-interval 0 >> results.json
```

In benchmark mode:
- Progress logs and load balancer test are suppressed
- Cluster info is queried from `sys.nodes` (CPUs, memory, disk, heap, version)
- Rate samples are collected every 10 seconds for percentile stats
- A single-line JSON object is printed to stdout (JSONL, appendable with `>>`)
- A summary line is printed to stderr: `CrateDB 6.2.1 | rec/s per CPU: avg=1842 p95=2017 max=2017`

The JSON includes: timestamp, client type (`python-http` / `rust-http`), cluster info, run config, and results with min/max/avg/p90/p95 for both `records_per_second` and `records_per_cpu_second`.

## Table Schema

Both implementations create identical tables:

```sql
CREATE TABLE IF NOT EXISTS your_table (
    id TEXT PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE,
    region TEXT,
    product_category TEXT,
    event_type TEXT,
    user_id INTEGER,
    user_segment TEXT,
    amount DOUBLE PRECISION,
    quantity INTEGER,
    metadata OBJECT(DYNAMIC)
    -- obj_0 TEXT, obj_1 TEXT, ... when using --objects
) WITH (number_of_replicas = 1)
```

## Performance Tuning

For maximum throughput against a remote cluster:

```bash
# Python
uv run crate-write --table-name perf --duration 5 --threads 64 --batch-size 1000 --batch-interval 0

# Rust
cargo run -- --table-name perf --duration 5 --threads 128 --batch-size 1000 --batch-interval 0
```

Key parameters:
- **--threads 32-128**: More concurrent tasks = more HTTP requests in-flight while waiting for responses
- **--batch-size 1000**: Sweet spot — larger batches increase serialization time without proportional network savings
- **--batch-interval 0**: No artificial delay between batches

Both implementations use gzip compression on bulk payloads (~88% size reduction), reducing bandwidth usage significantly.

## Record Verification

Both implementations verify records after completion by comparing `SELECT COUNT(*)` (minus pre-existing rows) against records sent.

## Load Balancer Testing

```bash
uv run crate-write --test-loadbalancer
cargo run -- --test-loadbalancer
```

Creates fresh TCP connections to test whether the load balancer distributes traffic across CrateDB nodes using 5-tuple hashing. In normal mode, Python runs this automatically before starting workers.

## Project Structure

```
.
├── crate_write/
│   ├── __init__.py
│   └── main.py              # Python async engine (aiohttp)
├── rust/
│   ├── src/
│   │   ├── main.rs           # CLI, worker loop, benchmark JSON
│   │   ├── client.rs          # HTTP client (reqwest, gzip, spawn_blocking)
│   │   ├── generator.rs       # Record generation
│   │   ├── monitor.rs         # Atomic counters + percentile stats
│   │   └── config.rs          # TOML/JSON config loading
│   ├── config.toml            # Default config (gitignored)
│   └── Cargo.toml
├── pyproject.toml
├── BENCHMARKS.md              # Performance comparison data
├── .env                       # Connection string (gitignored)
└── README.md
```

## How We Fixed Python Performance

The original Python implementation used `threading.Thread` workers with the `requests` library. This had several compounding bottlenecks:

1. **GIL serialization**: Only one thread could generate records at a time.
2. **Sequential worker loop**: generate → HTTP POST → `time.sleep(0.1)` → repeat. Nothing overlapped.
3. **Connection pool limits**: `requests.Session()` defaults to 10 connections per host.
4. **Lock contention**: `threading.Lock` acquired on every batch insert.

The fix was **asyncio + aiohttp**: a single event loop runs N async tasks with unlimited connections, no GIL contention, no locks, and gzip compression on all payloads.

Similarly, Rust was improved by moving CPU work (record generation + JSON serialization + gzip) to `spawn_blocking`, keeping tokio I/O threads free for HTTP multiplexing.

### Benchmark Results

Tested against a 3-node CrateDB Cloud cluster (cr2 instance, 4 vCPU / 12 total, 2 TiB EBS gp3 per node, AWS NLB, us-east-1).

| Client location | Python | Rust | Bottleneck |
|---|---|---|---|
| Laptop (~150ms, ~30 Mbps up) | 32,895 rec/sec | 33,565 rec/sec | Uplink bandwidth |
| Hetzner Frankfurt (~80ms) | 16,449 rec/sec | — | Cross-Atlantic latency |

Both clients saturate the available bandwidth. From a client in the same AWS region (low latency, high bandwidth), throughput should approach the cluster's theoretical capacity of ~54K rec/sec (9K/cpu × 6 vCPU).

See `BENCHMARKS.md` for full data including percentile breakdowns, per-CPU metrics, and latency/bandwidth stats.

## License

MIT
