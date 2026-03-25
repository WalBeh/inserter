# CrateDB Record Generator

High-performance record generators for CrateDB. Available in Python (async) and Rust, both optimized for maximum insert throughput.

## Implementations

| | Python (async) | Rust |
|---|---|---|
| **Engine** | asyncio + aiohttp | tokio + reqwest |
| **Concurrency** | Async tasks (no GIL) | Async tasks + thread pool |
| **Throughput** | ~15,000-17,000 rec/sec | ~15,000-20,000 rec/sec |
| **Memory** | ~30-100MB | ~5-50MB |
| **Setup** | `uv run crate-write` | `cargo run` |

Both implementations share the same CLI interface, table schema, and record format.

## Quick Start

### Python

```bash
# Requires uv (https://astral.sh/uv)
uv venv && source .venv/bin/activate
uv pip install -e .

# Configure connection
echo 'CRATE_CONNECTION_STRING=https://admin:password@your-cluster:4200' > .env

# Run (1 minute, 32 async tasks, batch size 1000, no delay)
uv run crate-write --table-name test_events --duration 1 --threads 32 --batch-size 1000 --batch-interval 0
```

### Rust

```bash
cd rust

# Configure connection
echo 'CRATE_CONNECTION_STRING=https://admin:password@your-cluster:4200' > .env

# Run (reads config.toml: 32 threads, batch_size 1000, batch_interval 0)
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

The Rust implementation auto-detects `rust/config.toml`:

```toml
table_name = "performance_test"
duration = 10
batch_size = 1000
batch_interval = 0
threads = 32
objects = 0
log_level = "info"
```

CLI arguments override config file values when explicitly provided.

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
```

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
cargo run -- --table-name perf --duration 5 --threads 64 --batch-size 1000 --batch-interval 0
```

Key parameters:
- **--threads 32-128**: More concurrent tasks = more HTTP requests in-flight while waiting for responses
- **--batch-size 1000-5000**: Larger batches = fewer HTTP roundtrips
- **--batch-interval 0**: No artificial delay between batches

The bottleneck is typically CrateDB ingestion speed and network latency, not the client.

## Record Verification

Both implementations verify records after completion:

1. `REFRESH TABLE` to flush pending writes
2. `SELECT COUNT(*)` to get actual row count
3. Compare against records sent, report match or mismatch

## Load Balancer Testing

Both implementations include a 5-tuple load balancer distribution test:

```bash
# Python
uv run crate-write --test-loadbalancer

# Rust
cargo run -- --test-loadbalancer
```

This creates fresh TCP connections to test whether the load balancer distributes traffic across CrateDB nodes using 5-tuple hashing.

## Project Structure

```
.
├── crate_write/
│   ├── __init__.py
│   └── main.py              # Python async engine (aiohttp)
├── rust/
│   ├── src/
│   │   ├── main.rs           # CLI, table creation, worker loop
│   │   ├── client.rs          # HTTP client (reqwest)
│   │   ├── generator.rs       # Record generation
│   │   ├── monitor.rs         # Atomic performance counters
│   │   └── config.rs          # TOML/JSON config loading
│   ├── config.toml            # Default config
│   └── Cargo.toml
├── pyproject.toml
├── .env                       # Connection string (gitignored)
└── README.md
```

## How We Fixed Python Performance

The original Python implementation used `threading.Thread` workers with the `requests` library. This had several compounding bottlenecks:

1. **GIL serialization**: Python's Global Interpreter Lock meant only one thread could generate records at a time, even with multiple OS threads. Record generation (~12ms per batch) blocked all other threads.
2. **Sequential worker loop**: Each thread did generate → HTTP POST → `time.sleep(0.1)` → repeat. Nothing overlapped. With default settings, each thread managed ~500 records/sec.
3. **Connection pool limits**: `requests.Session()` defaults to 10 connections per host (urllib3 default). Under load, extra requests queued for a connection or triggered new TLS handshakes (~100-200ms each).
4. **Lock contention**: A `threading.Lock` was acquired on every batch insert to update the performance monitor counters.

The fix was to replace the entire insertion engine with **asyncio + aiohttp**:

- A single event loop runs N async tasks (e.g., 64). While one task awaits the HTTP response from CrateDB, the others are free to generate and send batches. This sidesteps the GIL entirely — there's only one thread, so no contention.
- `aiohttp.TCPConnector(limit=0)` removes the connection pool cap. Dozens of HTTP requests fly concurrently.
- The performance monitor no longer needs a lock (single-threaded async = no races).
- `time.sleep()` was replaced with `asyncio.sleep()` (or removed entirely with `--batch-interval 0`).

**Result**: Python went from ~260 records/sec (1 thread, default settings) to **~17,000 records/sec** (64 async tasks, batch_size 1000, no delay) — within striking distance of the Rust implementation.

### Python vs Rust (same cluster, same settings)

Tested against a single-node CrateDB Cloud cluster with ~150ms network latency:

| | Python (async) | Rust (tokio) |
|---|---|---|
| 32 tasks, batch 1000 | ~15,000 rec/sec | ~15,000 rec/sec |
| 64 tasks, batch 1000 | ~17,000 rec/sec | ~18,000 rec/sec |
| Memory usage | ~50MB | ~10MB |
| Errors | 0 | 0 |

At this point the bottleneck is CrateDB ingestion and network latency, not the client. Rust has a small edge due to lower per-request overhead and true parallelism in record generation, but for this I/O-bound workload the difference is marginal.

## License

MIT
