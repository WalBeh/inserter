# CrateDB Record Generator — Rust Implementation

High-performance CrateDB record generator using tokio async runtime and reqwest HTTP client.

## Build & Run

```bash
cd rust

# Set up connection
echo 'CRATE_CONNECTION_STRING=https://admin:password@your-cluster:4200' > .env

# Run with config.toml defaults (32 threads, batch_size 1000, no delay)
cargo run

# Or specify everything on the CLI
cargo run -- --table-name stress_test --duration 5 --threads 64 --batch-size 2000 --batch-interval 0

# Release build for max performance
cargo build --release
./target/release/crate-write
```

## Configuration

### config.toml (auto-detected in current directory)

```toml
table_name = "performance_test"
duration = 10
batch_size = 1000
batch_interval = 0      # milliseconds, 0 = no delay
threads = 32
objects = 0
log_level = "info"
```

CLI arguments override config.toml values when explicitly provided. The connection string comes from `.env` or `--connection-string`.

### CLI Options

```
--table-name <NAME>           Table to create/insert into
--duration <MINUTES>          Minutes to run
--connection-string <URL>     CrateDB URL (overrides .env / CRATE_CONNECTION_STRING)
--batch-size <SIZE>           Records per bulk insert
--batch-interval <MS>         Milliseconds between batches (0 = none)
--threads <COUNT>             Concurrent async worker tasks
--objects <COUNT>             Extra low-cardinality TEXT columns
--test-loadbalancer           Run 5-tuple load balancer test and exit
--log-level <LEVEL>           error, warn, info, debug, trace
--config <FILE>               Config file path (.toml or .json)
```

## Architecture

### Worker Loop

Each worker task runs in a tight loop:

```
generate_batch(batch_size)  →  execute_bulk(sql, params)  →  [sleep if interval > 0]  →  repeat
```

- **Record generation** is synchronous (no async overhead for CPU work)
- **HTTP insert** is async via tokio + reqwest with connection pooling
- **Monitor** uses `AtomicU64` counters — no lock contention between workers

### Connection Pool

The reqwest HTTP client pool is sized to `max(threads + 2, 10)` connections, with 60s keepalive and 60s request timeout. For high thread counts, each worker gets its own pooled connection.

### Record Verification

After all workers stop, the engine:
1. Runs `REFRESH TABLE` to flush pending writes
2. Runs `SELECT COUNT(*)` to verify actual row count
3. Compares against records sent and reports match/mismatch

## Performance

With a remote CrateDB Cloud cluster (~150ms latency):

| Threads | Batch Size | Interval | Throughput |
|---------|-----------|----------|------------|
| 1 | 100 | 100ms | ~260 rec/sec |
| 8 | 500 | 50ms | ~2,000 rec/sec |
| 32 | 1000 | 0 | ~15,000 rec/sec |
| 64 | 2000 | 0 | ~18,000+ rec/sec |

The bottleneck at high concurrency is CrateDB's ingestion speed, not the client.

## Tests

```bash
cargo test
```

19 tests covering client creation, URL parsing, auth headers, config loading/validation, record generation, batch generation, parameter serialization, and monitor operations.

## Dependencies

| Crate | Purpose |
|-------|---------|
| tokio | Async runtime |
| reqwest | HTTP client with connection pooling |
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
