# Benchmark Results

## Cluster: xdemo2 (3-node CrateDB Cloud, EC2, 2TiB gp3 EBS)

Tested 2026-03-25 from a MacBook over the internet (~150ms latency to us-east-1).

### Python (asyncio + aiohttp, gzip level 1)

| Tasks | Batch Size | Avg rec/sec | Total (60s) | Errors |
|-------|-----------|-------------|-------------|--------|
| 32 | 1000 | 22,869 | 1,373,000 | 3 |
| **64** | **1000** | **32,895** | **1,974,000** | **5** |
| 64 | 2000 | 29,628 | 1,778,000 | 2 |
| 128 | 1000 | ~31,000 | ~1,860,000 | — |

Sweet spot: **64 tasks, batch_size 1000** — 32,895 rec/sec.

Batch_size 2000 is slower than 1000 because gzip + JSON serialization of larger payloads blocks the single-threaded event loop longer between I/O yields. 128 tasks adds scheduling overhead without enough I/O benefit.

### Rust (tokio + reqwest, gzip flate2::fast)

#### Before: CPU work on tokio threads (v1)

| Threads | Batch Size | Avg rec/sec | Total (60s) | Errors |
|---------|-----------|-------------|-------------|--------|
| 32 | 1000 | 24,498 | 1,497,000 | 0 |
| 64 | 1000 | 18,424 | 1,124,000 | 0 |
| 64 | 2000 | 25,031 | 1,532,000 | 0 |

Rust v1 *slowed down* at 64 threads because record generation + JSON serialization + gzip ran synchronously inside async tasks, blocking the tokio worker thread pool.

#### After: CPU work on spawn_blocking (v2)

| Threads | Batch Size | Avg rec/sec | Total (60s) | Errors |
|---------|-----------|-------------|-------------|--------|
| 32 | 1000 | 31,105 | 1,903,000 | 0 |
| 64 | 1000 | 30,691 | 1,870,000 | 0 |
| **128** | **1000** | **33,565** | **2,042,000** | **0** |

Moving CPU work to `spawn_blocking` freed the tokio I/O threads. Rust now scales with concurrency: **+67% at 64 threads** (18K → 31K), and 128 threads hits **33,565 rec/sec**.

### Comparison (best config each)

| | Python | Rust | Delta |
|---|---|---|---|
| Best config | 64 tasks / 1000 batch | 128 threads / 1000 batch | |
| **Throughput** | **32,895 rec/sec** | **33,565 rec/sec** | Rust +2% |
| Errors | 5 | 0 | Rust cleaner |
| Records (60s) | 1,974,000 | 2,042,000 | |

### Previous cluster: xdemo (1-node CrateDB Cloud)

| | Python | Rust |
|---|---|---|
| 32 tasks, batch 1000 | ~15,000 rec/sec | ~15,000 rec/sec |
| 64 tasks, batch 1000 | ~17,000 rec/sec | ~18,000 rec/sec |

On a single node both were bottlenecked by CrateDB ingestion, so performance was similar.

## Key Observations

1. **Both implementations now perform equally** at ~33K rec/sec against the 3-node cluster.
2. **Rust's initial scaling problem was fixed** by moving CPU work (record generation, JSON serialization, gzip) to `spawn_blocking`, keeping tokio I/O threads free for HTTP multiplexing.
3. **Batch size 1000 is the sweet spot** for both. Larger batches increase per-request CPU time (serialization + compression) without proportional network savings.
4. **The 3-node cluster can absorb ~33K rec/sec** from a single client over the internet. The bottleneck is now CrateDB ingestion, not the client.
5. **Rust has zero errors** across all runs. Python had a few transient failures (3-5 per run) likely from connection churn under high concurrency.
