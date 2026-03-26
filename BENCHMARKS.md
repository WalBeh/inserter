# Benchmark Results

All tests use `--benchmark` mode with gzip compression unless noted. CrateDB Cloud on AWS (us-east-1).

## Localhost: 32-vCPU single node (CrateDB 6.2.2, 125GB RAM)

Client and CrateDB on the same machine. No network overhead. `--no-compression` used (gzip wastes CPU on localhost).

### Rust (tokio + reqwest, spawn_blocking, no compression)

| Threads | Batch | Shards | Replicas | Avg rec/sec | Per CPU | Rejected | Errors |
|---------|-------|--------|----------|-------------|---------|----------|--------|
| 128 | 2000 | 4 | 1 | **214,209** | 6,694 | 0 | 15 |
| **128** | **15000** | **32** | **1** | **225,150** | **7,036** | **1,806,910** | **82** |
| 128 | 2000 | 32 | 0 | ~214,000 | ~6,700 | 0 | 0 |

Best clean run: **214K rec/sec** (128 threads, batch 2000, 4 shards, replicas=1). Batch 15000 pushes higher but causes 1.8M rejected writes — cluster overloaded.

### Python (asyncio + aiohttp + orjson, no compression)

| Tasks | Batch | Shards | Replicas | Avg rec/sec | Per CPU | Errors |
|-------|-------|--------|----------|-------------|---------|--------|
| 128 | 1000 | 32 | 0 | **76,350** | 2,386 | 0 |
| 128 | 1000 | 4 | 1 | 63,719 | 1,991 | 0 |
| 128 | 100 | 4 | 1 | 47,421 | 1,482 | 10 |

Python is single-core CPU-bound (GIL): one core at 100%, the other 31 idle. `json.dumps` → `orjson.dumps` gave +17%. To match Rust, run multiple Python processes in parallel.

## Remote: 3-node cluster (xdemo2, cr2, 4 vCPU/node, 12 total)

### From Hetzner Frankfurt (~180ms RTT, gzip on)

| Client | Tasks | Batch | Shards | Replicas | Avg rec/sec | Per CPU | Bandwidth | Errors |
|--------|-------|-------|--------|----------|-------------|---------|-----------|--------|
| Python | 64 | 1200 | **12** | 1 | **49,837** | 4,153 | 30 Mbps | 0 |
| Python | 64 | 1200 | 4 | 1 | 32,006 | 2,667 | 19.5 Mbps | 0 |
| Python | 64 | 1200 | 12 | 0 | 32,566 | 2,714 | 19.5 Mbps | 0 |

12 shards gave +56% over 4 shards. Bandwidth fluctuates on shared Hetzner networking (19-30 Mbps).

### From Hetzner Ashburn (~40ms RTT, gzip on)

| Client | Tasks | Batch | Shards | Replicas | Avg rec/sec | Per CPU | Bandwidth | Errors |
|--------|-------|-------|--------|----------|-------------|---------|-----------|--------|
| Python | 64 | 1200 | 12 | 1 | 25,596 | 2,133 | 13.9 Mbps | 0 |
| Python | 64 | 1000 | 12 | 1 | 20,342 | 1,695 | 12.2 Mbps | 0 |

Lower RTT but capped at ~14 Mbps — Hetzner Ashburn → AWS cross-cloud peering bottleneck.

### From MacBook (~150ms RTT, ~30 Mbps uplink, gzip on)

| Client | Tasks | Batch | Shards | Avg rec/sec | Per CPU | Bandwidth | Errors |
|--------|-------|-------|--------|-------------|---------|-----------|--------|
| Python | 64 | 1000 | 4 | 32,895 | 2,741 | 28.6 Mbps | 5 |
| Rust | 128 | 1000 | 4 | 33,565 | 2,797 | 26.0 Mbps | 0 |
| Rust | 32 | 1000 | 4 | 24,498 | 2,041 | — | 0 |

Both saturate the laptop uplink at ~28-30 Mbps.

## Remote: 5-node cluster (xdemo2 scaled, cr2, 4 vCPU/node, 20 total)

### From Hetzner Frankfurt (~180ms RTT, gzip on)

| Client | Tasks | Batch | Shards | Replicas | Avg rec/sec | Per CPU | Bandwidth | Errors |
|--------|-------|-------|--------|----------|-------------|---------|-----------|--------|
| Rust | 64 | 2000 | 4 | 1 | 52,426 | 2,621 | 39.3 Mbps | 0 |

Again bandwidth-limited at ~39 Mbps.

## Key Observations

1. **On localhost, Rust is 3x faster than Python** (214K vs 76K) because Rust parallelizes CPU work across cores via `spawn_blocking`, while Python is single-core GIL-bound.
2. **Over the network, both perform equally** — the bottleneck is bandwidth and latency, not client CPU.
3. **Shards matter**: 12 shards on 3 nodes gave +56% over 4 shards (Frankfurt test).
4. **Batch size 15,000 overloads a single node** — 1.8M rejected writes. Batch 1000-2000 is safer.
5. **Gzip compression**: essential over the network (~88% payload reduction), counterproductive on localhost (wastes single-core CPU).
6. **`orjson` vs `json.dumps`**: +17% for Python on localhost. Marginal over the network.
7. **Rejected writes** (`sys.nodes` thread pool query) are the key health indicator — any > 0 means the cluster is overloaded.
8. **Network bandwidth is almost always the bottleneck** from remote clients. From the same cloud region, expect much higher throughput.
