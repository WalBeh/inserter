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

### From k8s pod (same cluster, ~1ms RTT, no compression)

| Client | Tasks | Batch | Shards | Replicas | Avg rec/sec | P90 rec/sec | Per CPU avg | Per CPU P95 | Latency avg | Bandwidth | Rejected | Errors |
|--------|-------|-------|--------|----------|-------------|-------------|-------------|-------------|-------------|-----------|----------|--------|
| **Elixir** | **64** | **1000** | **4** | **0** | **84,302** | **102,259** | **4,215** | **5,166** | **296ms** | **187 Mbps** | **0** | **0** |
| Elixir | 64 | 1000 | 20 | 0 | 78,405 | 105,589 | 3,920 | 5,280 | 582ms | 180 Mbps | 1,174 | 1 |
| Rust | 64 | 2000 | 20 | 0 | 75,668 | 105,549 | 3,783 | 5,330 | 1,510ms | 58 Mbps | 0 | 0 |
| Rust | 128 | 2000 | 20 | 0 | 67,586 | 87,257 | 3,379 | 4,457 | 3,282ms | 51 Mbps | 0 | 0 |

Elixir beats Rust on avg throughput (+11%) and latency (296ms vs 1,510ms) from the same pod. BEAM's preemptive scheduler keeps all workers responsive without `spawn_blocking` overhead.

Note: Rust ran with 20 shards (suboptimal for this cluster). A Rust run with 4 shards from the same pod is needed for a true apples-to-apples comparison.

### From Hetzner Frankfurt (~180ms RTT, gzip on)

| Client | Tasks | Batch | Shards | Replicas | Avg rec/sec | Per CPU | Bandwidth | Errors |
|--------|-------|-------|--------|----------|-------------|---------|-----------|--------|
| Rust | 64 | 2000 | 4 | 1 | 52,426 | 2,621 | 39.3 Mbps | 0 |
| Elixir | 64 | 1000 | 4 | 0 | 22,154 | 1,108 | 46.7 Mbps | 0 |

Rust wins over the network. Elixir's higher per-request latency (Mint is pure Elixir vs reqwest's C-based hyper) matters more when RTT is already high.

### From laptop (5G, ~150ms RTT, gzip on)

| Client | Tasks | Batch | Shards | Replicas | Avg rec/sec | Per CPU | Bandwidth | Errors |
|--------|-------|-------|--------|----------|-------------|---------|-----------|--------|
| Rust | 42 | 1000 | 4 | 1 | 56,258 | 2,813 | 41.3 Mbps | 0 |
| Elixir | 64 | 1000 | 4 | 0 | 25,039 | 1,252 | 46.7 Mbps | 0 |

## Clean benchmarks: 4-CPU single node (k8s pod, no compression)

| Threads | Batch | Shards | Replicas | Avg rec/sec | Per CPU avg | Per CPU P95 | Rejected | Latency avg |
|---------|-------|--------|----------|-------------|-------------|-------------|----------|-------------|
| 16 | 2000 | 4 | 0 | 24,903 | 6,226 | 8,344 | 0 | 1,204ms |
| 24 | 2000 | 4 | 0 | 24,947 | 6,237 | 7,749 | 0 | 1,740ms |
| 32 | 2000 | 4 | 0 | 38,853 | 9,713 | 11,650 | 186K (1.5%) | 1,574ms |

All Rust. 16 threads is the clean ceiling for a 4-CPU node: **~25K rec/sec, ~6.2K/cpu, 0 rejections**.

## Key Observations

1. **Elixir beats Rust on same-network** (84K vs 76K, +11%) thanks to BEAM's preemptive scheduling — lower per-request latency, better worker utilization.
2. **Rust beats Elixir over high-latency networks** — reqwest/hyper's C-based HTTP stack has lower per-request overhead than Mint (pure Elixir).
3. **Python is single-core GIL-bound** on localhost (76K with orjson). Over the network it matches Rust since bandwidth is the bottleneck.
4. **On localhost, Rust is 3x faster than Python** (214K vs 76K) because `spawn_blocking` parallelizes CPU work.
5. **Shards**: 4 shards was optimal for the 5-node cluster at this concurrency. Over-sharding (20) caused rejections.
6. **Batch size 15,000 overloads a single node** — 1.8M rejected writes. Batch 1000-2000 is safer.
7. **Gzip compression**: essential over the network (~88% reduction), counterproductive on localhost.
8. **Rejected writes** are the key health indicator — any > 0 means the cluster is overloaded.
9. **Network bandwidth is almost always the bottleneck** from remote clients.
