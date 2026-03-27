# Auto-Tune: Binary Search Ingestion Controller

The `--auto-tune` flag (Elixir only) automatically finds the maximum ingestion rate a CrateDB cluster can sustain without overloading it.

## The Problem

CrateDB's ingestion performance has a **cliff** — it handles 40 concurrent senders fine, 50 fine, then suddenly at 55 the write thread pool overflows, latency spikes 10x, and writes get rejected. This makes manual tuning tedious: too few senders underutilizes the cluster, too many overloads it.

A traditional PID controller can't handle this because the relationship between senders and latency is non-linear — it's flat until the cliff, then vertical.

## The Algorithm

The controller uses a **three-phase binary search** to find the cliff edge:

```
PROBE → CLIFF → BISECT → CONVERGE → HOLD
```

### Phase 1: PROBE

Start with a small number of senders and ramp up aggressively until we overshoot.

```
AUTO-TUNE: PROBE senders=12→24 batch=750→975 (p95 ok)
AUTO-TUNE: PROBE senders=24→36 batch=975→1268 (p95 ok)
AUTO-TUNE: PROBE senders=36→54 batch=1268→1648 (p95 ok)
```

- Starts at 12 senders, batch size 750
- Doubles senders until 24, then increases by 50%
- Batch size grows by 30% each step
- P95 latency is checked each cycle (every 5 seconds)
- If P95 stays under the target → mark current count as `good`, keep ramping

### CLIFF Detection

When P95 exceeds the latency target, we've found the cliff.

```
AUTO-TUNE: CLIFF at 54 senders (p95=2279ms) — bisecting [36, 54]
```

- The last good count becomes the **lower bound** (36)
- The current (overloaded) count becomes the **upper bound** (54)
- Batch size is locked — only senders are adjusted from here

### Phase 2: BISECT

Binary search between the lower and upper bounds to find the exact sweet spot.

```
AUTO-TUNE: BISECT next=45 (bounds=[36, 54])
AUTO-TUNE: BISECT 45 OK (p95=900ms) bounds=[45, 54]
AUTO-TUNE: BISECT next=49 (bounds=[45, 54])
AUTO-TUNE: BISECT 49 HIGH (p95=2800ms) bounds=[45, 49]
AUTO-TUNE: BISECT next=47 (bounds=[45, 49])
AUTO-TUNE: BISECT 47 OK (p95=950ms) bounds=[47, 49]
AUTO-TUNE: CONVERGED → 47 senders (batch=1648)
```

- Tests the midpoint of [good, bad]
- If P95 < target × 1.2 → midpoint is OK, becomes new lower bound
- If P95 > target × 1.2 → midpoint is too high, becomes new upper bound
- The 20% tolerance prevents flip-flopping at the exact boundary
- Converges when the range is ≤ 2 senders (typically 3-5 steps)

### Phase 3: HOLD

Lock at the converged sender count for the rest of the run.

```
AUTO-TUNE: CONVERGED → 47 senders (batch=1648)
```

- No further adjustments — the bisection found the ceiling
- Emergency brake still active: if rejected writes appear, senders are reduced by 25%

## Latency Measurement

The controller uses **windowed P95 latency** — only samples from the last 5-second window, not the entire run. This prevents a spike during the overloaded PROBE phase from polluting the readings during BISECT.

## Emergency Brake

If CrateDB starts rejecting writes (detected via `sys.nodes` thread pool query), the controller immediately:
- Reduces senders by 25%
- Reduces batch size by 25%
- Enters bisection between the reduced count and the previous count

## Usage

```bash
# Auto-tune with default 2s latency target
mix run -e "CrateWrite.main()" -- --auto-tune --no-compression \
  --table-name bench --duration 5 --threads 128 --batch-size 3000 \
  --batch-interval 0 --shards 4 --replicas 0

# Tighter latency target (finds a lower but more stable sender count)
mix run -e "CrateWrite.main()" -- --auto-tune --latency-target 1.0 \
  --table-name bench --duration 5 --threads 64 --batch-size 2000 \
  --batch-interval 0 --shards 4 --replicas 0
```

The `--threads` and `--batch-size` flags set the **maximum ceiling** the controller can ramp up to. It starts at 12 senders / batch 750 and works up from there.

## Benchmark JSON Output

When combined with `--benchmark`, the JSON includes the controller's state:

```json
{
  "auto_tune": {
    "enabled": true,
    "algorithm": "bisect",
    "latency_target_ms": 2000,
    "initial_senders": 12,
    "initial_batch_size": 750,
    "final_senders": 47,
    "final_batch_size": 1648,
    "final_phase": "hold",
    "peak_senders": 54,
    "adjustments": 12,
    "emergency_brakes": 0,
    "sender_history": [12, 24, 36, 54, 45, 49, 47],
    "batch_history": [750, 975, 1268, 1648]
  }
}
```

## Why Not PID?

We tried PID first. It failed because:

1. **CrateDB has a cliff, not a slope** — PID assumes a linear control surface
2. **Death spiral** — PID kept reducing senders without finding equilibrium
3. **Cumulative latency stats** — historical spikes polluted the controller's readings
4. **Tuning gains** (Kp/Ki/Kd) is fragile and cluster-dependent

Binary search has zero tunable parameters and converges deterministically.

---

*Built with the guidance of a great engineering mentor who suggested PID controllers — which led us to discover why they don't work here, and ultimately to a better solution. Greetings from the BEAM! 👋*
