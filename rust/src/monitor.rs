use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::client::CrateClient;

#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub total_records: u64,
    pub total_batches: u64,
    pub total_errors: u64,
    pub current_rate: f64,
    pub average_rate: f64,
    pub runtime_seconds: f64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PercentileStats {
    pub avg: f64,
    pub min: f64,
    pub max: f64,
    pub p90: f64,
    pub p95: f64,
}

/// Time-tracking state that needs a lock (read infrequently)
struct TimingState {
    start_time: Instant,
    last_report_time: Instant,
    last_report_records: u64,
    rate_samples: Vec<f64>,
    latency_samples: Vec<f64>,
}

/// Lock-free counters for the hot path
struct Counters {
    total_records: AtomicU64,
    total_batches: AtomicU64,
    total_errors: AtomicU64,
    total_bytes_sent: AtomicU64,
}

#[derive(Clone)]
pub struct PerformanceMonitor {
    counters: Arc<Counters>,
    timing: Arc<RwLock<TimingState>>,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        let now = Instant::now();

        Self {
            counters: Arc::new(Counters {
                total_records: AtomicU64::new(0),
                total_batches: AtomicU64::new(0),
                total_errors: AtomicU64::new(0),
                total_bytes_sent: AtomicU64::new(0),
            }),
            timing: Arc::new(RwLock::new(TimingState {
                start_time: now,
                last_report_time: now,
                last_report_records: 0,
                rate_samples: Vec::new(),
                latency_samples: Vec::new(),
            })),
        }
    }

    /// Lock-free: no contention between workers
    pub fn add_records(&self, count: usize) {
        self.counters
            .total_records
            .fetch_add(count as u64, Ordering::Relaxed);
        self.counters.total_batches.fetch_add(1, Ordering::Relaxed);
        debug!("Added {} records", count);
    }

    /// Lock-free: no contention between workers
    pub fn add_error(&self) {
        self.counters.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Track bytes sent and request latency (latency needs lock, but batched)
    pub async fn add_request_stats(&self, bytes_sent: usize, latency_ms: f64) {
        self.counters
            .total_bytes_sent
            .fetch_add(bytes_sent as u64, Ordering::Relaxed);
        let mut timing = self.timing.write().await;
        timing.latency_samples.push(latency_ms);
    }

    /// Called only by the reporting task every 10 seconds
    pub async fn get_current_stats(&self) -> PerformanceStats {
        let total_records = self.counters.total_records.load(Ordering::Relaxed);
        let total_batches = self.counters.total_batches.load(Ordering::Relaxed);
        let total_errors = self.counters.total_errors.load(Ordering::Relaxed);

        let mut timing = self.timing.write().await;
        let now = Instant::now();
        let total_runtime = now.duration_since(timing.start_time);
        let time_since_last_report = now.duration_since(timing.last_report_time);

        let records_since_last = total_records.saturating_sub(timing.last_report_records);
        let current_rate = if time_since_last_report.as_secs_f64() > 0.0 {
            records_since_last as f64 / time_since_last_report.as_secs_f64()
        } else {
            0.0
        };

        let average_rate = if total_runtime.as_secs_f64() > 0.0 {
            total_records as f64 / total_runtime.as_secs_f64()
        } else {
            0.0
        };

        timing.last_report_time = now;
        timing.last_report_records = total_records;
        // Skip zero-rate samples (e.g. first tick before any batches complete)
        if records_since_last > 0 {
            timing.rate_samples.push(current_rate);
        }

        PerformanceStats {
            total_records,
            total_batches,
            total_errors,
            current_rate,
            average_rate,
            runtime_seconds: total_runtime.as_secs_f64(),
        }
    }

    pub async fn get_final_stats_async(&self) -> PerformanceStats {
        let total_records = self.counters.total_records.load(Ordering::Relaxed);
        let total_batches = self.counters.total_batches.load(Ordering::Relaxed);
        let total_errors = self.counters.total_errors.load(Ordering::Relaxed);

        let timing = self.timing.read().await;
        let now = Instant::now();
        let total_runtime = now.duration_since(timing.start_time);

        let average_rate = if total_runtime.as_secs_f64() > 0.0 {
            total_records as f64 / total_runtime.as_secs_f64()
        } else {
            0.0
        };

        PerformanceStats {
            total_records,
            total_batches,
            total_errors,
            current_rate: 0.0,
            average_rate,
            runtime_seconds: total_runtime.as_secs_f64(),
        }
    }

    pub async fn reset(&self) {
        self.counters.total_records.store(0, Ordering::Relaxed);
        self.counters.total_batches.store(0, Ordering::Relaxed);
        self.counters.total_errors.store(0, Ordering::Relaxed);
        self.counters.total_bytes_sent.store(0, Ordering::Relaxed);

        let mut timing = self.timing.write().await;
        let now = Instant::now();
        timing.start_time = now;
        timing.last_report_time = now;
        timing.last_report_records = 0;
        timing.rate_samples.clear();
        timing.latency_samples.clear();

        info!("Performance monitor reset");
    }

    pub async fn get_percentile_stats(&self) -> PercentileStats {
        let timing = self.timing.read().await;
        compute_percentiles(&timing.rate_samples)
    }

    pub async fn get_latency_stats(&self) -> PercentileStats {
        let timing = self.timing.read().await;
        compute_percentiles(&timing.latency_samples)
    }

    pub async fn get_bandwidth_mbps(&self) -> f64 {
        let bytes = self.counters.total_bytes_sent.load(Ordering::Relaxed);
        let timing = self.timing.read().await;
        let elapsed = Instant::now()
            .duration_since(timing.start_time)
            .as_secs_f64();
        if elapsed > 0.0 {
            (bytes as f64 * 8.0 / 1_000_000.0) / elapsed
        } else {
            0.0
        }
    }

    pub fn get_total_bytes_sent(&self) -> u64 {
        self.counters.total_bytes_sent.load(Ordering::Relaxed)
    }

    pub fn get_total_records(&self) -> u64 {
        self.counters.total_records.load(Ordering::Relaxed)
    }

    pub async fn get_error_rate(&self) -> f64 {
        let total_batches = self.counters.total_batches.load(Ordering::Relaxed);
        let total_errors = self.counters.total_errors.load(Ordering::Relaxed);
        if total_batches > 0 {
            (total_errors as f64 / total_batches as f64) * 100.0
        } else {
            0.0
        }
    }

    pub fn get_average_batch_size(&self) -> f64 {
        let total_records = self.counters.total_records.load(Ordering::Relaxed);
        let total_batches = self.counters.total_batches.load(Ordering::Relaxed);
        if total_batches > 0 {
            total_records as f64 / total_batches as f64
        } else {
            0.0
        }
    }
}

impl Default for PerformanceMonitor {
    fn default() -> Self {
        Self::new()
    }
}

fn compute_percentiles(samples: &[f64]) -> PercentileStats {
    if samples.is_empty() {
        return PercentileStats {
            avg: 0.0,
            min: 0.0,
            max: 0.0,
            p90: 0.0,
            p95: 0.0,
        };
    }
    let mut sorted: Vec<f64> = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted.len();
    let avg = sorted.iter().sum::<f64>() / n as f64;
    PercentileStats {
        avg: (avg * 10.0).round() / 10.0,
        min: (sorted[0] * 10.0).round() / 10.0,
        max: (sorted[n - 1] * 10.0).round() / 10.0,
        p90: (sorted[(n as f64 * 0.9) as usize] * 10.0).round() / 10.0,
        p95: (sorted[(n as f64 * 0.95) as usize] * 10.0).round() / 10.0,
    }
}

// ── Thread Pool Monitor ─────────────────────────────────────────────────────

/// Find the "write" pool object from the thread_pools array column.
fn find_write_pool(value: Option<&serde_json::Value>) -> Option<&serde_json::Map<String, serde_json::Value>> {
    value?
        .as_array()?
        .iter()
        .filter_map(|v| v.as_object())
        .find(|obj| obj.get("name").and_then(|n| n.as_str()) == Some("write"))
}

/// Per-node snapshot from a single poll of sys.nodes
#[derive(Debug, Clone)]
struct NodeSample {
    active: u64,
    queue: u64,
}

#[derive(Debug, Clone)]
struct NodePoolCounters {
    name: String,
    pool_size: u64,
    completed: u64,
    rejected: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeThreadPoolStats {
    pub name: String,
    pub pool_size: u64,
    pub active: PercentileStats,
    pub queued: PercentileStats,
    pub completed_delta: u64,
    pub rejected_delta: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ClusterThreadPoolStats {
    pub total_pool_size: u64,
    pub total_completed: u64,
    pub total_rejected: u64,
    pub active_threads: PercentileStats,
    pub queued_tasks: PercentileStats,
    pub samples: usize,
    pub nodes: Vec<NodeThreadPoolStats>,
}

/// Configuration for queue-based throttling.
#[derive(Debug, Clone)]
pub struct QueueThrottleConfig {
    pub enabled: bool,
    pub capacity: u64,
    pub threshold_pct: f64,
}

/// Configuration for CPU-based throttling.
#[derive(Debug, Clone)]
pub struct CpuThrottleConfig {
    pub enabled: bool,
    /// Target max CPU percentage across the cluster (0.0 - 1.0)
    pub max_cpu_pct: f64,
}

/// Monitors CrateDB write thread pool by polling sys.nodes during the benchmark.
pub struct ThreadPoolMonitor {
    client: CrateClient,
    samples: Arc<RwLock<std::collections::BTreeMap<String, Vec<NodeSample>>>>,
    baseline: Arc<RwLock<Vec<NodePoolCounters>>>,
    /// Semaphore controlling max concurrent in-flight requests.
    /// Initialized with a large number; the poller holds reserve permits to limit concurrency.
    pub concurrency_sem: Arc<tokio::sync::Semaphore>,
    /// Current target concurrency (for reporting).
    pub current_concurrency: Arc<AtomicU64>,
    /// Total permits in the semaphore (workers never see more than max_concurrency).
    total_permits: usize,
}

// Large enough that we can always steal from the pool without blocking.
const SEM_TOTAL: usize = 1024;

impl ThreadPoolMonitor {
    pub fn new(client: CrateClient, max_concurrency: usize) -> Self {
        Self {
            client,
            samples: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            baseline: Arc::new(RwLock::new(Vec::new())),
            concurrency_sem: Arc::new(tokio::sync::Semaphore::new(SEM_TOTAL)),
            current_concurrency: Arc::new(AtomicU64::new(max_concurrency as u64)),
            total_permits: SEM_TOTAL,
        }
    }

    pub async fn capture_baseline(&self) {
        if let Ok(counters) = self.query_counters().await {
            *self.baseline.write().await = counters;
        }
    }

    pub fn spawn_poller(
        &self,
        mut shutdown_rx: broadcast::Receiver<()>,
        queue_throttle: QueueThrottleConfig,
        cpu_throttle: CpuThrottleConfig,
    ) -> tokio::task::JoinHandle<()> {
        let client = self.client.clone();
        let samples = self.samples.clone();
        let sem = self.concurrency_sem.clone();
        let current_concurrency = self.current_concurrency.clone();
        let max_concurrency = current_concurrency.load(Ordering::Relaxed) as usize;
        let min_concurrency = (max_concurrency as f64 * 0.25).ceil() as usize;
        let total_permits = self.total_permits;
        let either_throttle = queue_throttle.enabled || cpu_throttle.enabled;

        tokio::spawn(async move {
            let reserve = total_permits - max_concurrency;
            sem.acquire_many(reserve as u32).await.expect("semaphore closed").forget();
            let mut extra_stolen: usize = 0;

            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        if extra_stolen > 0 {
                            sem.add_permits(extra_stolen);
                        }
                        break;
                    },
                    _ = interval.tick() => {
                        if let Ok(rows) = client.execute_query(
                            "SELECT name, thread_pools, os['cpu']['used'] FROM sys.nodes ORDER BY name"
                        ).await {
                            let mut total_queue: u64 = 0;
                            let mut total_cpu: f64 = 0.0;
                            let mut node_count: u64 = 0;
                            let mut smap = samples.write().await;
                            for row in &rows {
                                let name = row.first().and_then(|v| v.as_str()).unwrap_or("unknown").to_string();
                                if let Some(pool) = find_write_pool(row.get(1)) {
                                    let active = pool.get("active").and_then(|v| v.as_u64()).unwrap_or(0);
                                    let queue = pool.get("queue").and_then(|v| v.as_u64()).unwrap_or(0);
                                    total_queue += queue;
                                    node_count += 1;
                                    smap.entry(name).or_default().push(NodeSample { active, queue });
                                }
                                if let Some(cpu) = row.get(2).and_then(|v| v.as_f64()) {
                                    total_cpu += cpu;
                                }
                            }
                            drop(smap);

                            if either_throttle && node_count > 0 {
                                let current = max_concurrency - extra_stolen;

                                // Queue-based desired concurrency
                                let queue_desired = if queue_throttle.enabled {
                                    let avg_queue = total_queue as f64 / node_count as f64;
                                    let queue_pct = avg_queue / queue_throttle.capacity as f64;

                                    if queue_pct > queue_throttle.threshold_pct {
                                        let pressure = (queue_pct - queue_throttle.threshold_pct)
                                            / (1.0 - queue_throttle.threshold_pct);
                                        let target = max_concurrency as f64 - pressure * (max_concurrency - min_concurrency) as f64;
                                        (target as usize).max(min_concurrency)
                                    } else if queue_pct < queue_throttle.threshold_pct * 0.5 {
                                        (current + 1).min(max_concurrency)
                                    } else {
                                        current
                                    }
                                } else {
                                    max_concurrency
                                };

                                // CPU-based desired concurrency
                                let cpu_desired = if cpu_throttle.enabled {
                                    let avg_cpu = total_cpu / node_count as f64 / 100.0; // 0.0-1.0

                                    if avg_cpu > cpu_throttle.max_cpu_pct {
                                        // Over target: reduce proportionally
                                        // e.g., at 50% CPU with 25% target → overshoot=1.0 → min concurrency
                                        let overshoot = (avg_cpu - cpu_throttle.max_cpu_pct)
                                            / (1.0 - cpu_throttle.max_cpu_pct);
                                        let target = max_concurrency as f64 - overshoot * (max_concurrency - min_concurrency) as f64;
                                        (target as usize).max(min_concurrency)
                                    } else if avg_cpu < cpu_throttle.max_cpu_pct * 0.5 {
                                        // Well below target: restore 1 per tick
                                        (current + 1).min(max_concurrency)
                                    } else {
                                        current
                                    }
                                } else {
                                    max_concurrency
                                };

                                // Take the more restrictive of the two
                                let desired = queue_desired.min(cpu_desired);

                                if desired < current {
                                    let to_steal = current - desired;
                                    for _ in 0..to_steal {
                                        match tokio::time::timeout(
                                            std::time::Duration::from_millis(100),
                                            sem.acquire(),
                                        ).await {
                                            Ok(Ok(permit)) => {
                                                permit.forget();
                                                extra_stolen += 1;
                                            }
                                            _ => break,
                                        }
                                    }
                                } else if desired > current && extra_stolen > 0 {
                                    let to_release = (desired - current).min(extra_stolen);
                                    sem.add_permits(to_release);
                                    extra_stolen -= to_release;
                                }

                                current_concurrency.store(
                                    (max_concurrency - extra_stolen) as u64,
                                    Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }
        })
    }

    async fn query_counters(&self) -> anyhow::Result<Vec<NodePoolCounters>> {
        let rows = self.client.execute_query(
            "SELECT name, thread_pools FROM sys.nodes ORDER BY name"
        ).await?;

        Ok(rows.iter().filter_map(|row| {
            let name = row.first().and_then(|v| v.as_str())?.to_string();
            let pool = find_write_pool(row.get(1))?;
            Some(NodePoolCounters {
                name,
                pool_size: pool.get("threads").and_then(|v| v.as_u64()).unwrap_or(0),
                completed: pool.get("completed").and_then(|v| v.as_u64()).unwrap_or(0),
                rejected: pool.get("rejected").and_then(|v| v.as_u64()).unwrap_or(0),
            })
        }).collect())
    }

    /// Call after the poller has stopped.
    pub async fn finalize(&self) -> Option<ClusterThreadPoolStats> {
        // Read and release baseline before the network call
        let baseline = self.baseline.read().await.clone();
        let final_counters = self.query_counters().await.ok()?;
        let samples = self.samples.read().await;

        if samples.is_empty() {
            return None;
        }

        let mut nodes = Vec::new();
        let mut total_pool_size: u64 = 0;
        let mut total_completed: u64 = 0;
        let mut total_rejected: u64 = 0;
        let mut num_samples: usize = 0;

        for fc in &final_counters {
            let base = baseline.iter().find(|b| b.name == fc.name);
            let completed_delta = fc.completed.saturating_sub(base.map(|b| b.completed).unwrap_or(0));
            let rejected_delta = fc.rejected.saturating_sub(base.map(|b| b.rejected).unwrap_or(0));

            total_pool_size += fc.pool_size;
            total_completed += completed_delta;
            total_rejected += rejected_delta;

            let node_samples = samples.get(&fc.name);
            let active_vals: Vec<f64> = node_samples
                .map(|s| s.iter().map(|ns| ns.active as f64).collect())
                .unwrap_or_default();
            let queue_vals: Vec<f64> = node_samples
                .map(|s| s.iter().map(|ns| ns.queue as f64).collect())
                .unwrap_or_default();

            if let Some(s) = node_samples {
                num_samples = num_samples.max(s.len());
            }

            nodes.push(NodeThreadPoolStats {
                name: fc.name.clone(),
                pool_size: fc.pool_size,
                active: compute_percentiles(&active_vals),
                queued: compute_percentiles(&queue_vals),
                completed_delta,
                rejected_delta,
            });
        }

        // Compute cluster-aggregate from per-node samples
        let cluster_active: Vec<f64> = (0..num_samples)
            .map(|i| {
                samples.values().filter_map(|v| v.get(i)).map(|s| s.active as f64).sum()
            })
            .collect();
        let cluster_queue: Vec<f64> = (0..num_samples)
            .map(|i| {
                samples.values().filter_map(|v| v.get(i)).map(|s| s.queue as f64).sum()
            })
            .collect();

        Some(ClusterThreadPoolStats {
            total_pool_size,
            total_completed,
            total_rejected,
            active_threads: compute_percentiles(&cluster_active),
            queued_tasks: compute_percentiles(&cluster_queue),
            samples: num_samples,
            nodes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_monitor_basic_operations() {
        let monitor = PerformanceMonitor::new();

        monitor.add_records(100);
        monitor.add_records(50);

        let stats = monitor.get_current_stats().await;
        assert_eq!(stats.total_records, 150);
        assert_eq!(stats.total_batches, 2);
        assert_eq!(stats.total_errors, 0);
    }

    #[tokio::test]
    async fn test_monitor_error_tracking() {
        let monitor = PerformanceMonitor::new();

        monitor.add_records(100);
        monitor.add_error();
        monitor.add_records(50);

        let stats = monitor.get_current_stats().await;
        assert_eq!(stats.total_records, 150);
        assert_eq!(stats.total_batches, 2);
        assert_eq!(stats.total_errors, 1);

        let error_rate = monitor.get_error_rate().await;
        assert_eq!(error_rate, 50.0); // 1 error out of 2 batches
    }

    #[tokio::test]
    async fn test_monitor_rate_calculation() {
        let monitor = PerformanceMonitor::new();

        monitor.add_records(100);

        // Wait a bit to ensure time passes
        sleep(Duration::from_millis(10)).await;

        let stats = monitor.get_current_stats().await;
        assert!(stats.average_rate > 0.0);
        assert!(stats.runtime_seconds > 0.0);
    }

    #[tokio::test]
    async fn test_monitor_reset() {
        let monitor = PerformanceMonitor::new();

        monitor.add_records(100);
        monitor.add_error();

        let stats_before = monitor.get_current_stats().await;
        assert_eq!(stats_before.total_records, 100);

        monitor.reset().await;

        let stats_after = monitor.get_current_stats().await;
        assert_eq!(stats_after.total_records, 0);
        assert_eq!(stats_after.total_errors, 0);
    }

    #[tokio::test]
    async fn test_average_batch_size() {
        let monitor = PerformanceMonitor::new();

        monitor.add_records(100);
        monitor.add_records(200);
        monitor.add_records(50);

        let avg_batch_size = monitor.get_average_batch_size();
        assert_eq!(avg_batch_size, 350.0 / 3.0);
    }
}
