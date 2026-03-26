use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info};

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
}

/// Lock-free counters for the hot path
struct Counters {
    total_records: AtomicU64,
    total_batches: AtomicU64,
    total_errors: AtomicU64,
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
            }),
            timing: Arc::new(RwLock::new(TimingState {
                start_time: now,
                last_report_time: now,
                last_report_records: 0,
                rate_samples: Vec::new(),
            })),
        }
    }

    /// Lock-free: no contention between workers
    pub fn add_records(&self, count: usize) {
        self.counters.total_records.fetch_add(count as u64, Ordering::Relaxed);
        self.counters.total_batches.fetch_add(1, Ordering::Relaxed);
        debug!("Added {} records", count);
    }

    /// Lock-free: no contention between workers
    pub fn add_error(&self) {
        self.counters.total_errors.fetch_add(1, Ordering::Relaxed);
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
        timing.rate_samples.push(current_rate);

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

        let mut timing = self.timing.write().await;
        let now = Instant::now();
        timing.start_time = now;
        timing.last_report_time = now;
        timing.last_report_records = 0;
        timing.rate_samples.clear();

        info!("Performance monitor reset");
    }

    pub async fn get_percentile_stats(&self) -> PercentileStats {
        let timing = self.timing.read().await;
        let s = &timing.rate_samples;
        if s.is_empty() {
            return PercentileStats { avg: 0.0, min: 0.0, max: 0.0, p90: 0.0, p95: 0.0 };
        }
        let mut sorted: Vec<f64> = s.clone();
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
