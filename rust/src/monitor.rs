use std::sync::Arc;
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
    pub last_batch_time: Option<Instant>,
}

#[derive(Debug)]
struct MonitorState {
    total_records: u64,
    total_batches: u64,
    total_errors: u64,
    start_time: Instant,
    last_report_time: Instant,
    last_report_records: u64,
    records_in_last_period: u64,
}

#[derive(Clone)]
pub struct PerformanceMonitor {
    state: Arc<RwLock<MonitorState>>,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        let now = Instant::now();
        let state = MonitorState {
            total_records: 0,
            total_batches: 0,
            total_errors: 0,
            start_time: now,
            last_report_time: now,
            last_report_records: 0,
            records_in_last_period: 0,
        };

        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn add_records(&self, count: usize) {
        let mut state = self.state.write().await;
        state.total_records += count as u64;
        state.total_batches += 1;
        state.records_in_last_period += count as u64;

        debug!("Added {} records, total: {}", count, state.total_records);
    }

    pub async fn add_error(&self) {
        let mut state = self.state.write().await;
        state.total_errors += 1;
        debug!("Error recorded, total errors: {}", state.total_errors);
    }

    pub async fn get_current_stats(&self) -> PerformanceStats {
        let mut state = self.state.write().await;
        let now = Instant::now();
        let total_runtime = now.duration_since(state.start_time);
        let time_since_last_report = now.duration_since(state.last_report_time);

        // Calculate current rate (records per second in the last period)
        let current_rate = if time_since_last_report.as_secs_f64() > 0.0 {
            state.records_in_last_period as f64 / time_since_last_report.as_secs_f64()
        } else {
            0.0
        };

        // Calculate average rate
        let average_rate = if total_runtime.as_secs_f64() > 0.0 {
            state.total_records as f64 / total_runtime.as_secs_f64()
        } else {
            0.0
        };

        // Reset period counters
        state.last_report_time = now;
        state.records_in_last_period = 0;

        PerformanceStats {
            total_records: state.total_records,
            total_batches: state.total_batches,
            total_errors: state.total_errors,
            current_rate,
            average_rate,
            runtime_seconds: total_runtime.as_secs_f64(),
            last_batch_time: Some(now),
        }
    }

    pub async fn get_final_stats(&self) -> PerformanceStats {
        let state = self.state.read().await;
        let now = Instant::now();
        let total_runtime = now.duration_since(state.start_time);

        let average_rate = if total_runtime.as_secs_f64() > 0.0 {
            state.total_records as f64 / total_runtime.as_secs_f64()
        } else {
            0.0
        };

        PerformanceStats {
            total_records: state.total_records,
            total_batches: state.total_batches,
            total_errors: state.total_errors,
            current_rate: 0.0, // Not meaningful for final stats
            average_rate,
            runtime_seconds: total_runtime.as_secs_f64(),
            last_batch_time: Some(now),
        }
    }

    pub async fn reset(&self) {
        let mut state = self.state.write().await;
        let now = Instant::now();

        state.total_records = 0;
        state.total_batches = 0;
        state.total_errors = 0;
        state.start_time = now;
        state.last_report_time = now;
        state.last_report_records = 0;
        state.records_in_last_period = 0;

        info!("Performance monitor reset");
    }

    pub async fn get_throughput_history(&self, _duration_seconds: u64) -> Vec<(Instant, f64)> {
        // This is a simplified version - in a full implementation,
        // you might want to keep a rolling window of historical data
        let state = self.state.read().await;
        let now = Instant::now();
        let average_rate = if state.start_time.elapsed().as_secs_f64() > 0.0 {
            state.total_records as f64 / state.start_time.elapsed().as_secs_f64()
        } else {
            0.0
        };

        vec![(now, average_rate)]
    }

    /// Get error rate as percentage
    pub async fn get_error_rate(&self) -> f64 {
        let state = self.state.read().await;
        if state.total_batches > 0 {
            (state.total_errors as f64 / state.total_batches as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Get records per batch
    pub async fn get_average_batch_size(&self) -> f64 {
        let state = self.state.read().await;
        if state.total_batches > 0 {
            state.total_records as f64 / state.total_batches as f64
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

        // Add some records
        monitor.add_records(100).await;
        monitor.add_records(50).await;

        let stats = monitor.get_current_stats().await;
        assert_eq!(stats.total_records, 150);
        assert_eq!(stats.total_batches, 2);
        assert_eq!(stats.total_errors, 0);
    }

    #[tokio::test]
    async fn test_monitor_error_tracking() {
        let monitor = PerformanceMonitor::new();

        monitor.add_records(100).await;
        monitor.add_error().await;
        monitor.add_records(50).await;

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

        monitor.add_records(100).await;

        // Wait a bit to ensure time passes
        sleep(Duration::from_millis(10)).await;

        let stats = monitor.get_current_stats().await;
        assert!(stats.average_rate > 0.0);
        assert!(stats.runtime_seconds > 0.0);
    }

    #[tokio::test]
    async fn test_monitor_reset() {
        let monitor = PerformanceMonitor::new();

        monitor.add_records(100).await;
        monitor.add_error().await;

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

        monitor.add_records(100).await;
        monitor.add_records(200).await;
        monitor.add_records(50).await;

        let avg_batch_size = monitor.get_average_batch_size().await;
        assert_eq!(avg_batch_size, 350.0 / 3.0); // Total records / total batches
    }
}
