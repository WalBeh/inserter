use anyhow::{Context, Result};
use base64::Engine;
use clap::Parser;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

mod client;
mod config;
mod generator;
mod monitor;

// Dashboard module disabled for now
// #[cfg(feature = "dashboard")]
// mod dashboard;

use client::CrateClient;
use config::Config;
use generator::RecordGenerator;
use monitor::{CpuThrottleConfig, PerformanceMonitor, QueueThrottleConfig, ThreadPoolMonitor};

#[derive(Parser)]
#[command(
    name = "crate-write",
    about = "High-performance CrateDB record generator and inserter",
    long_about = "A Rust implementation of the CrateDB record generator with maximum performance and concurrency"
)]
struct Cli {
    /// Name of the CrateDB table to insert records into
    #[arg(long)]
    table_name: Option<String>,

    /// CrateDB connection string (can be read from .env file)
    #[arg(long, env = "CRATE_CONNECTION_STRING")]
    connection_string: Option<String>,

    /// Duration to run the generator (in minutes)
    #[arg(long)]
    duration: Option<u64>,

    /// Number of records to insert in each batch
    #[arg(long)]
    batch_size: Option<usize>,

    /// Interval between batches in milliseconds
    #[arg(long)]
    batch_interval: Option<u64>,

    /// Number of parallel worker tasks
    #[arg(long)]
    threads: Option<usize>,

    /// Number of additional low-cardinality object columns to create
    #[arg(long)]
    objects: Option<usize>,

    /// Enable real-time dashboard (not implemented yet)
    #[arg(long)]
    dashboard: bool,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Configuration file path
    #[arg(long)]
    config: Option<PathBuf>,

    /// Run only the 5-tuple load balancer test (no table creation or data insertion)
    #[arg(long)]
    test_loadbalancer: bool,

    /// Benchmark mode: minimal output, JSON result to stdout
    #[arg(long)]
    benchmark: bool,

    /// Enable adaptive batching (dynamically adjusts batch_size and batch_interval)
    #[arg(long)]
    adaptive_batching: Option<bool>,

    /// Minimum batch size when adaptive batching is enabled
    #[arg(long)]
    min_batch_size: Option<usize>,

    /// Maximum batch size when adaptive batching is enabled
    #[arg(long)]
    max_batch_size: Option<usize>,

    /// Target latency for inserts in milliseconds (adaptive batching)
    #[arg(long)]
    target_latency_ms: Option<f64>,

    /// Percentage tolerance for target latency (adaptive batching)
    #[arg(long)]
    latency_tolerance_pct: Option<f64>,

    /// Factor by which to increase/decrease batch size (adaptive batching)
    #[arg(long)]
    batch_size_factor: Option<f64>,

    /// Minimum batch interval in milliseconds (adaptive batching)
    #[arg(long)]
    min_batch_interval: Option<u64>,

    /// Maximum batch interval in milliseconds (adaptive batching)
    #[arg(long)]
    max_batch_interval: Option<u64>,

    /// Factor by which to increase/decrease batch interval (adaptive batching)
    #[arg(long)]
    batch_interval_factor: Option<f64>,

    /// Number of shards for table creation
    #[arg(long)]
    shards: Option<usize>,

    /// Number of replicas for table creation
    #[arg(long)]
    replicas: Option<usize>,

    /// Disable gzip compression (faster on localhost/low-latency)
    #[arg(long)]
    no_compression: bool,

    /// Enable queue-based throttling (adjusts worker sleep based on write queue depth)
    #[arg(long)]
    queue_throttle: bool,

    /// Write thread pool queue capacity (default: 200, matches CrateDB default)
    #[arg(long, default_value_t = 200)]
    queue_capacity: u64,

    /// Queue fill percentage at which throttling starts (default: 50)
    #[arg(long, default_value_t = 50)]
    queue_throttle_pct: u64,

    /// Enable CPU-based throttling (reduces concurrency when cluster CPU exceeds target)
    #[arg(long)]
    cpu_throttle: bool,

    /// Target max cluster CPU percentage (default: 50)
    #[arg(long, default_value_t = 50)]
    max_cpu_load_pct: u64,
}

fn sanitize_connection_string(connection_string: &str) -> String {
    match url::Url::parse(connection_string) {
        Ok(url) => {
            format!(
                "{}://{}:{}",
                url.scheme(),
                url.host_str().unwrap_or("unknown"),
                url.port().unwrap_or(4200)
            )
        }
        Err(_) => "invalid-connection-string".to_string(),
    }
}

async fn create_table(
    client: &CrateClient,
    table_name: &str,
    objects: usize,
    shards: usize,
    replicas: usize,
) -> Result<()> {
    let mut columns = vec![
        "id TEXT PRIMARY KEY".to_string(),
        "timestamp TIMESTAMP WITH TIME ZONE".to_string(),
        "region TEXT".to_string(),
        "product_category TEXT".to_string(),
        "event_type TEXT".to_string(),
        "user_id INTEGER".to_string(),
        "user_segment TEXT".to_string(),
        "amount DOUBLE PRECISION".to_string(),
        "quantity INTEGER".to_string(),
        "metadata OBJECT(DYNAMIC)".to_string(),
    ];

    // Add object columns
    for i in 0..objects {
        columns.push(format!("obj_{} TEXT", i));
    }

    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) CLUSTERED INTO {} SHARDS WITH (number_of_replicas = {})",
        table_name,
        columns.join(", "),
        shards,
        replicas
    );

    info!("Creating table: {}", table_name);
    client
        .execute(&sql, &[])
        .await
        .with_context(|| format!("Failed to create table {}", table_name))?;

    info!("✅ Table '{}' created successfully", table_name);
    Ok(())
}

async fn query_cluster_info(client: &CrateClient) -> serde_json::Value {
    let mut info = serde_json::json!({});

    if let Ok(rows) = client
        .execute_query("SELECT os_info['available_processors'] FROM sys.nodes")
        .await
    {
        let cpus: Vec<u64> = rows
            .iter()
            .filter_map(|r| r.first().and_then(|v| v.as_u64()))
            .collect();
        info["cpus_per_node"] = serde_json::json!(cpus);
        info["total_cpus"] = serde_json::json!(cpus.iter().sum::<u64>());
        info["nodes"] = serde_json::json!(cpus.len());
    }
    if let Ok(rows) = client
        .execute_query("SELECT mem['used'] FROM sys.nodes")
        .await
    {
        let mem: Vec<u64> = rows
            .iter()
            .filter_map(|r| r.first().and_then(|v| v.as_u64()))
            .collect();
        info["memory_used_bytes"] = serde_json::json!(mem);
    }
    if let Ok(rows) = client
        .execute_query("SELECT fs['total']['size'] FROM sys.nodes")
        .await
    {
        let disk: Vec<u64> = rows
            .iter()
            .filter_map(|r| r.first().and_then(|v| v.as_u64()))
            .collect();
        info["disk_total_bytes"] = serde_json::json!(disk);
    }
    if let Ok(rows) = client
        .execute_query("SELECT heap['max'], version['number'] FROM sys.nodes LIMIT 1")
        .await
    {
        if let Some(row) = rows.first() {
            if let Some(heap) = row.first().and_then(|v| v.as_u64()) {
                info["heap_max_bytes"] = serde_json::json!(heap);
            }
            if let Some(ver) = row.get(1).and_then(|v| v.as_str()) {
                info["version"] = serde_json::json!(ver);
            }
        }
    }
    info
}

/// Shared state for worker tasks that allows dynamic adjustment of batching parameters.
#[derive(Debug, Clone)]
pub struct SharedWorkerState {
    pub current_batch_size: Arc<AtomicUsize>,
    pub current_batch_interval: Arc<AtomicU64>,
    // Adaptive Batching parameters from Config
    pub adaptive_batching_enabled: bool,
    pub min_batch_size: usize,
    pub max_batch_size: usize,
    pub target_latency_ms: f64,
    pub latency_tolerance_pct: f64,
    pub batch_size_factor: f64,
    pub min_batch_interval: u64,
    pub max_batch_interval: u64,
    pub batch_interval_factor: f64,
}

impl SharedWorkerState {
    pub fn new(config: &Config) -> Self {
        Self {
            current_batch_size: Arc::new(AtomicUsize::new(config.batch_size)),
            current_batch_interval: Arc::new(AtomicU64::new(config.batch_interval)),
            adaptive_batching_enabled: config.adaptive_batching,
            min_batch_size: config.min_batch_size,
            max_batch_size: config.max_batch_size,
            target_latency_ms: config.target_latency_ms,
            latency_tolerance_pct: config.latency_tolerance_pct,
            batch_size_factor: config.batch_size_factor,
            min_batch_interval: config.min_batch_interval,
            max_batch_interval: config.max_batch_interval,
            batch_interval_factor: config.batch_interval_factor,
        }
    }
}

async fn run_data_generation(
    client: CrateClient,
    config: Config,
    monitor: PerformanceMonitor,
    benchmark: bool,
    queue_throttle: QueueThrottleConfig,
    cpu_throttle: CpuThrottleConfig,
) -> Result<()> {
    let shared_worker_state = SharedWorkerState::new(&config);

    let table_name = config.table_name.as_ref().unwrap();

    // Create table
    create_table(
        &client,
        table_name,
        config.objects,
        config.shards,
        config.replicas,
    )
    .await?;

    // Query cluster info
    let cluster_info = query_cluster_info(&client).await;

    // Create thread pool monitor and capture baseline counters
    let tp_monitor = ThreadPoolMonitor::new(client.clone(), config.threads);
    tp_monitor.capture_baseline().await;

    // Get pre-existing record count and rejected writes baseline
    let pre_count = {
        let _ = client
            .execute(&format!("REFRESH TABLE {}", table_name), &[])
            .await;
        client
            .execute_query(&format!("SELECT COUNT(*) FROM {}", table_name))
            .await
            .ok()
            .and_then(|rows| {
                rows.first()
                    .and_then(|r| r.first().and_then(|v| v.as_u64()))
            })
            .unwrap_or(0)
    };
    // Prepare insert statement
    let mut placeholders = vec!["?"; 10]; // Base fields
    placeholders.extend(vec!["?"; config.objects]); // Object fields

    let mut field_names = vec![
        "id",
        "timestamp",
        "region",
        "product_category",
        "event_type",
        "user_id",
        "user_segment",
        "amount",
        "quantity",
        "metadata",
    ];
    let mut obj_field_names = Vec::new();
    for i in 0..config.objects {
        obj_field_names.push(format!("obj_{}", i));
    }
    for field in &obj_field_names {
        field_names.push(field);
    }

    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name,
        field_names.join(", "),
        placeholders.join(", ")
    );

    info!("Starting {} worker tasks...", config.threads);

    // Create shutdown signal
    let (shutdown_tx, _shutdown_rx) = tokio::sync::broadcast::channel(1);

    // Spawn worker tasks
    let mut tasks = Vec::new();
    for worker_id in 0..config.threads {
        let client = client.clone();
        let monitor = monitor.clone();
        let generator = Arc::new(std::sync::Mutex::new(RecordGenerator::new(config.objects)));
        let insert_sql = insert_sql.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let worker_state = shared_worker_state.clone();
        let concurrency_sem = tp_monitor.concurrency_sem.clone();
        let throttle_enabled = queue_throttle.enabled || cpu_throttle.enabled;

        let task = tokio::spawn(async move {
            loop {
                // Get current batch_size and batch_interval from shared state
                let batch_size = worker_state.current_batch_size.load(Ordering::Relaxed);
                let batch_interval = Duration::from_millis(
                    worker_state.current_batch_interval.load(Ordering::Relaxed),
                );

                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Worker {} shutting down", worker_id);
                        break;
                    }
                    _ = async {
                        // Generate batch + convert to params on blocking thread
                        let gen = generator.clone();
                        let params = tokio::task::spawn_blocking(move || {
                            let batch = gen.lock().expect("generator lock poisoned").generate_batch(batch_size);
                            batch.into_iter()
                                .map(|record| record.into_params())
                                .collect::<Vec<Vec<serde_json::Value>>>()
                        }).await.expect("batch generation panicked");

                        // Acquire concurrency permit (blocks if throttle has reduced permits)
                        let _permit = if throttle_enabled {
                            Some(concurrency_sem.acquire().await.expect("semaphore closed"))
                        } else {
                            None
                        };

                        // Insert batch
                        let (success, latency_ms) = match client.execute_bulk(&insert_sql, params).await {
                            Ok((bytes_sent, latency)) => {
                                monitor.add_records(batch_size);
                                monitor.add_request_stats(bytes_sent, latency).await;
                                (true, latency)
                            }
                            Err(e) => {
                                error!("Worker {} error: {}", worker_id, e);
                                monitor.add_error();
                                (false, -1.0)
                            }
                        };

                        drop(_permit); // release permit after response

                        // Adaptive batching logic
                        if worker_state.adaptive_batching_enabled {
                            if success {
                                if latency_ms < worker_state.target_latency_ms * (1.0 - worker_state.latency_tolerance_pct / 100.0) {
                                    // Latency well below target, cautiously increase batch size
                                    let current = worker_state.current_batch_size.load(Ordering::Relaxed);
                                    let new_size = ((current as f64 * worker_state.batch_size_factor) as usize)
                                        .min(worker_state.max_batch_size)
                                        .max(worker_state.min_batch_size);
                                    worker_state.current_batch_size.store(new_size, Ordering::Relaxed);
                                    debug!("Worker {} increased batch_size to {}", worker_id, new_size);
                                } else if latency_ms > worker_state.target_latency_ms * (1.0 + worker_state.latency_tolerance_pct / 100.0) {
                                    // Latency too high, decrease batch size and increase interval
                                    let current_size = worker_state.current_batch_size.load(Ordering::Relaxed);
                                    let new_size = ((current_size as f64 / worker_state.batch_size_factor) as usize)
                                        .min(worker_state.max_batch_size)
                                        .max(worker_state.min_batch_size);
                                    worker_state.current_batch_size.store(new_size, Ordering::Relaxed);
                                    debug!("Worker {} decreased batch_size to {}", worker_id, new_size);

                                    let current_interval = worker_state.current_batch_interval.load(Ordering::Relaxed);
                                    let new_interval = ((current_interval as f64 * worker_state.batch_interval_factor) as u64)
                                        .min(worker_state.max_batch_interval)
                                        .max(worker_state.min_batch_interval);
                                    worker_state.current_batch_interval.store(new_interval, Ordering::Relaxed);
                                    debug!("Worker {} increased batch_interval to {}ms", worker_id, new_interval);
                                }
                            } else { // On error, aggressively reduce batch size and increase interval
                                let current_size = worker_state.current_batch_size.load(Ordering::Relaxed);
                                let new_size = ((current_size as f64 / (worker_state.batch_size_factor * 2.0)) as usize)
                                    .min(worker_state.max_batch_size)
                                    .max(worker_state.min_batch_size);
                                worker_state.current_batch_size.store(new_size, Ordering::Relaxed);
                                debug!("Worker {} (error) decreased batch_size to {}", worker_id, new_size);

                                let current_interval = worker_state.current_batch_interval.load(Ordering::Relaxed);
                                let new_interval = ((current_interval as f64 * (worker_state.batch_interval_factor * 2.0)) as u64)
                                    .min(worker_state.max_batch_interval)
                                    .max(worker_state.min_batch_interval);
                                worker_state.current_batch_interval.store(new_interval, Ordering::Relaxed);
                                debug!("Worker {} (error) increased batch_interval to {}ms", worker_id, new_interval);
                            }
                        }

                        // Wait before next batch
                        if !batch_interval.is_zero() {
                            tokio::time::sleep(batch_interval).await;
                        }
                    } => {}
                }
            }
        });

        tasks.push(task);
    }

    // Spawn thread pool poller (1s interval, collects active/queue per node)
    let tp_poller = tp_monitor.spawn_poller(shutdown_tx.subscribe(), queue_throttle.clone(), cpu_throttle.clone());

    // Spawn reporting task (collects rate samples; logs only in non-benchmark mode)
    let monitor_clone = monitor.clone();
    let shutdown_tx_clone = shutdown_tx.clone();
    let benchmark_mode = benchmark;
    let num_threads = config.threads;
    let worker_state_clone = shared_worker_state.clone(); // Clone for reporting task
    let reporting_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        let mut shutdown_rx = shutdown_tx_clone.subscribe();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                _ = interval.tick() => {
                    let stats = monitor_clone.get_current_stats().await;
                    let current_batch_size = worker_state_clone.current_batch_size.load(Ordering::Relaxed);
                    let current_batch_interval = worker_state_clone.current_batch_interval.load(Ordering::Relaxed);

                    if !benchmark_mode {
                        info!(
                            "Performance: {:.1} records/sec (current), {:.1} records/sec (avg), Total: {} records, Batches: {}, Threads: {}, Errors: {}, Batch Size: {}, Batch Interval: {}ms",
                            stats.current_rate,
                            stats.average_rate,
                            stats.total_records,
                            stats.total_batches,
                            num_threads,
                            stats.total_errors,
                            current_batch_size,
                            current_batch_interval,
                        );
                    }
                }
            }
        }
    });

    // Wait for duration or Ctrl+C
    let duration_future = tokio::time::sleep(Duration::from_secs(config.duration.unwrap() * 60));
    let ctrl_c_future = tokio::signal::ctrl_c();

    tokio::select! {
        _ = duration_future => {
            info!("Duration completed, stopping workers...");
        }
        _ = ctrl_c_future => {
            warn!("Received interrupt signal, stopping workers...");
        }
    }

    // Signal shutdown
    let _ = shutdown_tx.send(());

    // Wait for all tasks to complete
    for task in tasks {
        let _ = task.await;
    }
    let _ = reporting_task.await;
    let _ = tp_poller.await;

    // Finalize thread pool stats (query final counters, compute percentiles)
    let thread_pool_stats = tp_monitor.finalize().await;

    // Collect one final rate sample
    let _ = monitor.get_current_stats().await;

    // Final statistics
    let final_stats = monitor.get_final_stats_async().await;

    // Verify records
    let verified_count = {
        let _ = client
            .execute(&format!("REFRESH TABLE {}", table_name), &[])
            .await;
        let post_count = client
            .execute_query(&format!("SELECT COUNT(*) FROM {}", table_name))
            .await
            .ok()
            .and_then(|rows| {
                rows.first()
                    .and_then(|r| r.first().and_then(|v| v.as_u64()))
            })
            .unwrap_or(0);
        post_count.saturating_sub(pre_count)
    };

    let rejected_writes = thread_pool_stats
        .as_ref()
        .map(|tp| tp.total_rejected)
        .unwrap_or(0);

    // Query average row size on disk
    let avg_row_bytes = client
        .execute_query(&format!(
            "SELECT CASE WHEN SUM(num_docs) > 0 THEN ROUND(SUM(size)::double precision / SUM(num_docs), 0) ELSE 0 END FROM sys.shards WHERE table_name = '{}' AND primary = true",
            table_name
        ))
        .await
        .ok()
        .and_then(|rows| rows.first().and_then(|r| r.first().and_then(|v| v.as_u64())))
        .unwrap_or(0);

    if benchmark {
        // Benchmark mode: JSONL to stdout
        let rate_stats = monitor.get_percentile_stats().await;
        let latency_stats = monitor.get_latency_stats().await;
        let bandwidth_mbps = monitor.get_bandwidth_mbps().await;
        let bytes_sent = monitor.get_total_bytes_sent();
        let total_cpus = cluster_info
            .get("total_cpus")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as f64;
        let per_cpu = serde_json::json!({
            "avg": (rate_stats.avg / total_cpus * 10.0).round() / 10.0,
            "min": (rate_stats.min / total_cpus * 10.0).round() / 10.0,
            "max": (rate_stats.max / total_cpus * 10.0).round() / 10.0,
            "p90": (rate_stats.p90 / total_cpus * 10.0).round() / 10.0,
            "p95": (rate_stats.p95 / total_cpus * 10.0).round() / 10.0,
        });

        // Build cluster info with thread pool stats
        let mut cluster_with_tp = cluster_info.clone();
        if let Some(ref tp) = thread_pool_stats {
            cluster_with_tp["write_thread_pool"] = serde_json::json!(tp);
        }

        let result = serde_json::json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "client": "rust-http",
            "cluster": cluster_with_tp,
            "config": {
                "threads": config.threads,
                "initial_batch_size": config.batch_size,
                "initial_batch_interval_ms": config.batch_interval,
                "actual_final_batch_size": shared_worker_state.current_batch_size.load(Ordering::Relaxed),
                "actual_final_batch_interval_ms": shared_worker_state.current_batch_interval.load(Ordering::Relaxed),
                "num_objects_generated": config.objects,
                "duration_minutes": config.duration.unwrap(),
                "table_name": table_name,
                "shards": config.shards,
                "replicas": config.replicas,
                "adaptive_batching_enabled": config.adaptive_batching,
                "min_batch_size": config.min_batch_size,
                "max_batch_size": config.max_batch_size,
                "target_latency_ms": config.target_latency_ms,
                "latency_tolerance_pct": config.latency_tolerance_pct,
                "batch_size_factor": config.batch_size_factor,
                "min_batch_interval": config.min_batch_interval,
                "max_batch_interval": config.max_batch_interval,
                "batch_interval_factor": config.batch_interval_factor,
                "queue_throttle": queue_throttle.enabled,
                "queue_capacity": queue_throttle.capacity,
                "queue_throttle_pct": (queue_throttle.threshold_pct * 100.0) as u64,
                "cpu_throttle": cpu_throttle.enabled,
                "max_cpu_load_pct": (cpu_throttle.max_cpu_pct * 100.0) as u64,
            },
            "results": {
                "total_records": final_stats.total_records,
                "total_batches": final_stats.total_batches,
                "runtime_seconds": (final_stats.runtime_seconds * 10.0).round() / 10.0,
                "errors": final_stats.total_errors,
                "records_per_second": rate_stats,
                "records_per_cpu_second": per_cpu,
                "request_latency_ms": latency_stats,
                "bytes_sent": bytes_sent,
                "bandwidth_mbps": (bandwidth_mbps * 100.0).round() / 100.0,
                "verified_count": verified_count,
                "rejected_writes": rejected_writes,
                "rejected_pct": (rejected_writes as f64 / final_stats.total_records.max(1) as f64 * 100.0 * 100.0).round() / 100.0,
                "average_batch_size": monitor.get_average_batch_size(),
                "error_rate_pct": (monitor.get_error_rate().await * 10.0).round() / 10.0,
                "final_concurrency": tp_monitor.current_concurrency.load(Ordering::Relaxed),
                "avg_row_bytes_on_disk": avg_row_bytes,
                "avg_payload_bytes": if final_stats.total_records > 0 { bytes_sent / final_stats.total_records } else { 0 },
            }
        });
        println!("{}", serde_json::to_string(&result).unwrap());
        // Summary to stderr for quick reading
        let version = cluster_info
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        let avg = per_cpu.get("avg").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let p95 = per_cpu.get("p95").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let max = per_cpu.get("max").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let total_cpus_int = cluster_info
            .get("total_cpus")
            .and_then(|v| v.as_u64())
            .unwrap_or(1);
        let effective_rec_per_cpu = (final_stats.total_records as f64
            / final_stats.runtime_seconds
            / total_cpus_int as f64)
            .round();
        let rej_str = if rejected_writes > 0 {
            let total = final_stats.total_records.max(1) as f64;
            let rej_pct = (rejected_writes as f64 / total) * 100.0;
            format!(" | REJECTED: {} ({:.1}%)", rejected_writes, rej_pct)
        } else {
            String::new()
        };
        eprintln!(
            "CrateDB {} | {} CPUs | p90={:.0} rec/s | per CPU: avg={:.0} p95={:.0} max={:.0} | effective rec/cpu/s={:.0} | avg_row={}B{}",
            version, total_cpus_int, rate_stats.p90, avg, p95, max, effective_rec_per_cpu, avg_row_bytes, rej_str
        );
    } else {
        // Normal mode
        info!("{}", "=".repeat(60));
        info!("FINAL PERFORMANCE SUMMARY");
        info!("{}", "=".repeat(60));
        info!("✅ Worker threads: {}", config.threads);
        info!("✅ Total records sent: {}", final_stats.total_records);
        info!("✅ Total batches: {}", final_stats.total_batches);
        info!(
            "✅ Total runtime: {:.1} seconds",
            final_stats.runtime_seconds
        );
        info!(
            "✅ Average insertion rate: {:.1} records/second",
            final_stats.average_rate
        );
        info!(
            "✅ Records per thread: {:.0} avg",
            final_stats.total_records as f64 / config.threads as f64
        );
        info!("✅ Total errors: {}", final_stats.total_errors);
        info!("{}", "=".repeat(60));

        info!("RECORD VERIFICATION");
        info!(
            "Records sent: {}  |  Verified in CrateDB: {}",
            final_stats.total_records, verified_count
        );
        if verified_count == final_stats.total_records {
            info!("✅ MATCH");
        } else if verified_count > final_stats.total_records {
            info!(
                "ℹ️  CrateDB has {} extra (pre-existing data)",
                verified_count - final_stats.total_records
            );
        } else {
            let missing = final_stats.total_records - verified_count;
            warn!(
                "⚠️  MISMATCH - {} missing ({:.2}% loss)",
                missing,
                (missing as f64 / final_stats.total_records as f64) * 100.0
            );
        }
        info!("{}", "=".repeat(60));
    }

    // Clean up: drop the benchmark table
    let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
    match client.execute(&drop_sql, &[]).await {
        Ok(_) => info!("Cleaned up table '{}'", table_name),
        Err(e) => warn!("Failed to drop table: {}", e),
    }

    Ok(())
}

struct FreshRequestResult {
    node_name: String,
    source_port: u16,
    connect_time_ms: f64,
    request_time_ms: f64,
    total_time_ms: f64,
}

async fn make_fresh_request(
    host: &str,
    port: u16,
    use_tls: bool,
    auth_header: Option<&str>,
) -> Result<FreshRequestResult> {
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let start_connect = Instant::now();

    let addr = format!("{}:{}", host, port);
    let stream = tokio::net::TcpStream::connect(&addr)
        .await
        .with_context(|| format!("Failed to connect to {}", addr))?;

    let source_port = stream.local_addr()?.port();
    let connect_time = start_connect.elapsed();

    // Build HTTP request
    let mut http_request = format!(
        "GET / HTTP/1.1\r\nHost: {}:{}\r\nConnection: close\r\nUser-Agent: CrateDB-5Tuple-Tester/1.0\r\n",
        host, port
    );
    if let Some(auth) = auth_header {
        http_request.push_str(&format!("Authorization: {}\r\n", auth));
    }
    http_request.push_str("\r\n");

    let start_request = Instant::now();

    if use_tls {
        let connector = tokio_native_tls::TlsConnector::from(
            native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()?,
        );
        let mut tls_stream = connector.connect(host, stream).await?;
        tls_stream.write_all(http_request.as_bytes()).await?;
        let mut response_data = Vec::new();
        tls_stream.read_to_end(&mut response_data).await?;
        let request_time = start_request.elapsed();
        let node_name = parse_node_name(&response_data);
        Ok(FreshRequestResult {
            node_name,
            source_port,
            connect_time_ms: connect_time.as_secs_f64() * 1000.0,
            request_time_ms: request_time.as_secs_f64() * 1000.0,
            total_time_ms: start_connect.elapsed().as_secs_f64() * 1000.0,
        })
    } else {
        let mut stream = stream;
        stream.write_all(http_request.as_bytes()).await?;
        let mut response_data = Vec::new();
        stream.read_to_end(&mut response_data).await?;
        let request_time = start_request.elapsed();
        let node_name = parse_node_name(&response_data);
        Ok(FreshRequestResult {
            node_name,
            source_port,
            connect_time_ms: connect_time.as_secs_f64() * 1000.0,
            request_time_ms: request_time.as_secs_f64() * 1000.0,
            total_time_ms: start_connect.elapsed().as_secs_f64() * 1000.0,
        })
    }
}

fn parse_node_name(response_data: &[u8]) -> String {
    let response_text = String::from_utf8_lossy(response_data);
    // Split headers from body
    if let Some(body_start) = response_text.find("\r\n\r\n") {
        let body = &response_text[body_start + 4..];
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(body) {
            if let Some(name) = json.get("name").and_then(|v| v.as_str()) {
                return shorten_node_name(name);
            }
        }
    }
    "unknown".to_string()
}

fn shorten_node_name(name: &str) -> String {
    // Extract pattern like "prefix" + "number" from node names
    let re_like: Option<(String, String)> = {
        let mut alpha = String::new();
        let mut digit = String::new();
        let mut found_digit = false;
        for ch in name.chars() {
            if !found_digit && ch.is_alphabetic() {
                alpha.push(ch);
            } else if ch.is_ascii_digit() {
                found_digit = true;
                digit.push(ch);
            }
        }
        if !alpha.is_empty() && !digit.is_empty() {
            Some((alpha, digit))
        } else {
            None
        }
    };

    match re_like {
        Some((prefix, num)) => format!("{}-{}", prefix, num),
        None => name.chars().take(10).collect(),
    }
}

async fn test_5tuple_distribution(connection_string: &str) -> Result<()> {
    let parsed = url::Url::parse(connection_string).context("Invalid connection string")?;

    let host = parsed.host_str().context("Missing hostname")?.to_string();
    let port = parsed.port().unwrap_or(4200);
    let use_tls = parsed.scheme() == "https";

    let auth_header = if let Some(password) = parsed.password() {
        let username = parsed.username();
        let credentials = format!("{}:{}", username, password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(credentials.as_bytes());
        Some(format!("Basic {}", encoded))
    } else {
        None
    };

    println!("🔍 5-TUPLE LOAD BALANCER TEST");
    println!("{}", "=".repeat(60));
    println!(
        "Target: {}:{} ({})",
        host,
        port,
        if use_tls { "HTTPS" } else { "HTTP" }
    );

    // Query sys.nodes to determine cluster size
    let expected_nodes = {
        let client = CrateClient::new(connection_string).await?;
        match client
            .execute_query("SELECT count(*) as node_count FROM sys.nodes")
            .await
        {
            Ok(rows) => {
                if let Some(first) = rows.first() {
                    if let Some(first_val) = first.first() {
                        first_val.as_u64().unwrap_or(1) as usize
                    } else {
                        1
                    }
                } else {
                    1
                }
            }
            Err(_) => {
                println!("⚠️  Could not determine cluster size, assuming 1 node");
                1
            }
        }
    };
    println!("✅ Cluster has {} node(s)", expected_nodes);

    let num_requests = std::cmp::max(30, expected_nodes * 30);
    println!(
        "📊 Test plan: {} requests ({} per expected node)",
        num_requests,
        num_requests / expected_nodes
    );
    println!();

    println!("📊 Request Details:");
    println!("Req# |    Node    | SrcPort | ConnTime | ReqTime | TotalTime");
    println!("{}", "-".repeat(65));

    let mut node_counts: HashMap<String, usize> = HashMap::new();
    let mut source_ports: Vec<u16> = Vec::new();
    let mut failed_requests = 0usize;
    let mut successful_requests = 0usize;
    let mut total_connect_ms = 0.0f64;
    let mut total_request_ms = 0.0f64;
    let mut total_time_ms = 0.0f64;

    for i in 0..num_requests {
        match make_fresh_request(&host, port, use_tls, auth_header.as_deref()).await {
            Ok(result) => {
                if result.node_name == "unknown" || result.node_name.starts_with("error") {
                    failed_requests += 1;
                    println!(
                        "{:4} | {:10} | {:>7} | {:>8} | {:>7} | ERROR",
                        i + 1,
                        "ERROR",
                        "N/A",
                        "N/A",
                        "N/A"
                    );
                } else {
                    *node_counts.entry(result.node_name.clone()).or_insert(0) += 1;
                    source_ports.push(result.source_port);
                    successful_requests += 1;
                    total_connect_ms += result.connect_time_ms;
                    total_request_ms += result.request_time_ms;
                    total_time_ms += result.total_time_ms;

                    println!(
                        "{:4} | {:10} | {:7} | {:6.1}ms | {:5.1}ms | {:7.1}ms",
                        i + 1,
                        result.node_name,
                        result.source_port,
                        result.connect_time_ms,
                        result.request_time_ms,
                        result.total_time_ms
                    );
                }
            }
            Err(e) => {
                failed_requests += 1;
                println!(
                    "{:4} | {:10} | {:>7} | {:>8} | {:>7} | {}",
                    i + 1,
                    "ERROR",
                    "N/A",
                    "N/A",
                    "N/A",
                    e
                );
            }
        }

        // Small delay to ensure different source ports
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("{}", "-".repeat(65));

    let unique_ports = source_ports
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    let unique_nodes = node_counts.len();

    // Summary
    println!("\n📊 SUMMARY:");
    println!("   Total requests: {}", num_requests);
    println!("   Successful: {}", successful_requests);
    println!("   Failed: {}", failed_requests);
    println!("   Unique source ports: {}", unique_ports);
    println!("   Unique nodes hit: {}", unique_nodes);

    if successful_requests > 0 {
        println!(
            "   Avg connect time: {:.1}ms",
            total_connect_ms / successful_requests as f64
        );
        println!(
            "   Avg request time: {:.1}ms",
            total_request_ms / successful_requests as f64
        );
        println!(
            "   Avg total time: {:.1}ms",
            total_time_ms / successful_requests as f64
        );
    }

    // Distribution
    println!("\n📈 NODE DISTRIBUTION:");
    let mut sorted_nodes: Vec<_> = node_counts.iter().collect();
    sorted_nodes.sort_by_key(|(name, _)| name.clone());
    for (name, count) in &sorted_nodes {
        let percentage = (**count as f64 / successful_requests as f64) * 100.0;
        let bar = "█".repeat((percentage / 2.0) as usize);
        println!(
            "   {:15} | {:3} hits | {:5.1}% | {}",
            name, count, percentage, bar
        );
    }

    // 5-tuple analysis
    println!("\n🔍 5-TUPLE LOAD BALANCING ANALYSIS:");
    if unique_ports < 2 {
        println!("   ❌ INCONCLUSIVE: Need more unique source ports to test");
    } else {
        println!(
            "   ✅ Good test conditions: {} different source ports",
            unique_ports
        );

        if unique_nodes == 1 {
            println!("   🚨 VERDICT: Load balancer NOT using 5-tuple distribution");
            println!(
                "   📝 Evidence: {} different source ports, but all hit same node",
                unique_ports
            );
        } else if unique_nodes > 1 {
            println!("   ✅ VERDICT: Load balancer IS distributing across nodes");
            println!(
                "   📝 Evidence: {} source ports hit {} different nodes",
                unique_ports, unique_nodes
            );
        }
    }

    // Final verdict
    println!("\n{}", "=".repeat(60));
    println!("🎯 FINAL VERDICT");
    println!("{}", "=".repeat(60));
    println!("📊 CLUSTER ANALYSIS:");
    println!("   Expected nodes: {}", expected_nodes);
    println!("   Nodes hit during test: {}", unique_nodes);

    if unique_nodes == expected_nodes {
        println!(
            "   ✅ Perfect distribution - hit all {} nodes",
            expected_nodes
        );
    } else if unique_nodes < expected_nodes {
        println!(
            "   ⚠️  Partial distribution - hit {}/{} nodes",
            unique_nodes, expected_nodes
        );
    } else {
        println!(
            "   🤔 Unexpected - hit more nodes ({}) than expected ({})",
            unique_nodes, expected_nodes
        );
    }

    if unique_nodes == 1 && unique_ports > 5 {
        println!("\n🚨 CONFIRMED: Load balancer NOT using 5-tuple distribution");
        println!("💡 Contact CrateDB Cloud support about load balancer config");
    } else if unique_nodes > 1 {
        println!("\n✅ Load balancer IS distributing traffic across nodes");
        println!("🔧 Performance tests will distribute across nodes");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file FIRST
    let dotenv_result = dotenvy::dotenv();

    // Parse CLI arguments AFTER environment variables are loaded
    let cli = Cli::parse();

    // Initialize logging (suppress in benchmark mode)
    let effective_log_level = if cli.benchmark {
        "warn".to_string()
    } else {
        cli.log_level.clone()
    };
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&effective_log_level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    // Report .env loading status after logging is initialized
    match dotenv_result {
        Ok(path) => {
            info!("✅ Loaded .env file from: {:?}", path);
        }
        Err(e) => {
            warn!("⚠️  No .env file found or could not load: {}", e);
            info!("You can provide connection string via --connection-string or CRATE_CONNECTION_STRING env var");
        }
    }

    // Debug: Check if environment variable is set
    match std::env::var("CRATE_CONNECTION_STRING") {
        Ok(value) => {
            info!(
                "✅ CRATE_CONNECTION_STRING environment variable is set (length: {})",
                value.len()
            );
        }
        Err(_) => {
            warn!("❌ CRATE_CONNECTION_STRING environment variable is NOT set");
        }
    }

    // Handle load balancer test mode
    if cli.test_loadbalancer {
        // Need a connection string for the test
        let connection_string = cli.connection_string
            .or_else(|| std::env::var("CRATE_CONNECTION_STRING").ok())
            .context("Connection string required: use --connection-string or CRATE_CONNECTION_STRING env var")?;

        info!("🚀 CrateDB 5-Tuple Load Balancer Test");
        info!("{}", "=".repeat(60));
        info!("This test creates fresh TCP connections to properly test");
        info!("whether load balancers use 5-tuple hashing for distribution.");
        info!(
            "🔗 Connection: {}",
            sanitize_connection_string(&connection_string)
        );

        match test_5tuple_distribution(&connection_string).await {
            Ok(_) => {
                info!("✅ Load balancer test completed successfully");
                return Ok(());
            }
            Err(e) => {
                error!("❌ Load balancer test failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Load configuration: explicit --config, or auto-detect config.toml, or defaults
    let mut config = if let Some(config_path) = cli.config {
        Config::from_file(&config_path)?
    } else if Path::new("config.toml").exists() {
        info!("✅ Auto-detected config.toml in current directory");
        Config::from_file("config.toml")?
    } else {
        Config::default()
    };

    // Override with CLI arguments
    if let Some(table_name) = cli.table_name {
        config.table_name = Some(table_name);
    }
    if let Some(connection_string) = cli.connection_string {
        config.connection_string = Some(connection_string);
    }
    if let Some(duration) = cli.duration {
        config.duration = Some(duration);
    }
    if let Some(batch_size) = cli.batch_size {
        config.batch_size = batch_size;
    }
    if let Some(batch_interval) = cli.batch_interval {
        config.batch_interval = batch_interval;
    }
    if let Some(threads) = cli.threads {
        config.threads = threads;
    }
    if let Some(objects) = cli.objects {
        config.objects = objects;
    }
    if let Some(shards) = cli.shards {
        config.shards = shards;
    }
    if let Some(replicas) = cli.replicas {
        config.replicas = replicas;
    }

    // Adaptive batching CLI overrides
    if let Some(adaptive_batching) = cli.adaptive_batching {
        config.adaptive_batching = adaptive_batching;
    }
    if let Some(min_batch_size) = cli.min_batch_size {
        config.min_batch_size = min_batch_size;
    }
    if let Some(max_batch_size) = cli.max_batch_size {
        config.max_batch_size = max_batch_size;
    }
    if let Some(target_latency_ms) = cli.target_latency_ms {
        config.target_latency_ms = target_latency_ms;
    }
    if let Some(latency_tolerance_pct) = cli.latency_tolerance_pct {
        config.latency_tolerance_pct = latency_tolerance_pct;
    }
    if let Some(batch_size_factor) = cli.batch_size_factor {
        config.batch_size_factor = batch_size_factor;
    }
    if let Some(min_batch_interval) = cli.min_batch_interval {
        config.min_batch_interval = min_batch_interval;
    }
    if let Some(max_batch_interval) = cli.max_batch_interval {
        config.max_batch_interval = max_batch_interval;
    }
    if let Some(batch_interval_factor) = cli.batch_interval_factor {
        config.batch_interval_factor = batch_interval_factor;
    }

    // Validate configuration
    config.validate()?;

    info!("🚀 CrateDB Record Generator (Rust)");
    info!(
        "Connection: {}",
        sanitize_connection_string(config.connection_string.as_ref().unwrap())
    );

    // Create client with connection pool sized to thread count
    let pool_size = std::cmp::max(config.threads + 2, 10);
    let compress = !cli.no_compression;
    let client = CrateClient::with_options(
        config.connection_string.as_ref().unwrap(),
        pool_size,
        compress,
    )
    .await?;
    info!(
        "Connection pool size: {}, compression: {}",
        pool_size,
        if compress { "gzip" } else { "off" }
    );

    // Create performance monitor
    let monitor = PerformanceMonitor::new();

    // Run data generation
    let queue_throttle = QueueThrottleConfig {
        enabled: cli.queue_throttle,
        capacity: cli.queue_capacity,
        threshold_pct: cli.queue_throttle_pct as f64 / 100.0,
    };
    let cpu_throttle = CpuThrottleConfig {
        enabled: cli.cpu_throttle,
        max_cpu_pct: cli.max_cpu_load_pct as f64 / 100.0,
    };
    run_data_generation(client, config, monitor, cli.benchmark, queue_throttle, cpu_throttle).await?;

    Ok(())
}
