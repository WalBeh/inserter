use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use postgres::types::ToSql;
use postgres::{Client, NoTls, Statement};
use postgres_native_tls::MakeTlsConnector;
use serde_json::{json, Map, Value as JsonValue};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{info, warn};
use url::Url;

mod generator;

use generator::{Record, RecordGenerator};

#[derive(Parser, Debug, Clone)]
#[command(
    name = "crate-write-pg",
    about = "Tiny CrateDB batch inserter over the PostgreSQL wire protocol"
)]
struct Cli {
    /// CrateDB table name
    #[arg(long)]
    table_name: String,

    /// Connection string, e.g. postgres://user:pass@host:5432/db?sslmode=require
    #[arg(long, env = "CRATE_CONNECTION_STRING")]
    connection_string: String,

    /// Run time in minutes
    #[arg(long, default_value_t = 1)]
    duration: u64,

    /// Rows per batch insert
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,

    /// Number of worker threads
    #[arg(long, default_value_t = 4)]
    threads: usize,

    /// Delay between batches in milliseconds
    #[arg(long, default_value_t = 0)]
    batch_interval: u64,

    /// Extra low-cardinality object columns to generate
    #[arg(long, default_value_t = 0)]
    objects: usize,

    /// Number of shards for the created table
    #[arg(long, default_value_t = 4)]
    shards: usize,

    /// Number of replicas for the created table
    #[arg(long, default_value_t = 0)]
    replicas: usize,

    /// Do not create the table first
    #[arg(long)]
    no_create_table: bool,

    /// Output a JSON benchmark report and stderr summary
    #[arg(long)]
    benchmark: bool,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Enable queue-based throttling (adjusts concurrency based on write queue depth)
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

    /// Extra WITH options appended to CREATE TABLE, as comma-separated key=value pairs.
    /// Keys containing '.' are automatically double-quoted.
    /// Example: "translog.durability='ASYNC',translog.flush_threshold_size='512mb'"
    #[arg(long)]
    table_options: Option<String>,
}

#[derive(Clone)]
struct PgRecord {
    id: String,
    timestamp: chrono::DateTime<chrono::Utc>,
    region: String,
    product_category: String,
    event_type: String,
    user_id: i32,
    user_segment: String,
    amount: f64,
    quantity: i32,
    metadata: serde_json::Value,
    objects: Vec<String>,
}

impl PgRecord {
    /// Estimate PG wire payload size: 4-byte length prefix per field + field data.
    fn estimated_wire_bytes(&self) -> usize {
        let fixed = 4 + 8 + 4 + 8 + 4; // user_id(i32) + timestamp(i64) + quantity(i32) + amount(f64) + per-field overhead
        let metadata_len = serde_json::to_string(&self.metadata).map(|s| s.len()).unwrap_or(64);
        let strings = self.id.len() + self.region.len() + self.product_category.len()
            + self.event_type.len() + self.user_segment.len() + metadata_len
            + self.objects.iter().map(|s| s.len()).sum::<usize>();
        let num_fields = 10 + self.objects.len();
        fixed + strings + num_fields * 4
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct PercentileStats {
    avg: f64,
    min: f64,
    max: f64,
    p90: f64,
    p95: f64,
}

struct BenchmarkMonitor {
    start_time: Instant,
    last_sample_time: Mutex<Instant>,
    last_sample_rows: Mutex<u64>,
    total_rows: AtomicU64,
    total_batches: AtomicU64,
    total_errors: AtomicU64,
    total_bytes_sent: AtomicU64,
    rate_samples: Mutex<Vec<f64>>,
    latency_samples: Mutex<Vec<f64>>,
}

impl BenchmarkMonitor {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            start_time: now,
            last_sample_time: Mutex::new(now),
            last_sample_rows: Mutex::new(0),
            total_rows: AtomicU64::new(0),
            total_batches: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            total_bytes_sent: AtomicU64::new(0),
            rate_samples: Mutex::new(Vec::new()),
            latency_samples: Mutex::new(Vec::new()),
        }
    }

    fn add_batch(&self, rows: usize, bytes_sent: usize, latency_ms: f64) {
        self.total_rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_sent.fetch_add(bytes_sent as u64, Ordering::Relaxed);
        self.latency_samples
            .lock()
            .expect("latency_samples lock poisoned")
            .push(latency_ms);
    }

    fn add_error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn sample_rate(&self) {
        let total_rows = self.total_rows.load(Ordering::Relaxed);
        let mut last_rows = self
            .last_sample_rows
            .lock()
            .expect("last_sample_rows lock poisoned");
        let mut last_time = self
            .last_sample_time
            .lock()
            .expect("last_sample_time lock poisoned");
        let now = Instant::now();
        let elapsed = now.duration_since(*last_time).as_secs_f64();
        let delta = total_rows.saturating_sub(*last_rows);
        *last_rows = total_rows;
        *last_time = now;
        // Skip zero-rate samples (e.g. first tick before any batches complete)
        if delta > 0 {
            let rate = if elapsed > 0.0 { delta as f64 / elapsed } else { 0.0 };
            self.rate_samples
                .lock()
                .expect("rate_samples lock poisoned")
                .push(rate);
        }
    }

    fn final_stats(&self) -> JsonValue {
        let total_records = self.total_rows.load(Ordering::Relaxed);
        let total_batches = self.total_batches.load(Ordering::Relaxed);
        let total_errors = self.total_errors.load(Ordering::Relaxed);
        let runtime_seconds = self.start_time.elapsed().as_secs_f64();
        let average_rate = if runtime_seconds > 0.0 {
            total_records as f64 / runtime_seconds
        } else {
            0.0
        };

        json!({
            "total_records": total_records,
            "total_batches": total_batches,
            "total_errors": total_errors,
            "runtime_seconds": runtime_seconds,
            "average_rate": average_rate,
        })
    }

    fn average_batch_size(&self) -> f64 {
        let total_records = self.total_rows.load(Ordering::Relaxed);
        let total_batches = self.total_batches.load(Ordering::Relaxed);
        if total_batches > 0 {
            total_records as f64 / total_batches as f64
        } else {
            0.0
        }
    }

    fn error_rate_pct(&self) -> f64 {
        let total_batches = self.total_batches.load(Ordering::Relaxed);
        let total_errors = self.total_errors.load(Ordering::Relaxed);
        if total_batches > 0 {
            (total_errors as f64 / total_batches as f64) * 100.0
        } else {
            0.0
        }
    }

    fn rate_stats(&self) -> PercentileStats {
        let samples = self.rate_samples.lock().expect("rate_samples lock poisoned");
        compute_percentiles(&samples)
    }

    fn latency_stats(&self) -> PercentileStats {
        let samples = self
            .latency_samples
            .lock()
            .expect("latency_samples lock poisoned");
        compute_percentiles(&samples)
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

    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted.len();
    let idx = |p: f64| -> usize {
        let i = (n as f64 * p).floor() as usize;
        i.min(n - 1)
    };
    let avg = sorted.iter().sum::<f64>() / n as f64;

    PercentileStats {
        avg: (avg * 10.0).round() / 10.0,
        min: (sorted[0] * 10.0).round() / 10.0,
        max: (sorted[n - 1] * 10.0).round() / 10.0,
        p90: (sorted[idx(0.90)] * 10.0).round() / 10.0,
        p95: (sorted[idx(0.95)] * 10.0).round() / 10.0,
    }
}

// ── Thread Pool Monitor (synchronous) ────────────────────────────────────────

#[derive(Debug, Clone)]
struct NodeSample {
    active: u64,
    queue: u64,
    cpu: f64,
}

#[derive(Debug, Clone)]
struct NodePoolCounters {
    name: String,
    pool_size: u64,
    completed: u64,
    rejected: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
struct NodeThreadPoolStats {
    name: String,
    pool_size: u64,
    active: PercentileStats,
    queued: PercentileStats,
    cpu_usage: PercentileStats,
    completed_delta: u64,
    rejected_delta: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ClusterThreadPoolStats {
    total_pool_size: u64,
    total_completed: u64,
    total_rejected: u64,
    active_threads: PercentileStats,
    queued_tasks: PercentileStats,
    cpu_usage: PercentileStats,
    samples: usize,
    nodes: Vec<NodeThreadPoolStats>,
}

/// Find the "write" pool from the thread_pools JSON array.
fn find_write_pool(pools: &JsonValue) -> Option<&serde_json::Map<String, JsonValue>> {
    pools
        .as_array()?
        .iter()
        .filter_map(|v| v.as_object())
        .find(|obj| obj.get("name").and_then(|n| n.as_str()) == Some("write"))
}

/// Query thread_pools from sys.nodes. CrateDB returns ARRAY(OBJECT) as text
/// over the PG wire protocol; we parse it as JSON.
/// Query thread_pools via CrateDB's HTTP /_sql endpoint since the PG wire
/// protocol cannot serialize OBJECT/ARRAY(OBJECT) types from sys.nodes.
fn query_thread_pools_http(http_url: &str) -> Vec<(String, JsonValue, f64)> {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let url = match Url::parse(http_url) {
        Ok(u) => u,
        Err(_) => return Vec::new(),
    };
    let host = url.host_str().unwrap_or("localhost");
    let port = url.port().unwrap_or(4200);
    let body = r#"{"stmt":"SELECT name, thread_pools, process['cpu']['percent'] FROM sys.nodes ORDER BY name"}"#;

    let mut stream = match TcpStream::connect((host, port)) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));

    let request = format!(
        "POST /_sql HTTP/1.0\r\nHost: {}:{}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        host, port, body.len(), body
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return Vec::new();
    }

    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    let text = String::from_utf8_lossy(&response);

    // Split HTTP headers from body
    let json_body = match text.find("\r\n\r\n") {
        Some(pos) => &text[pos + 4..],
        None => return Vec::new(),
    };

    let parsed: JsonValue = match serde_json::from_str(json_body) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    parsed
        .get("rows")
        .and_then(|r| r.as_array())
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    let name = row.get(0)?.as_str()?.to_string();
                    let pools = row.get(1)?.clone();
                    let cpu = row.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
                    Some((name, pools, cpu))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn query_thread_pool_counters(http_url: &str) -> Vec<NodePoolCounters> {
    query_thread_pools_http(http_url)
        .into_iter()
        .filter_map(|(name, pools, _cpu)| {
            let pool = find_write_pool(&pools)?;
            Some(NodePoolCounters {
                name,
                pool_size: pool.get("threads").and_then(|v| v.as_u64()).unwrap_or(0),
                completed: pool.get("completed").and_then(|v| v.as_u64()).unwrap_or(0),
                rejected: pool.get("rejected").and_then(|v| v.as_u64()).unwrap_or(0),
            })
        })
        .collect()
}

fn poll_thread_pool_samples(http_url: &str) -> Vec<(String, NodeSample)> {
    query_thread_pools_http(http_url)
        .into_iter()
        .filter_map(|(name, pools, cpu)| {
            let pool = find_write_pool(&pools)?;
            Some((name, NodeSample {
                active: pool.get("active").and_then(|v| v.as_u64()).unwrap_or(0),
                queue: pool.get("queue").and_then(|v| v.as_u64()).unwrap_or(0),
                cpu,
            }))
        })
        .collect()
}

/// Derive the HTTP base URL from a PG connection string (same host, port 4200).
fn pg_to_http_url(connection_string: &str) -> String {
    match Url::parse(connection_string) {
        Ok(url) => format!("http://{}:{}", url.host_str().unwrap_or("localhost"), 4200),
        Err(_) => "http://localhost:4200".to_string(),
    }
}

fn finalize_thread_pool_stats(
    baseline: &[NodePoolCounters],
    final_counters: &[NodePoolCounters],
    samples: &std::collections::BTreeMap<String, Vec<NodeSample>>,
) -> Option<ClusterThreadPoolStats> {
    if samples.is_empty() {
        return None;
    }

    let mut nodes = Vec::new();
    let mut total_pool_size: u64 = 0;
    let mut total_completed: u64 = 0;
    let mut total_rejected: u64 = 0;
    let mut num_samples: usize = 0;

    for fc in final_counters {
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
        let cpu_vals: Vec<f64> = node_samples
            .map(|s| s.iter().map(|ns| ns.cpu).collect())
            .unwrap_or_default();

        if let Some(s) = node_samples {
            num_samples = num_samples.max(s.len());
        }

        nodes.push(NodeThreadPoolStats {
            name: fc.name.clone(),
            pool_size: fc.pool_size,
            active: compute_percentiles(&active_vals),
            queued: compute_percentiles(&queue_vals),
            cpu_usage: compute_percentiles(&cpu_vals),
            completed_delta,
            rejected_delta,
        });
    }

    let cluster_active: Vec<f64> = (0..num_samples)
        .map(|i| samples.values().filter_map(|v| v.get(i)).map(|s| s.active as f64).sum())
        .collect();
    let cluster_queue: Vec<f64> = (0..num_samples)
        .map(|i| samples.values().filter_map(|v| v.get(i)).map(|s| s.queue as f64).sum())
        .collect();
    let node_count = samples.len().max(1) as f64;
    let cluster_cpu: Vec<f64> = (0..num_samples)
        .map(|i| {
            let sum: f64 = samples.values().filter_map(|v| v.get(i)).map(|s| s.cpu).sum();
            sum / node_count
        })
        .collect();

    Some(ClusterThreadPoolStats {
        total_pool_size,
        total_completed,
        total_rejected,
        active_threads: compute_percentiles(&cluster_active),
        queued_tasks: compute_percentiles(&cluster_queue),
        cpu_usage: compute_percentiles(&cluster_cpu),
        samples: num_samples,
        nodes,
    })
}

// ── Concurrency Limiter (std::sync, for use with std::thread workers) ────────

/// A counting semaphore that the reporter thread can adjust at runtime.
/// Workers call acquire() before sending a request and release() after.
/// The reporter calls set_max() to raise or lower the limit.
struct ConcurrencyLimiter {
    state: Mutex<ConcurrencyState>,
    cond: Condvar,
}

struct ConcurrencyState {
    max: usize,
    active: usize,
}

impl ConcurrencyLimiter {
    fn new(max: usize) -> Self {
        Self {
            state: Mutex::new(ConcurrencyState { max, active: 0 }),
            cond: Condvar::new(),
        }
    }

    /// Block until a slot is available, then increment active count.
    fn acquire(&self) {
        let mut state = self.state.lock().unwrap();
        while state.active >= state.max {
            state = self.cond.wait(state).unwrap();
        }
        state.active += 1;
    }

    /// Release a slot and wake one waiting worker.
    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.active -= 1;
        self.cond.notify_one();
    }

    /// Adjust the concurrency limit. If increased, wake blocked workers.
    fn set_max(&self, new_max: usize) {
        let mut state = self.state.lock().unwrap();
        let old_max = state.max;
        state.max = new_max;
        if new_max > old_max {
            // Wake workers that might now be able to proceed
            self.cond.notify_all();
        }
    }

    fn current_max(&self) -> usize {
        self.state.lock().unwrap().max
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn default_pg_port(url: &Url) -> u16 {
    match url.scheme() {
        "http" | "https" => 5432,
        _ => 5432,
    }
}

fn parse_tls_mode(url: &Url) -> bool {
    let sslmode = url
        .query_pairs()
        .find(|(k, _)| k == "sslmode")
        .map(|(_, v)| v.to_string());

    match sslmode.as_deref() {
        Some("disable") => false,
        Some("allow") | Some("prefer") | Some("require") | Some("verify-ca") | Some("verify-full") => true,
        None => matches!(url.scheme(), "http" | "https"),
        Some(_) => true,
    }
}

fn connect(connection_string: &str) -> Result<Client> {
    let url = Url::parse(connection_string)
        .with_context(|| format!("invalid connection string: {connection_string}"))?;

    let host = url.host_str().context("missing host in connection string")?;
    let mut cfg = postgres::Config::new();
    cfg.host(host);
    cfg.port(url.port().unwrap_or_else(|| default_pg_port(&url)));

    if !url.username().is_empty() {
        cfg.user(url.username());
    }
    if let Some(pass) = url.password() {
        cfg.password(pass);
    }
    if let Some(db) = url
        .path_segments()
        .and_then(|mut s| s.next())
        .filter(|s| !s.is_empty())
    {
        cfg.dbname(db);
    }

    if parse_tls_mode(&url) {
        let tls = native_tls::TlsConnector::builder()
            .build()
            .context("failed to create TLS connector")?;
        let tls = MakeTlsConnector::new(tls);
        cfg.connect(tls).context("failed to connect to CrateDB")
    } else {
        cfg.connect(NoTls).context("failed to connect to CrateDB")
    }
}

fn record_to_pg(record: Record) -> PgRecord {
    PgRecord {
        id: record.id,
        timestamp: record.timestamp,
        region: record.region,
        product_category: record.product_category,
        event_type: record.event_type,
        user_id: record.user_id as i32,
        user_segment: record.user_segment,
        amount: record.amount,
        quantity: record.quantity as i32,
        metadata: serde_json::from_str(&record.metadata).unwrap_or(serde_json::Value::Null),
        objects: record.objects,
    }
}

fn build_columns(object_count: usize) -> Vec<String> {
    let mut cols = vec![
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

    for i in 0..object_count {
        cols.push(format!("obj_{i} TEXT"));
    }

    cols
}

fn parse_kv_pair(s: &str) -> anyhow::Result<(String, String)> {
    let eq = s.find('=').ok_or_else(|| anyhow::anyhow!(
        "invalid table option {:?}: expected key=value", s
    ))?;
    let key = s[..eq].trim().to_string();
    let value = s[eq + 1..].trim().to_string();
    if key.is_empty() {
        anyhow::bail!("empty key in table option {:?}", s);
    }
    if value.is_empty() {
        anyhow::bail!("empty value in table option {:?}", s);
    }
    Ok((key, value))
}

fn parse_table_options(s: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut result = Vec::new();
    let mut token = String::new();
    let mut in_quotes = false;

    for ch in s.chars() {
        match ch {
            '\'' => {
                in_quotes = !in_quotes;
                token.push(ch);
            }
            ',' if !in_quotes => {
                let pair = token.trim().to_string();
                if !pair.is_empty() {
                    result.push(parse_kv_pair(&pair)?);
                }
                token.clear();
            }
            _ => token.push(ch),
        }
    }
    if in_quotes {
        anyhow::bail!("unterminated single quote in --table-options");
    }
    let pair = token.trim().to_string();
    if !pair.is_empty() {
        result.push(parse_kv_pair(&pair)?);
    }
    Ok(result)
}

fn build_with_clause(replicas: usize, table_options: &[(String, String)]) -> String {
    let mut parts = vec![format!("number_of_replicas = {}", replicas)];
    for (key, value) in table_options {
        let quoted_key = if key.contains('.') {
            format!("\"{}\"", key)
        } else {
            key.clone()
        };
        parts.push(format!("{} = {}", quoted_key, value));
    }
    parts.join(", ")
}

fn table_options_to_json(opts: &[(String, String)]) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = opts
        .iter()
        .map(|(k, v)| {
            let val = if v.starts_with('\'') && v.ends_with('\'') && v.len() >= 2 {
                serde_json::Value::String(v[1..v.len() - 1].to_string())
            } else {
                serde_json::Value::String(v.clone())
            };
            (k.clone(), val)
        })
        .collect();
    serde_json::Value::Object(map)
}

fn create_table_sql(table_name: &str, shards: usize, replicas: usize, object_count: usize, table_options: &[(String, String)]) -> String {
    let columns = build_columns(object_count).join(", ");
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) CLUSTERED INTO {} SHARDS WITH ({})",
        quote_ident(table_name),
        columns,
        shards,
        build_with_clause(replicas, table_options)
    )
}

fn build_insert_sql(table_name: &str, rows: usize, object_count: usize) -> String {
    let mut column_names = vec![
        "id".to_string(),
        "timestamp".to_string(),
        "region".to_string(),
        "product_category".to_string(),
        "event_type".to_string(),
        "user_id".to_string(),
        "user_segment".to_string(),
        "amount".to_string(),
        "quantity".to_string(),
        "metadata".to_string(),
    ];
    for i in 0..object_count {
        column_names.push(format!("obj_{i}"));
    }

    let mut values = Vec::with_capacity(rows);
    let mut param = 1usize;
    for _ in 0..rows {
        let mut placeholders = Vec::with_capacity(column_names.len());
        for _ in 0..column_names.len() {
            placeholders.push(format!("${param}"));
            param += 1;
        }
        values.push(format!("({})", placeholders.join(", ")));
    }

    format!(
        "INSERT INTO {} ({}) VALUES {}",
        quote_ident(table_name),
        column_names
            .into_iter()
            .map(|c| quote_ident(&c))
            .collect::<Vec<_>>()
            .join(", "),
        values.join(", ")
    )
}

fn build_params_refs<'a>(records: &'a [PgRecord], object_count: usize) -> Vec<&'a (dyn ToSql + Sync)> {
    let mut out: Vec<&'a (dyn ToSql + Sync)> = Vec::with_capacity(records.len() * (10 + object_count));

    for record in records {
        out.push(&record.id as &(dyn ToSql + Sync));
        out.push(&record.timestamp as &(dyn ToSql + Sync));
        out.push(&record.region as &(dyn ToSql + Sync));
        out.push(&record.product_category as &(dyn ToSql + Sync));
        out.push(&record.event_type as &(dyn ToSql + Sync));
        out.push(&record.user_id as &(dyn ToSql + Sync));
        out.push(&record.user_segment as &(dyn ToSql + Sync));
        out.push(&record.amount as &(dyn ToSql + Sync));
        out.push(&record.quantity as &(dyn ToSql + Sync));
        out.push(&record.metadata as &(dyn ToSql + Sync));
        for obj in &record.objects {
            out.push(obj as &(dyn ToSql + Sync));
        }
    }

    out
}

fn execute_batch(client: &mut Client, stmt: &Statement, records: &[PgRecord], object_count: usize) -> Result<(u64, f64)> {
    if records.is_empty() {
        return Ok((0, 0.0));
    }

    let params = build_params_refs(records, object_count);
    let start = Instant::now();
    let inserted = client
        .execute(stmt, &params)
        .context("failed to execute batch insert")?;
    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
    Ok((inserted, latency_ms))
}

fn query_single_i64(client: &mut Client, sql: &str) -> Option<i64> {
    client
        .query(sql, &[])
        .ok()
        .and_then(|rows| rows.into_iter().next())
        .and_then(|row| row.try_get::<_, i64>(0).ok())
}

fn query_cluster_info(client: &mut Client) -> JsonValue {
    let mut info = Map::new();

    if let Ok(rows) = client.query("SELECT os_info['available_processors']::bigint FROM sys.nodes", &[]) {
        let cpus: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.try_get::<_, i64>(0).ok())
            .collect();
        info.insert("cpus_per_node".to_string(), json!(cpus));
        info.insert("total_cpus".to_string(), json!(cpus.iter().sum::<i64>()));
        info.insert("nodes".to_string(), json!(cpus.len()));
    }

    if let Ok(rows) = client.query("SELECT mem['used']::bigint FROM sys.nodes", &[]) {
        let mem: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.try_get::<_, i64>(0).ok())
            .collect();
        info.insert("memory_used_bytes".to_string(), json!(mem));
    }

    if let Ok(rows) = client.query("SELECT fs['total']['size']::bigint FROM sys.nodes", &[]) {
        let disk: Vec<i64> = rows
            .iter()
            .filter_map(|r| r.try_get::<_, i64>(0).ok())
            .collect();
        info.insert("disk_total_bytes".to_string(), json!(disk));
    }

    if let Ok(rows) = client.query("SELECT heap['max']::bigint, version['number']::text FROM sys.nodes LIMIT 1", &[]) {
        if let Some(row) = rows.first() {
            if let Ok(heap) = row.try_get::<_, i64>(0) {
                info.insert("heap_max_bytes".to_string(), json!(heap));
            }
            if let Ok(ver) = row.try_get::<_, String>(1) {
                info.insert("version".to_string(), json!(ver));
            }
        }
    }

    JsonValue::Object(info)
}

fn build_report(
    cluster_info: JsonValue,
    table_name: &str,
    cli: &Cli,
    table_options: &[(String, String)],
    monitor: &BenchmarkMonitor,
    verified_count: u64,
    rejected_writes: u64,
    final_concurrency: usize,
    avg_row_bytes: u64,
) -> JsonValue {
    let rate_stats = monitor.rate_stats();
    let latency_stats = monitor.latency_stats();
    let final_stats = monitor.final_stats();
    let total_bytes_sent = monitor.total_bytes_sent.load(Ordering::Relaxed);
    let total_records = final_stats.get("total_records").and_then(|v| v.as_u64()).unwrap_or(0);
    let runtime_seconds = final_stats.get("runtime_seconds").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let total_cpus = cluster_info
        .get("total_cpus")
        .and_then(|v| v.as_i64())
        .unwrap_or(1)
        .max(1) as f64;
    let per_cpu = PercentileStats {
        avg: (rate_stats.avg / total_cpus * 10.0).round() / 10.0,
        min: (rate_stats.min / total_cpus * 10.0).round() / 10.0,
        max: (rate_stats.max / total_cpus * 10.0).round() / 10.0,
        p90: (rate_stats.p90 / total_cpus * 10.0).round() / 10.0,
        p95: (rate_stats.p95 / total_cpus * 10.0).round() / 10.0,
    };

    let mut report = json!({
        "timestamp": Utc::now().to_rfc3339(),
        "client": "rust-pg",
        "cluster": cluster_info,
        "config": {
            "threads": cli.threads,
            "initial_batch_size": cli.batch_size,
            "initial_batch_interval_ms": cli.batch_interval,
            "actual_final_batch_size": cli.batch_size,
            "actual_final_batch_interval_ms": cli.batch_interval,
            "num_objects_generated": cli.objects,
            "duration_minutes": cli.duration,
            "table_name": table_name,
            "shards": cli.shards,
            "replicas": cli.replicas,
            "queue_throttle": cli.queue_throttle,
            "queue_capacity": cli.queue_capacity,
            "queue_throttle_pct": cli.queue_throttle_pct,
            "cpu_throttle": cli.cpu_throttle,
            "max_cpu_load_pct": cli.max_cpu_load_pct,
        },
        "results": {
            "total_records": final_stats.get("total_records").cloned().unwrap_or(json!(0)),
            "total_batches": final_stats.get("total_batches").cloned().unwrap_or(json!(0)),
            "runtime_seconds": (final_stats.get("runtime_seconds").and_then(|v| v.as_f64()).unwrap_or(0.0) * 10.0).round() / 10.0,
            "errors": final_stats.get("total_errors").cloned().unwrap_or(json!(0)),
            "records_per_second": rate_stats,
            "records_per_cpu_second": per_cpu,
            "request_latency_ms": latency_stats,
            "bytes_sent": total_bytes_sent,
            "bandwidth_mbps": if runtime_seconds > 0.0 { (total_bytes_sent as f64 * 8.0 / 1_000_000.0 / runtime_seconds * 100.0).round() / 100.0 } else { 0.0 },
            "verified_count": verified_count,
            "rejected_writes": rejected_writes,
            "rejected_pct": if final_stats.get("total_records").and_then(|v| v.as_u64()).unwrap_or(0) > 0 {
                (rejected_writes as f64 / final_stats.get("total_records").and_then(|v| v.as_u64()).unwrap_or(1) as f64 * 100.0 * 100.0).round() / 100.0
            } else { 0.0 },
            "average_batch_size": monitor.average_batch_size(),
            "error_rate_pct": (monitor.error_rate_pct() * 10.0).round() / 10.0,
            "final_concurrency": final_concurrency,
            "avg_row_bytes_on_disk": avg_row_bytes,
            "avg_payload_bytes": if total_records > 0 { total_bytes_sent / total_records } else { 0 },
        }
    });
    if !table_options.is_empty() {
        report["config"]["table_options"] = table_options_to_json(table_options);
    }
    report
}

fn init_tracing(level: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(level))
        .try_init();
}

fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    init_tracing(&cli.log_level);

    if cli.threads == 0 {
        anyhow::bail!("threads must be greater than 0");
    }
    if cli.batch_size == 0 {
        anyhow::bail!("batch_size must be greater than 0");
    }

    let deadline = Instant::now() + Duration::from_secs(cli.duration.saturating_mul(60));
    let interval = Duration::from_millis(cli.batch_interval);
    let monitor = Arc::new(BenchmarkMonitor::new());

    info!(
        table = %cli.table_name,
        threads = cli.threads,
        batch_size = cli.batch_size,
        objects = cli.objects,
        benchmark = cli.benchmark,
        "starting CrateDB PostgreSQL inserter"
    );

    let table_options: Vec<(String, String)> = match cli.table_options.as_deref() {
        Some(raw) => parse_table_options(raw).context("invalid --table-options")?,
        None => Vec::new(),
    };

    let mut admin_client = connect(&cli.connection_string)?;

    if !cli.no_create_table {
        let sql = create_table_sql(&cli.table_name, cli.shards, cli.replicas, cli.objects, &table_options);
        admin_client
            .batch_execute(&sql)
            .with_context(|| format!("failed to create table {}", cli.table_name))?;
        info!("table ready");
    }

    let cluster_info = query_cluster_info(&mut admin_client);
    let pre_count = query_single_i64(&mut admin_client, &format!("SELECT COUNT(*) FROM {}", quote_ident(&cli.table_name))).unwrap_or(0).max(0) as u64;
    let http_url = pg_to_http_url(&cli.connection_string);
    let tp_baseline = query_thread_pool_counters(&http_url);

    // Concurrency limiter
    let limiter = Arc::new(ConcurrencyLimiter::new(cli.threads));
    let queue_throttle_enabled = cli.queue_throttle;
    let cpu_throttle_enabled = cli.cpu_throttle;
    let either_throttle = queue_throttle_enabled || cpu_throttle_enabled;
    let queue_capacity = cli.queue_capacity as f64;
    let threshold_pct = cli.queue_throttle_pct as f64 / 100.0;
    let max_cpu_pct = cli.max_cpu_load_pct as f64 / 100.0;
    let max_concurrency = cli.threads;
    let min_concurrency = (max_concurrency as f64 * 0.25).ceil() as usize;

    let mut handles = Vec::with_capacity(cli.threads);
    let stop = Arc::new(AtomicBool::new(false));
    let monitor_for_reporter = Arc::clone(&monitor);
    let stop_for_reporter = Arc::clone(&stop);
    let http_url_for_reporter = http_url.clone();
    let limiter_for_reporter = Arc::clone(&limiter);

    let reporter = if cli.benchmark {
        Some(thread::spawn(move || {
            let mut tp_samples: std::collections::BTreeMap<String, Vec<NodeSample>> =
                std::collections::BTreeMap::new();

            while !stop_for_reporter.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));
                if stop_for_reporter.load(Ordering::Relaxed) {
                    break;
                }
                // Sample rate every 5 ticks (5s), poll thread pool every tick (1s)
                static TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                let tick = TICK.fetch_add(1, Ordering::Relaxed);
                if tick % 5 == 4 {
                    monitor_for_reporter.sample_rate();
                }

                let polled = poll_thread_pool_samples(&http_url_for_reporter);
                let mut total_queue: u64 = 0;
                let mut total_cpu: f64 = 0.0;
                let mut node_count: u64 = 0;
                for (name, sample) in &polled {
                    total_queue += sample.queue;
                    total_cpu += sample.cpu;
                    node_count += 1;
                    tp_samples.entry(name.clone()).or_default().push(sample.clone());
                }

                if either_throttle && node_count > 0 {
                    let current = limiter_for_reporter.current_max();

                    // Queue-based desired concurrency
                    let queue_desired = if queue_throttle_enabled {
                        let avg_queue = total_queue as f64 / node_count as f64;
                        let queue_pct = avg_queue / queue_capacity;

                        if queue_pct > threshold_pct {
                            let pressure = (queue_pct - threshold_pct) / (1.0 - threshold_pct);
                            let target = max_concurrency as f64 - pressure * (max_concurrency - min_concurrency) as f64;
                            (target as usize).max(min_concurrency)
                        } else if queue_pct < threshold_pct * 0.5 {
                            (current + 1).min(max_concurrency)
                        } else {
                            current
                        }
                    } else {
                        max_concurrency
                    };

                    // CPU-based desired concurrency
                    let cpu_desired = if cpu_throttle_enabled {
                        let avg_cpu = total_cpu / node_count as f64 / 100.0;

                        if avg_cpu > max_cpu_pct {
                            let overshoot = (avg_cpu - max_cpu_pct) / (1.0 - max_cpu_pct);
                            let target = max_concurrency as f64 - overshoot * (max_concurrency - min_concurrency) as f64;
                            (target as usize).max(min_concurrency)
                        } else if avg_cpu < max_cpu_pct * 0.5 {
                            (current + 1).min(max_concurrency)
                        } else {
                            current
                        }
                    } else {
                        max_concurrency
                    };

                    limiter_for_reporter.set_max(queue_desired.min(cpu_desired));
                }
            }

            tp_samples
        }))
    } else {
        None
    };

    let start = Instant::now();
    for worker_id in 0..cli.threads {
        let conn_str = cli.connection_string.clone();
        let table = cli.table_name.clone();
        let objects = cli.objects;
        let batch_size = cli.batch_size;
        let mon = Arc::clone(&monitor);
        let stp = Arc::clone(&stop);
        let lim = Arc::clone(&limiter);
        let thr = either_throttle;

        handles.push(thread::spawn(move || worker(
            worker_id, conn_str, table, objects, batch_size, interval, deadline, mon, stp, lim, thr,
        )));
    }

    for handle in handles {
        if let Err(err) = handle.join() {
            warn!("worker thread panicked: {:?}", err);
            monitor.add_error();
        }
    }

    stop.store(true, Ordering::Relaxed);
    monitor.sample_rate();
    let tp_samples = if let Some(reporter) = reporter {
        reporter.join().ok()
    } else {
        None
    };

    let elapsed = start.elapsed().as_secs_f64();
    let rows = monitor.total_rows.load(Ordering::Relaxed);
    let err_count = monitor.total_errors.load(Ordering::Relaxed);
    let rps = if elapsed > 0.0 { rows as f64 / elapsed } else { 0.0 };

    // Finalize thread pool stats
    let tp_final = query_thread_pool_counters(&http_url);
    let thread_pool_stats = tp_samples
        .as_ref()
        .and_then(|samples| finalize_thread_pool_stats(&tp_baseline, &tp_final, samples));
    let rejected_writes = thread_pool_stats
        .as_ref()
        .map(|tp| tp.total_rejected)
        .unwrap_or(0);

    let _ = admin_client.batch_execute(&format!("REFRESH TABLE {}", quote_ident(&cli.table_name)));
    let verified_count = query_single_i64(&mut admin_client, &format!("SELECT COUNT(*) FROM {}", quote_ident(&cli.table_name))).unwrap_or(0).max(0) as u64;

    // Query average row size on disk
    let avg_row_bytes = query_single_i64(
        &mut admin_client,
        &format!(
            "SELECT CASE WHEN SUM(num_docs) > 0 THEN (SUM(size) / SUM(num_docs))::bigint ELSE 0 END FROM sys.shards WHERE table_name = '{}' AND primary = true",
            cli.table_name
        ),
    ).unwrap_or(0).max(0) as u64;

    if cli.benchmark {
        let mut cluster_with_tp = cluster_info.clone();
        if let Some(ref tp) = thread_pool_stats {
            cluster_with_tp["write_thread_pool"] = json!(tp);
        }

        let report = build_report(
            cluster_with_tp,
            &cli.table_name,
            &cli,
            &table_options,
            &monitor,
            verified_count.saturating_sub(pre_count),
            rejected_writes,
            limiter.current_max(),
            avg_row_bytes,
        );
        println!("{}", serde_json::to_string(&report).unwrap());

        let version = report
            .get("cluster")
            .and_then(|c| c.get("version"))
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        let total_cpus = report
            .get("cluster")
            .and_then(|c| c.get("total_cpus"))
            .and_then(|v| v.as_i64())
            .unwrap_or(1)
            .max(1);
        let rate_stats = monitor.rate_stats();
        let per_cpu = report
            .get("results")
            .and_then(|r| r.get("records_per_cpu_second"))
            .cloned()
            .unwrap_or(json!({"avg":0.0,"min":0.0,"max":0.0,"p90":0.0,"p95":0.0}));
        let avg = per_cpu.get("avg").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let p95 = per_cpu.get("p95").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let max = per_cpu.get("max").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let effective_rec_per_cpu = (rows as f64 / elapsed / total_cpus as f64).round();
        let rej_str = if rejected_writes > 0 {
            let rej_pct = if rows > 0 { (rejected_writes as f64 / rows as f64) * 100.0 } else { 0.0 };
            format!(" | REJECTED: {} ({:.1}%)", rejected_writes, rej_pct)
        } else {
            String::new()
        };
        eprintln!(
            "CrateDB {} | {} CPUs | p90={:.0} rec/s | per CPU: avg={:.0} p95={:.0} max={:.0} | effective rec/cpu/s={:.0} | avg_row={}B{}",
            version, total_cpus, rate_stats.p90, avg, p95, max, effective_rec_per_cpu, avg_row_bytes, rej_str
        );
    } else {
        info!("inserted {rows} rows in {elapsed:.2}s ({rps:.0} rows/sec), errors={err_count}");
    }

    // Clean up: drop the benchmark table
    let drop_sql = format!("DROP TABLE IF EXISTS {}", quote_ident(&cli.table_name));
    match admin_client.batch_execute(&drop_sql) {
        Ok(_) => info!("Cleaned up table '{}'", cli.table_name),
        Err(e) => warn!("Failed to drop table: {}", e),
    }

    Ok(())
}

fn worker(
    worker_id: usize,
    connection_string: String,
    table_name: String,
    object_count: usize,
    batch_size: usize,
    interval: Duration,
    deadline: Instant,
    monitor: Arc<BenchmarkMonitor>,
    stop: Arc<AtomicBool>,
    limiter: Arc<ConcurrencyLimiter>,
    throttle_enabled: bool,
) {
    let mut client = match connect(&connection_string) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("worker {worker_id}: connection failed: {err:#}");
            monitor.add_error();
            stop.store(true, Ordering::Relaxed);
            return;
        }
    };

    let mut generator = RecordGenerator::new(object_count);
    let insert_sql = build_insert_sql(&table_name, batch_size, object_count);
    let stmt = match client.prepare(&insert_sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            eprintln!("worker {worker_id}: prepare failed: {err:#}");
            monitor.add_error();
            stop.store(true, Ordering::Relaxed);
            return;
        }
    };

    while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
        let records: Vec<PgRecord> = generator
            .generate_batch(batch_size)
            .into_iter()
            .map(record_to_pg)
            .collect();

        if throttle_enabled {
            limiter.acquire();
        }

        let batch_bytes: usize = records.iter().map(|r| r.estimated_wire_bytes()).sum();

        match execute_batch(&mut client, &stmt, &records, object_count) {
            Ok((inserted, latency_ms)) => {
                monitor.add_batch(inserted as usize, batch_bytes, latency_ms);
            }
            Err(err) => {
                tracing::error!("Worker {worker_id} error: {err:#}");
                monitor.add_error();
            }
        }

        if throttle_enabled {
            limiter.release();
        }

        if !interval.is_zero() {
            thread::sleep(interval);
        }
    }
}
