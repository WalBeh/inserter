use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use postgres::types::ToSql;
use postgres::{Client, NoTls, Statement};
use postgres_native_tls::MakeTlsConnector;
use serde_json::{json, Map, Value as JsonValue};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
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
    metadata: String,
    objects: Vec<String>,
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
            rate_samples: Mutex::new(Vec::new()),
            latency_samples: Mutex::new(Vec::new()),
        }
    }

    fn add_batch(&self, rows: usize, latency_ms: f64) {
        self.total_rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.total_batches.fetch_add(1, Ordering::Relaxed);
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
        let rate = if elapsed > 0.0 { delta as f64 / elapsed } else { 0.0 };
        self.rate_samples
            .lock()
            .expect("rate_samples lock poisoned")
            .push(rate);
        *last_rows = total_rows;
        *last_time = now;
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
        metadata: record.metadata,
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
        "metadata TEXT".to_string(),
    ];

    for i in 0..object_count {
        cols.push(format!("obj_{i} TEXT"));
    }

    cols
}

fn create_table_sql(table_name: &str, shards: usize, replicas: usize, object_count: usize) -> String {
    let columns = build_columns(object_count).join(", ");
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) CLUSTERED INTO {} SHARDS WITH (number_of_replicas = {})",
        quote_ident(table_name),
        columns,
        shards,
        replicas
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
    monitor: &BenchmarkMonitor,
    verified_count: u64,
    rejected_writes: u64,
) -> JsonValue {
    let rate_stats = monitor.rate_stats();
    let latency_stats = monitor.latency_stats();
    let final_stats = monitor.final_stats();
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

    json!({
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
        },
        "results": {
            "total_records": final_stats.get("total_records").cloned().unwrap_or(json!(0)),
            "total_batches": final_stats.get("total_batches").cloned().unwrap_or(json!(0)),
            "runtime_seconds": (final_stats.get("runtime_seconds").and_then(|v| v.as_f64()).unwrap_or(0.0) * 10.0).round() / 10.0,
            "errors": final_stats.get("total_errors").cloned().unwrap_or(json!(0)),
            "records_per_second": rate_stats,
            "records_per_cpu_second": per_cpu,
            "request_latency_ms": latency_stats,
            "bytes_sent": 0,
            "bandwidth_mbps": 0.0,
            "verified_count": verified_count,
            "rejected_writes": rejected_writes,
            "rejected_pct": if final_stats.get("total_records").and_then(|v| v.as_u64()).unwrap_or(0) > 0 {
                (rejected_writes as f64 / final_stats.get("total_records").and_then(|v| v.as_u64()).unwrap_or(1) as f64 * 100.0 * 100.0).round() / 100.0
            } else { 0.0 },
            "average_batch_size": monitor.average_batch_size(),
            "error_rate_pct": (monitor.error_rate_pct() * 10.0).round() / 10.0,
        }
    })
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

    let mut admin_client = connect(&cli.connection_string)?;

    if !cli.no_create_table {
        let sql = create_table_sql(&cli.table_name, cli.shards, cli.replicas, cli.objects);
        admin_client
            .batch_execute(&sql)
            .with_context(|| format!("failed to create table {}", cli.table_name))?;
        info!("table ready");
    }

    let cluster_info = query_cluster_info(&mut admin_client);
    let pre_count = query_single_i64(&mut admin_client, &format!("SELECT COUNT(*) FROM {}", quote_ident(&cli.table_name))).unwrap_or(0).max(0) as u64;
    let pre_rejected = query_single_i64(
        &mut admin_client,
        "SELECT SUM(pool['rejected'])::bigint FROM (SELECT UNNEST(thread_pools) AS pool FROM sys.nodes) x WHERE pool['name'] = 'write'",
    )
    .unwrap_or(0)
    .max(0) as u64;

    let mut handles = Vec::with_capacity(cli.threads);
    let stop = Arc::new(AtomicBool::new(false));
    let monitor_for_reporter = Arc::clone(&monitor);
    let stop_for_reporter = Arc::clone(&stop);

    let reporter = if cli.benchmark {
        Some(thread::spawn(move || {
            while !stop_for_reporter.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(10));
                if stop_for_reporter.load(Ordering::Relaxed) {
                    break;
                }
                monitor_for_reporter.sample_rate();
            }
        }))
    } else {
        None
    };

    let start = Instant::now();
    for worker_id in 0..cli.threads {
        let args = (
            worker_id,
            cli.connection_string.clone(),
            cli.table_name.clone(),
            cli.objects,
            cli.batch_size,
            interval,
            deadline,
            Arc::clone(&monitor),
            Arc::clone(&stop),
        );
        handles.push(thread::spawn(move || worker(
            args.0, args.1, args.2, args.3, args.4, args.5, args.6, args.7, args.8,
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
    if let Some(reporter) = reporter {
        let _ = reporter.join();
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rows = monitor.total_rows.load(Ordering::Relaxed);
    let err_count = monitor.total_errors.load(Ordering::Relaxed);
    let rps = if elapsed > 0.0 { rows as f64 / elapsed } else { 0.0 };

    let _ = admin_client.batch_execute(&format!("REFRESH TABLE {}", quote_ident(&cli.table_name)));
    let verified_count = query_single_i64(&mut admin_client, &format!("SELECT COUNT(*) FROM {}", quote_ident(&cli.table_name))).unwrap_or(0).max(0) as u64;
    let rejected_writes = query_single_i64(
        &mut admin_client,
        "SELECT SUM(pool['rejected'])::bigint FROM (SELECT UNNEST(thread_pools) AS pool FROM sys.nodes) x WHERE pool['name'] = 'write'",
    )
    .unwrap_or(0)
    .max(0) as u64;

    if cli.benchmark {
        let report = build_report(
            cluster_info,
            &cli.table_name,
            &cli,
            &monitor,
            verified_count.saturating_sub(pre_count),
            rejected_writes.saturating_sub(pre_rejected),
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
        let rej_str = if rejected_writes > pre_rejected {
            let rej_pct = if rows > 0 { ((rejected_writes - pre_rejected) as f64 / rows as f64) * 100.0 } else { 0.0 };
            format!(" | REJECTED: {} ({:.1}%)", rejected_writes - pre_rejected, rej_pct)
        } else {
            String::new()
        };
        eprintln!(
            "CrateDB {} | {} CPUs | p90={:.0} rec/s | per CPU: avg={:.0} p95={:.0} max={:.0}{}",
            version, total_cpus, rate_stats.p90, avg, p95, max, rej_str
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

        match execute_batch(&mut client, &stmt, &records, object_count) {
            Ok((inserted, latency_ms)) => {
                monitor.add_batch(inserted as usize, latency_ms);
            }
            Err(err) => {
                eprintln!("worker {worker_id}: insert failed: {err:#}");
                monitor.add_error();
                stop.store(true, Ordering::Relaxed);
                break;
            }
        }

        if !interval.is_zero() {
            thread::sleep(interval);
        }
    }
}
