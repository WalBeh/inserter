use anyhow::{Context, Result};
use clap::Parser;
use postgres::types::ToSql;
use postgres::{Client, NoTls, Statement};
use postgres_native_tls::MakeTlsConnector;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

fn execute_batch(client: &mut Client, stmt: &Statement, records: &[PgRecord], object_count: usize) -> Result<u64> {
    if records.is_empty() {
        return Ok(0);
    }

    let params = build_params_refs(records, object_count);
    let inserted = client
        .execute(stmt, &params)
        .context("failed to execute batch insert")?;
    Ok(inserted)
}

fn worker(
    worker_id: usize,
    connection_string: String,
    table_name: String,
    object_count: usize,
    batch_size: usize,
    interval: Duration,
    deadline: Instant,
    total_rows: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    let mut client = match connect(&connection_string) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("worker {worker_id}: connection failed: {err:#}");
            errors.fetch_add(1, Ordering::Relaxed);
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
            errors.fetch_add(1, Ordering::Relaxed);
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
            Ok(inserted) => {
                total_rows.fetch_add(inserted, Ordering::Relaxed);
            }
            Err(err) => {
                eprintln!("worker {worker_id}: insert failed: {err:#}");
                errors.fetch_add(1, Ordering::Relaxed);
                stop.store(true, Ordering::Relaxed);
                break;
            }
        }

        if !interval.is_zero() {
            thread::sleep(interval);
        }
    }
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

    info!(
        table = %cli.table_name,
        threads = cli.threads,
        batch_size = cli.batch_size,
        objects = cli.objects,
        "starting CrateDB PostgreSQL inserter"
    );

    if !cli.no_create_table {
        let mut client = connect(&cli.connection_string)?;
        let sql = create_table_sql(&cli.table_name, cli.shards, cli.replicas, cli.objects);
        client
            .batch_execute(&sql)
            .with_context(|| format!("failed to create table {}", cli.table_name))?;
        info!("table ready");
    }

    let total_rows = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(cli.threads);

    for worker_id in 0..cli.threads {
        let args = (
            worker_id,
            cli.connection_string.clone(),
            cli.table_name.clone(),
            cli.objects,
            cli.batch_size,
            interval,
            deadline,
            Arc::clone(&total_rows),
            Arc::clone(&errors),
            Arc::clone(&stop),
        );

        handles.push(thread::spawn(move || worker(
            args.0, args.1, args.2, args.3, args.4, args.5, args.6, args.7, args.8, args.9,
        )));
    }

    for handle in handles {
        if let Err(err) = handle.join() {
            warn!("worker thread panicked: {:?}", err);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rows = total_rows.load(Ordering::Relaxed);
    let err_count = errors.load(Ordering::Relaxed);
    let rps = if elapsed > 0.0 { rows as f64 / elapsed } else { 0.0 };

    println!(
        "inserted {rows} rows in {elapsed:.2}s ({rps:.0} rows/sec), errors={err_count}"
    );

    Ok(())
}
