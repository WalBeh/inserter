use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, warn, error};


mod client;
mod generator;
mod monitor;
mod config;

// Dashboard module disabled for now
// #[cfg(feature = "dashboard")]
// mod dashboard;

use client::CrateClient;
use generator::RecordGenerator;
use monitor::PerformanceMonitor;
use config::Config;

#[derive(Parser)]
#[command(
    name = "crate-write",
    about = "High-performance CrateDB record generator and inserter",
    long_about = "A Rust implementation of the CrateDB record generator with maximum performance and concurrency"
)]
struct Cli {
    /// Name of the CrateDB table to insert records into
    #[arg(long, required_unless_present = "test_loadbalancer")]
    table_name: Option<String>,

    /// CrateDB connection string (can be read from .env file)
    #[arg(long, env = "CRATE_CONNECTION_STRING")]
    connection_string: Option<String>,

    /// Duration to run the generator (in minutes)
    #[arg(long, required_unless_present = "test_loadbalancer")]
    duration: Option<u64>,

    /// Number of records to insert in each batch
    #[arg(long, default_value = "100")]
    batch_size: usize,

    /// Interval between batches in milliseconds
    #[arg(long, default_value = "100")]
    batch_interval: u64,

    /// Number of parallel worker tasks
    #[arg(long, default_value = "1")]
    threads: usize,

    /// Number of additional low-cardinality object columns to create
    #[arg(long, default_value = "0")]
    objects: usize,

    /// Enable real-time dashboard (not implemented yet)
    #[arg(long)]
    dashboard: bool,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Configuration file path
    #[arg(long)]
    config: Option<PathBuf>,
}

fn sanitize_connection_string(connection_string: &str) -> String {
    match url::Url::parse(connection_string) {
        Ok(url) => {
            format!("{}://{}:{}",
                url.scheme(),
                url.host_str().unwrap_or("unknown"),
                url.port().unwrap_or(4200)
            )
        }
        Err(_) => "invalid-connection-string".to_string(),
    }
}



async fn create_table(client: &CrateClient, table_name: &str, objects: usize) -> Result<()> {
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
        "CREATE TABLE IF NOT EXISTS {} ({}) WITH (number_of_replicas = 1)",
        table_name,
        columns.join(", ")
    );

    info!("Creating table: {}", table_name);
    client.execute(&sql, &[]).await
        .with_context(|| format!("Failed to create table {}", table_name))?;

    info!("✅ Table '{}' created successfully", table_name);
    Ok(())
}

async fn run_data_generation(
    client: CrateClient,
    config: Config,
    monitor: PerformanceMonitor,
) -> Result<()> {
    let table_name = config.table_name.as_ref().unwrap();

    // Create table
    create_table(&client, table_name, config.objects).await?;

    // Prepare insert statement
    let mut placeholders = vec!["?"; 10]; // Base fields
    placeholders.extend(vec!["?"; config.objects]); // Object fields

    let mut field_names = vec![
        "id", "timestamp", "region", "product_category", "event_type",
        "user_id", "user_segment", "amount", "quantity", "metadata"
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
        let generator = RecordGenerator::new(config.objects);
        let insert_sql = insert_sql.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let batch_size = config.batch_size;
        let batch_interval = Duration::from_millis(config.batch_interval);

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Worker {} shutting down", worker_id);
                        break;
                    }
                    _ = async {
                        // Generate batch
                        let batch = generator.generate_batch(batch_size).await;

                        // Convert to parameters
                        let params: Vec<Vec<serde_json::Value>> = batch.into_iter()
                            .map(|record| record.to_params())
                            .collect();

                        // Insert batch
                        match client.execute_bulk(&insert_sql, &params).await {
                            Ok(_) => {
                                monitor.add_records(batch_size).await;
                            }
                            Err(e) => {
                                error!("Worker {} error: {}", worker_id, e);
                                monitor.add_error().await;
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

    // Spawn reporting task
    let monitor_clone = monitor.clone();
    let shutdown_tx_clone = shutdown_tx.clone();
    let reporting_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let mut shutdown_rx = shutdown_tx_clone.subscribe();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                _ = interval.tick() => {
                    let stats = monitor_clone.get_current_stats().await;
                    info!(
                        "Performance: {:.1} records/sec (current), {:.1} records/sec (avg), Total: {} records, Batches: {}, Threads: {}, Errors: {}",
                        stats.current_rate,
                        stats.average_rate,
                        stats.total_records,
                        stats.total_batches,
                        config.threads,
                        stats.total_errors
                    );
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

    // Final statistics
    let final_stats = monitor.get_final_stats().await;
    info!("{}", "=".repeat(60));
    info!("FINAL PERFORMANCE SUMMARY");
    info!("{}", "=".repeat(60));
    info!("✅ Worker threads: {}", config.threads);
    info!("✅ Total records inserted: {}", final_stats.total_records);
    info!("✅ Total batches: {}", final_stats.total_batches);
    info!("✅ Total runtime: {:.1} seconds", final_stats.runtime_seconds);
    info!("✅ Average insertion rate: {:.1} records/second", final_stats.average_rate);
    info!("✅ Records per thread: {:.0} avg", final_stats.total_records as f64 / config.threads as f64);
    info!("✅ Total errors: {}", final_stats.total_errors);
    info!("{}", "=".repeat(60));

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file FIRST
    let dotenv_result = dotenvy::dotenv();

    // Parse CLI arguments AFTER environment variables are loaded
    let cli = Cli::parse();

    // Initialize logging
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cli.log_level));

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
            info!("✅ CRATE_CONNECTION_STRING environment variable is set (length: {})", value.len());
        }
        Err(_) => {
            warn!("❌ CRATE_CONNECTION_STRING environment variable is NOT set");
        }
    }

    // Load configuration
    let mut config = if let Some(config_path) = cli.config {
        Config::from_file(&config_path)?
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
    config.batch_size = cli.batch_size;
    config.batch_interval = cli.batch_interval;
    config.threads = cli.threads;
    config.objects = cli.objects;

    // Validate configuration
    config.validate()?;

    info!("🚀 CrateDB Record Generator (Rust)");
    info!("Connection: {}", sanitize_connection_string(config.connection_string.as_ref().unwrap()));

    // Create client
    let client = CrateClient::new(config.connection_string.as_ref().unwrap()).await?;

    // Create performance monitor
    let monitor = PerformanceMonitor::new();

    // Run data generation
    run_data_generation(client, config, monitor).await?;

    Ok(())
}
