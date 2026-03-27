defmodule CrateWrite do
  @moduledoc "CrateDB record generator and inserter — Elixir implementation."
  require Logger

  alias CrateWrite.{Config, Client, Cluster, Generator, Monitor, Worker, Benchmark}

  def main(args \\ nil) do
    args = args || System.argv()
    config = Config.from_cli(args)

    # Set log level
    if config.benchmark do
      Logger.configure(level: :warning)
    else
      level = String.to_atom(config.log_level)
      Logger.configure(level: level)
    end

    # Handle load balancer test mode
    if config.test_loadbalancer do
      IO.puts(:stderr, "Load balancer test not yet implemented in Elixir")
      System.halt(1)
    end

    # Validate
    config = Config.validate!(config)

    unless config.benchmark do
      IO.puts(:stderr, "Starting CrateDB record generator (Elixir/BEAM)")
      IO.puts(:stderr, "Connection: #{Config.sanitize_connection_string(config.connection_string)}")
      IO.puts(:stderr, "Table: #{config.table_name}")
      IO.puts(:stderr, "Duration: #{config.duration} minutes")
      IO.puts(:stderr, "Batch size: #{config.batch_size}")
      IO.puts(:stderr, "Batch interval: #{config.batch_interval}ms")
      IO.puts(:stderr, "Worker processes: #{config.threads}")
      IO.puts(:stderr, "Compression: #{if config.compress, do: "gzip", else: "off"}")
    end

    # Start Finch with pool sized to thread count
    pool_size = max(config.threads + 2, 10)
    CrateWrite.Application.start_finch(pool_size)

    # Create client
    client = Client.new(config.connection_string, compress: config.compress)

    # Create table
    case Cluster.create_table(client, config.table_name, config.objects, config.shards, config.replicas) do
      {:ok, _} ->
        unless config.benchmark, do: IO.puts(:stderr, "Table '#{config.table_name}' created successfully")

      {:error, reason} ->
        IO.puts(:stderr, "Failed to create table: #{reason}")
        System.halt(1)
    end

    # Query cluster info
    cluster_info = Cluster.query_cluster_info(client)

    # Capture pre-run baselines
    pre_count = Cluster.get_record_count(client, config.table_name)
    pre_rejected = Cluster.get_rejected_writes(client)

    # Build insert SQL
    insert_sql = Cluster.build_insert_sql(config.table_name, config.objects)

    # Create generator template
    generator = Generator.new(config.objects)

    # Start workers
    worker_pids =
      for i <- 0..(config.threads - 1) do
        {:ok, pid} =
          DynamicSupervisor.start_child(CrateWrite.WorkerSupervisor, {
            Worker,
            worker_id: i,
            client: client,
            generator: generator,
            insert_sql: insert_sql,
            batch_size: config.batch_size,
            batch_interval: config.batch_interval
          })

        pid
      end

    unless config.benchmark do
      IO.puts(:stderr, "Started #{config.threads} worker processes...")
    end

    # Start reporting timer (unless benchmark mode)
    reporter_pid =
      unless config.benchmark do
        spawn_link(fn -> reporter_loop(config.threads) end)
      end

    # Wait for duration or Ctrl+C
    duration_ms = config.duration * 60_000

    try do
      Process.sleep(duration_ms)
      unless config.benchmark, do: IO.puts(:stderr, "Duration completed, stopping workers...")
    catch
      :exit, _ ->
        unless config.benchmark, do: IO.puts(:stderr, "Interrupted, stopping workers...")
    end

    # Stop workers
    for pid <- worker_pids do
      if Process.alive?(pid), do: send(pid, :shutdown)
    end

    # Wait for in-flight requests to drain
    Process.sleep(2000)

    # Stop reporter
    if reporter_pid && Process.alive?(reporter_pid), do: Process.exit(reporter_pid, :shutdown)

    # Collect final rate sample
    Monitor.get_current_stats()

    # Get final stats
    final_stats = Monitor.get_final_stats()

    # Verification
    post_count = Cluster.get_record_count(client, config.table_name)
    post_rejected = Cluster.get_rejected_writes(client)

    verified_count = max(post_count - pre_count, 0)
    rejected_writes = max(post_rejected - pre_rejected, 0)

    verification = %{
      verified_count: verified_count,
      rejected_writes: rejected_writes,
      total_records: final_stats.total_records
    }

    # Output results
    if config.benchmark do
      rate_stats = Monitor.get_percentile_stats()
      latency_stats = Monitor.get_latency_stats()
      bytes_sent = Monitor.get_total_bytes_sent()
      bandwidth_mbps = Monitor.get_bandwidth_mbps()

      monitor_data = %{
        rate_stats: rate_stats,
        latency_stats: latency_stats,
        bytes_sent: bytes_sent,
        bandwidth_mbps: bandwidth_mbps
      }

      Benchmark.output_json(config, cluster_info, final_stats, monitor_data, verification)
      Benchmark.output_stderr_summary(config, cluster_info, rate_stats, verification)
    else
      Benchmark.output_normal_summary(config, final_stats, verification)
    end
  end

  defp reporter_loop(num_threads) do
    Process.sleep(10_000)
    stats = Monitor.get_current_stats()

    IO.puts(:stderr,
      "Performance: #{Float.round(stats.current_rate, 1)} records/sec (current), " <>
        "#{Float.round(stats.average_rate, 1)} records/sec (avg), " <>
        "Total: #{stats.total_records} records, " <>
        "Batches: #{stats.total_batches}, " <>
        "Workers: #{num_threads}, " <>
        "Errors: #{stats.total_errors}"
    )

    reporter_loop(num_threads)
  end
end
