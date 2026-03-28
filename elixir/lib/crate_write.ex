defmodule CrateWrite do
  @moduledoc "CrateDB record generator and inserter — Elixir pipeline implementation."
  require Logger

  alias CrateWrite.{Config, Client, Cluster, Generator, Monitor, Benchmark}

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

    # Calculate pipeline sizing: N generators feed M senders through a buffer
    # Use ~1/4 of threads for generation, ~3/4 for sending (I/O bound)
    num_generators = max(div(config.threads, 4), 1)
    num_senders = max(config.threads - num_generators, 1)

    unless config.benchmark do
      IO.puts(:stderr, "Starting CrateDB record generator (Elixir/BEAM pipeline)")
      IO.puts(:stderr, "Connection: #{Config.sanitize_connection_string(config.connection_string)}")
      IO.puts(:stderr, "Table: #{config.table_name}")
      IO.puts(:stderr, "Duration: #{config.duration} minutes")
      IO.puts(:stderr, "Batch size: #{config.batch_size}")
      IO.puts(:stderr, "Batch interval: #{config.batch_interval}ms")
      IO.puts(:stderr, "Pipeline: #{num_generators} generators → buffer → #{num_senders} senders")
      IO.puts(:stderr, "Compression: #{if config.compress, do: "gzip", else: "off"}")
    end

    # Start Finch with pool sized to sender count
    pool_size = max(num_senders + 2, 10)
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

    # Initialize dynamic batch size (for auto-tune)
    CrateWrite.GeneratorWorker.init_batch_size(config.batch_size)

    # Start generator processes (CPU work — feed the buffer)
    generator_pids =
      for i <- 0..(num_generators - 1) do
        {:ok, pid} = CrateWrite.GeneratorWorker.start_link(
          generator_id: i,
          generator: generator,
          batch_size: config.batch_size
        )
        pid
      end

    if config.auto_tune do
      # Auto-tune mode: PID controller manages senders dynamically
      latency_target_ms = round(config.latency_target * 1000)

      unless config.benchmark do
        IO.puts(:stderr, "AUTO-TUNE: starting with 6 senders, target P95 latency #{config.latency_target}s")
        IO.puts(:stderr, "AUTO-TUNE: max senders=#{num_senders}, max batch=#{config.batch_size}")
      end

      {:ok, _pid} = CrateWrite.PIDController.start_link(
        client: client,
        insert_sql: insert_sql,
        generator: generator,
        max_senders: num_senders,
        max_batch_size: config.batch_size,
        latency_target_ms: latency_target_ms,
        batch_interval: config.batch_interval,
        mode: config.auto_tune_mode
      )
    else
      # Fixed mode: start all senders immediately
      for i <- 0..(num_senders - 1) do
        CrateWrite.Sender.start_link(
          sender_id: i,
          client: client,
          insert_sql: insert_sql,
          batch_interval: config.batch_interval
        )
      end

      unless config.benchmark do
        IO.puts(:stderr, "Pipeline running: #{num_generators} generators, #{num_senders} senders")
      end
    end

    # Start reporting/sampling timer (auto-tune logs its own output)
    quiet = config.benchmark
    total_workers = num_generators + num_senders
    reporter_pid = spawn(fn -> reporter_loop(total_workers, quiet) end)

    # Wait for duration or Ctrl+C
    duration_ms = config.duration * 60_000
    main_pid = self()

    {:ok, _} = System.trap_signal(:sigint, fn ->
      send(main_pid, :interrupted)
    end)

    receive do
      :interrupted ->
        unless config.benchmark, do: IO.puts(:stderr, "Interrupted, stopping pipeline...")
    after
      duration_ms ->
        unless config.benchmark, do: IO.puts(:stderr, "Duration completed, stopping pipeline...")
    end

    System.untrap_signal(:sigint)

    # Stop PID controller if running
    if config.auto_tune do
      pid_state = try do
        CrateWrite.PIDController.get_state()
      catch
        _, _ -> nil
      end
      Process.put(:auto_tune_state, pid_state)

      try do
        case Process.whereis(CrateWrite.PIDController) do
          nil -> :ok
          pid -> GenServer.stop(pid, :normal, 5000)
        end
      catch
        _, _ -> :ok
      end
    end

    # Stop generators
    for pid <- generator_pids, Process.alive?(pid), do: Process.exit(pid, :kill)
    Process.sleep(500)

    # Stop reporter
    if reporter_pid && Process.alive?(reporter_pid), do: Process.exit(reporter_pid, :kill)
    Process.sleep(1000)

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

      auto_tune_data = Process.get(:auto_tune_state)

      monitor_data = %{
        rate_stats: rate_stats,
        latency_stats: latency_stats,
        bytes_sent: bytes_sent,
        bandwidth_mbps: bandwidth_mbps,
        auto_tune: auto_tune_data
      }

      Benchmark.output_json(config, cluster_info, final_stats, monitor_data, verification)
      Benchmark.output_stderr_summary(config, cluster_info, rate_stats, verification)
    else
      Benchmark.output_normal_summary(config, final_stats, verification)
    end
  end

  defp reporter_loop(num_workers, quiet) do
    Process.sleep(10_000)
    stats = Monitor.get_current_stats()

    unless quiet do
      IO.puts(:stderr,
        "Performance: #{Float.round(stats.current_rate, 1)} records/sec (current), " <>
          "#{Float.round(stats.average_rate, 1)} records/sec (avg), " <>
          "Total: #{stats.total_records} records, " <>
          "Batches: #{stats.total_batches}, " <>
          "Workers: #{num_workers}, " <>
          "Errors: #{stats.total_errors}"
      )
    end

    reporter_loop(num_workers, quiet)
  end
end
