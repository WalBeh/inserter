defmodule CrateWrite.Benchmark do
  @moduledoc "Benchmark JSON output and stderr summary, matching Rust/Python format exactly."

  def output_json(config, cluster_info, final_stats, monitor_data, verification) do
    total_cpus = Map.get(cluster_info, "total_cpus", 1)

    rate_stats = monitor_data.rate_stats
    per_cpu = percentiles_div(rate_stats, total_cpus)

    result = %{
      "timestamp" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "client" => "elixir-http",
      "cluster" => cluster_info,
      "config" => %{
        "threads" => config.threads,
        "batch_size" => config.batch_size,
        "batch_interval" => config.batch_interval / 1000.0,
        "duration_minutes" => config.duration,
        "table_name" => config.table_name,
        "shards" => config.shards,
        "replicas" => config.replicas
      },
      "results" => %{
        "total_records" => final_stats.total_records,
        "total_batches" => final_stats.total_batches,
        "runtime_seconds" => Float.round(final_stats.runtime_seconds, 1),
        "errors" => final_stats.total_errors,
        "records_per_second" => rate_stats,
        "records_per_cpu_second" => per_cpu,
        "request_latency_ms" => monitor_data.latency_stats,
        "bytes_sent" => monitor_data.bytes_sent,
        "bandwidth_mbps" => monitor_data.bandwidth_mbps,
        "verified_count" => verification.verified_count,
        "rejected_writes" => verification.rejected_writes,
        "rejected_pct" => Float.round(verification.rejected_writes / max(final_stats.total_records, 1) * 100.0, 2)
      }
    }

    # JSONL to stdout (one line)
    IO.puts(Jason.encode!(result))
  end

  def output_stderr_summary(config, cluster_info, rate_stats, verification) do
    total_cpus = Map.get(cluster_info, "total_cpus", 1)
    version = Map.get(cluster_info, "version", "?")
    per_cpu = percentiles_div(rate_stats, total_cpus)
    p90_total = Map.get(rate_stats, :p90, 0)

    rej = verification.rejected_writes

    rej_str =
      if rej > 0 do
        rej_pct = Float.round(rej / max(verification.total_records, 1) * 100.0, 1)
        " | REJECTED: #{rej} (#{rej_pct}%)"
      else
        ""
      end

    IO.write(:stderr,
      "CrateDB #{version} | #{total_cpus} CPUs | p90=#{trunc(p90_total)} rec/s | per CPU: avg=#{trunc(per_cpu.avg)} p95=#{trunc(per_cpu.p95)} max=#{trunc(per_cpu.max)}#{rej_str}\n"
    )
  end

  def output_normal_summary(config, final_stats, verification) do
    IO.puts(:stderr, String.duplicate("=", 60))
    IO.puts(:stderr, "FINAL PERFORMANCE SUMMARY")
    IO.puts(:stderr, String.duplicate("=", 60))
    IO.puts(:stderr, "Worker processes: #{config.threads}")
    IO.puts(:stderr, "Total records sent: #{final_stats.total_records}")
    IO.puts(:stderr, "Total batches: #{final_stats.total_batches}")
    IO.puts(:stderr, "Total runtime: #{Float.round(final_stats.runtime_seconds, 1)} seconds")
    IO.puts(:stderr, "Average insertion rate: #{Float.round(final_stats.average_rate, 1)} records/second")
    IO.puts(:stderr, "Records per worker: #{div(final_stats.total_records, max(config.threads, 1))} avg")
    IO.puts(:stderr, "Total errors: #{final_stats.total_errors}")
    IO.puts(:stderr, String.duplicate("=", 60))
    IO.puts(:stderr, "RECORD VERIFICATION")
    IO.puts(:stderr, "Records sent: #{final_stats.total_records}  |  Verified in CrateDB: #{verification.verified_count}")

    cond do
      verification.verified_count == final_stats.total_records ->
        IO.puts(:stderr, "MATCH")

      verification.verified_count > final_stats.total_records ->
        IO.puts(:stderr, "CrateDB has #{verification.verified_count - final_stats.total_records} extra (pre-existing data)")

      true ->
        missing = final_stats.total_records - verification.verified_count
        pct = Float.round(missing / max(final_stats.total_records, 1) * 100.0, 2)
        IO.puts(:stderr, "MISMATCH - #{missing} missing (#{pct}% loss)")
    end

    if verification.rejected_writes > 0 do
      IO.puts(:stderr, "REJECTED WRITES: #{verification.rejected_writes}")
    end

    IO.puts(:stderr, String.duplicate("=", 60))
  end

  defp percentiles_div(stats, divisor) when divisor > 0 do
    %{
      avg: Float.round(stats.avg / divisor, 1),
      min: Float.round(stats.min / divisor, 1),
      max: Float.round(stats.max / divisor, 1),
      p90: Float.round(stats.p90 / divisor, 1),
      p95: Float.round(stats.p95 / divisor, 1)
    }
  end

  defp percentiles_div(stats, _), do: stats
end
