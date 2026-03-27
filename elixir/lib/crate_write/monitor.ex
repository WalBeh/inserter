defmodule CrateWrite.Monitor do
  @moduledoc """
  Performance monitor using ETS for lock-free counter updates (hot path)
  and GenServer for rate/latency sample collection (cold path).
  """
  use GenServer

  @table :crate_write_monitor

  # --- Public API (called by workers, lock-free via ETS) ---

  def add_records(count) do
    :ets.update_counter(@table, :total_records, count)
    :ets.update_counter(@table, :total_batches, 1)
  end

  def add_error do
    :ets.update_counter(@table, :total_errors, 1)
  end

  def add_request_stats(bytes_sent, latency_ms) do
    :ets.update_counter(@table, :total_bytes_sent, bytes_sent)
    GenServer.cast(__MODULE__, {:latency_sample, latency_ms})
  end

  # --- Public API (called infrequently, via GenServer) ---

  def get_current_stats, do: GenServer.call(__MODULE__, :get_current_stats)
  def get_final_stats, do: GenServer.call(__MODULE__, :get_final_stats)
  def get_percentile_stats, do: GenServer.call(__MODULE__, :get_percentile_stats)
  def get_latency_stats, do: GenServer.call(__MODULE__, :get_latency_stats)

  def get_bandwidth_mbps do
    bytes = :ets.lookup_element(@table, :total_bytes_sent, 2)
    GenServer.call(__MODULE__, {:get_bandwidth_mbps, bytes})
  end

  def get_total_bytes_sent do
    :ets.lookup_element(@table, :total_bytes_sent, 2)
  end

  def get_counters do
    %{
      total_records: :ets.lookup_element(@table, :total_records, 2),
      total_batches: :ets.lookup_element(@table, :total_batches, 2),
      total_errors: :ets.lookup_element(@table, :total_errors, 2),
      total_bytes_sent: :ets.lookup_element(@table, :total_bytes_sent, 2)
    }
  end

  # --- GenServer implementation ---

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    table = :ets.new(@table, [:named_table, :public, :set])
    :ets.insert(table, {:total_records, 0})
    :ets.insert(table, {:total_batches, 0})
    :ets.insert(table, {:total_errors, 0})
    :ets.insert(table, {:total_bytes_sent, 0})

    now = System.monotonic_time(:millisecond)

    {:ok,
     %{
       start_time: now,
       last_report_time: now,
       last_report_records: 0,
       rate_samples: [],
       latency_samples: []
     }}
  end

  @impl true
  def handle_cast({:latency_sample, latency_ms}, state) do
    {:noreply, %{state | latency_samples: [latency_ms | state.latency_samples]}}
  end

  @impl true
  def handle_call(:get_current_stats, _from, state) do
    now = System.monotonic_time(:millisecond)
    total_records = :ets.lookup_element(@table, :total_records, 2)
    total_batches = :ets.lookup_element(@table, :total_batches, 2)
    total_errors = :ets.lookup_element(@table, :total_errors, 2)

    elapsed_ms = now - state.start_time
    period_ms = now - state.last_report_time

    records_since_last = total_records - state.last_report_records

    current_rate =
      if period_ms > 0, do: records_since_last / (period_ms / 1000.0), else: 0.0

    average_rate =
      if elapsed_ms > 0, do: total_records / (elapsed_ms / 1000.0), else: 0.0

    stats = %{
      total_records: total_records,
      total_batches: total_batches,
      total_errors: total_errors,
      current_rate: current_rate,
      average_rate: average_rate,
      runtime_seconds: elapsed_ms / 1000.0
    }

    new_state = %{
      state
      | last_report_time: now,
        last_report_records: total_records,
        rate_samples: [current_rate | state.rate_samples]
    }

    {:reply, stats, new_state}
  end

  def handle_call(:get_final_stats, _from, state) do
    now = System.monotonic_time(:millisecond)
    total_records = :ets.lookup_element(@table, :total_records, 2)
    total_batches = :ets.lookup_element(@table, :total_batches, 2)
    total_errors = :ets.lookup_element(@table, :total_errors, 2)

    elapsed_ms = now - state.start_time

    average_rate =
      if elapsed_ms > 0, do: total_records / (elapsed_ms / 1000.0), else: 0.0

    stats = %{
      total_records: total_records,
      total_batches: total_batches,
      total_errors: total_errors,
      average_rate: average_rate,
      runtime_seconds: elapsed_ms / 1000.0
    }

    {:reply, stats, state}
  end

  def handle_call(:get_percentile_stats, _from, state) do
    {:reply, compute_percentiles(state.rate_samples), state}
  end

  def handle_call(:get_latency_stats, _from, state) do
    {:reply, compute_percentiles(state.latency_samples), state}
  end

  def handle_call({:get_bandwidth_mbps, bytes}, _from, state) do
    now = System.monotonic_time(:millisecond)
    elapsed_s = (now - state.start_time) / 1000.0
    mbps = if elapsed_s > 0, do: bytes * 8 / 1_000_000 / elapsed_s, else: 0.0
    {:reply, Float.round(mbps, 2), state}
  end

  defp compute_percentiles([]), do: %{avg: 0, min: 0, max: 0, p90: 0, p95: 0}

  defp compute_percentiles(samples) do
    sorted = Enum.sort(samples)
    n = length(sorted)
    sum = Enum.sum(sorted)

    %{
      avg: Float.round(sum / n, 1),
      min: Float.round(hd(sorted), 1),
      max: Float.round(List.last(sorted), 1),
      p90: Float.round(Enum.at(sorted, trunc(n * 0.9)), 1),
      p95: Float.round(Enum.at(sorted, trunc(n * 0.95)), 1)
    }
  end
end
