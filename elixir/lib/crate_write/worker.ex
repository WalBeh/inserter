defmodule CrateWrite.Worker do
  @moduledoc """
  GenServer worker: generate batch → serialize → send → repeat.
  The generate_batch call is the seam point for future GenStage split.
  """
  use GenServer
  require Logger

  defstruct [:worker_id, :client, :generator, :insert_sql, :batch_size, :batch_interval]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      worker_id: opts[:worker_id],
      client: opts[:client],
      generator: opts[:generator],
      insert_sql: opts[:insert_sql],
      batch_size: opts[:batch_size],
      batch_interval: opts[:batch_interval]
    }

    # Start the loop
    send(self(), :run_batch)
    {:ok, state}
  end

  @impl true
  def handle_info(:run_batch, state) do
    # Generate batch (CPU work — runs in this BEAM process, preemptively scheduled)
    batch = CrateWrite.Generator.generate_batch(state.generator, state.batch_size)

    # Send to CrateDB
    case CrateWrite.Client.execute_bulk(state.client, state.insert_sql, batch) do
      {:ok, bytes_sent, latency_ms} ->
        CrateWrite.Monitor.add_records(state.batch_size)
        CrateWrite.Monitor.add_request_stats(bytes_sent, latency_ms)

      {:error, reason} ->
        Logger.error("Worker #{state.worker_id} error: #{reason}")
        CrateWrite.Monitor.add_error()
    end

    # Schedule next batch
    if state.batch_interval > 0 do
      Process.send_after(self(), :run_batch, state.batch_interval)
    else
      send(self(), :run_batch)
    end

    {:noreply, state}
  end

  def handle_info(:shutdown, state) do
    {:stop, :normal, state}
  end
end
