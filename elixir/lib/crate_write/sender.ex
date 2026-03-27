defmodule CrateWrite.Sender do
  @moduledoc """
  Sender process: pulls pre-generated batches from the buffer, serializes, and POSTs to CrateDB.
  Runs in a tight loop — no generation overhead, just serialize + send.
  """
  require Logger

  def start_link(opts) do
    pid = spawn_link(fn -> loop(opts) end)
    {:ok, pid}
  end

  defp loop(opts) do
    client = opts[:client]
    insert_sql = opts[:insert_sql]
    batch_interval = opts[:batch_interval]
    sender_id = opts[:sender_id]

    run(client, insert_sql, batch_interval, sender_id)
  end

  defp run(client, insert_sql, batch_interval, sender_id) do
    batch = CrateWrite.BatchBuffer.pull()

    case CrateWrite.Client.execute_bulk(client, insert_sql, batch) do
      {:ok, bytes_sent, latency_ms} ->
        CrateWrite.Monitor.add_records(length(batch))
        CrateWrite.Monitor.add_request_stats(bytes_sent, latency_ms)

      {:error, reason} ->
        Logger.error("Sender #{sender_id} error: #{reason}")
        CrateWrite.Monitor.add_error()
    end

    if batch_interval > 0, do: Process.sleep(batch_interval)

    run(client, insert_sql, batch_interval, sender_id)
  end
end
