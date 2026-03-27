defmodule CrateWrite.GeneratorWorker do
  @moduledoc """
  Generator process: continuously generates batches and pushes them to the buffer.
  Reads batch_size from ETS so the PID controller can adjust it at runtime.
  """

  @batch_size_table :generator_batch_size

  def init_batch_size(batch_size) do
    if :ets.whereis(@batch_size_table) == :undefined do
      :ets.new(@batch_size_table, [:named_table, :public, :set])
    end
    :ets.insert(@batch_size_table, {:batch_size, batch_size})
  end

  def set_batch_size(batch_size) do
    :ets.insert(@batch_size_table, {:batch_size, batch_size})
  end

  def get_batch_size do
    case :ets.lookup(@batch_size_table, :batch_size) do
      [{:batch_size, size}] -> size
      _ -> 1000
    end
  end

  def start_link(opts) do
    pid = spawn(fn -> loop(opts) end)
    {:ok, pid}
  end

  defp loop(opts) do
    generator = opts[:generator]
    run(generator)
  end

  defp run(generator) do
    batch_size = get_batch_size()
    batch = CrateWrite.Generator.generate_batch(generator, batch_size)
    CrateWrite.BatchBuffer.push(batch)
    run(generator)
  end
end
