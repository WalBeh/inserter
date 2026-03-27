defmodule CrateWrite.GeneratorWorker do
  @moduledoc """
  Generator process: continuously generates batches and pushes them to the buffer.
  Runs on its own BEAM process — CPU work is preemptively scheduled across cores.
  """

  def start_link(opts) do
    pid = spawn(fn -> loop(opts) end)
    {:ok, pid}
  end

  defp loop(opts) do
    generator = opts[:generator]
    batch_size = opts[:batch_size]

    run(generator, batch_size)
  end

  defp run(generator, batch_size) do
    batch = CrateWrite.Generator.generate_batch(generator, batch_size)
    CrateWrite.BatchBuffer.push(batch)
    run(generator, batch_size)
  end
end
