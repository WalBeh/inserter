defmodule CrateWrite.Application do
  use Application

  @impl true
  def start(_type, _args) do
    # Pool size will be reconfigured before workers start
    children = [
      CrateWrite.Monitor,
      {DynamicSupervisor, name: CrateWrite.WorkerSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_one, name: CrateWrite.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def start_finch(pool_size) do
    Finch.start_link(
      name: CrateWrite.Finch,
      pools: %{default: [size: pool_size, count: 1]}
    )
  end
end
