defmodule CrateWrite.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Finch, name: CrateWrite.Finch, pools: %{default: [size: 50, count: 1]}},
      CrateWrite.Monitor,
      {DynamicSupervisor, name: CrateWrite.WorkerSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_one, name: CrateWrite.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
