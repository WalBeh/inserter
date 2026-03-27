defmodule CrateWrite.MixProject do
  use Mix.Project

  def project do
    [
      app: :crate_write,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      escript: escript()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {CrateWrite.Application, []}
    ]
  end

  defp escript do
    [main_module: CrateWrite]
  end

  defp deps do
    [
      {:finch, "~> 0.18"},
      {:jason, "~> 1.4"},
      {:elixir_uuid, "~> 1.2"},
      {:dotenvy, "~> 0.8"}
    ]
  end
end
