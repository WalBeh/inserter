defmodule CrateWrite.Config do
  @moduledoc "Configuration struct with defaults matching Rust/Python implementations."

  defstruct [
    :table_name,
    :connection_string,
    :duration,
    batch_size: 100,
    batch_interval: 100,
    threads: 1,
    objects: 0,
    shards: 4,
    replicas: 1,
    compress: true,
    benchmark: false,
    test_loadbalancer: false,
    auto_tune: false,
    latency_target: 2.0,
    log_level: "info"
  ]

  def from_cli(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [
          table_name: :string,
          duration: :integer,
          connection_string: :string,
          batch_size: :integer,
          batch_interval: :integer,
          threads: :integer,
          objects: :integer,
          shards: :integer,
          replicas: :integer,
          test_loadbalancer: :boolean,
          benchmark: :boolean,
          no_compression: :boolean,
          auto_tune: :boolean,
          latency_target: :float,
          log_level: :string,
          config: :string
        ]
      )

    # Load .env (check current dir and parent)
    env_file = cond do
      File.exists?(".env") -> ".env"
      File.exists?("../.env") -> "../.env"
      true -> nil
    end

    if env_file do
      env_file
      |> File.read!()
      |> String.split("\n")
      |> Enum.each(fn line ->
        line = String.trim(line)
        unless line == "" or String.starts_with?(line, "#") do
          case String.split(line, "=", parts: 2) do
            [key, val] -> System.put_env(String.trim(key), String.trim(val))
            _ -> :ok
          end
        end
      end)
    end

    # Start with defaults
    config = %__MODULE__{}

    # Load config file if specified or auto-detect
    config =
      cond do
        opts[:config] -> load_file(config, opts[:config])
        File.exists?("config.toml") -> load_file(config, "config.toml")
        true -> config
      end

    # Connection string: CLI > env var
    connection_string =
      opts[:connection_string] || config.connection_string || System.get_env("CRATE_CONNECTION_STRING")

    # Merge CLI overrides (only non-nil)
    %{
      config
      | table_name: opts[:table_name] || config.table_name,
        connection_string: connection_string,
        duration: opts[:duration] || config.duration,
        batch_size: opts[:batch_size] || config.batch_size,
        batch_interval: opts[:batch_interval] || config.batch_interval,
        threads: opts[:threads] || config.threads,
        objects: opts[:objects] || config.objects,
        shards: opts[:shards] || config.shards,
        replicas: opts[:replicas] || config.replicas,
        compress: !opts[:no_compression] && config.compress,
        benchmark: opts[:benchmark] || config.benchmark,
        test_loadbalancer: opts[:test_loadbalancer] || config.test_loadbalancer,
        auto_tune: opts[:auto_tune] || config.auto_tune,
        latency_target: opts[:latency_target] || config.latency_target,
        log_level: opts[:log_level] || config.log_level
    }
  end

  def validate!(%__MODULE__{} = config) do
    unless config.connection_string, do: raise("Connection string is required")
    unless config.table_name, do: raise("Table name is required")
    unless config.duration, do: raise("Duration is required")
    if config.batch_size < 1, do: raise("Batch size must be > 0")
    if config.batch_size > 100_000, do: raise("Batch size cannot exceed 100000")
    if config.threads < 1, do: raise("Threads must be > 0")
    if config.threads > 1000, do: raise("Threads cannot exceed 1000")
    config
  end

  def sanitize_connection_string(nil), do: "not-set"

  def sanitize_connection_string(url) do
    uri = URI.parse(url)
    "#{uri.scheme}://#{uri.host}:#{uri.port || 4200}"
  end

  defp load_file(config, path) do
    # Simple key-value parsing for TOML-like config
    case File.read(path) do
      {:ok, content} ->
        content
        |> String.split("\n")
        |> Enum.reduce(config, fn line, acc ->
          line = String.trim(line)

          cond do
            String.starts_with?(line, "#") -> acc
            line == "" -> acc
            true ->
              case String.split(line, "=", parts: 2) do
                [key, val] ->
                  key = key |> String.trim() |> String.to_atom()
                  val = val |> String.trim() |> String.trim("\"")
                  apply_config_value(acc, key, val)

                _ ->
                  acc
              end
          end
        end)

      {:error, _} ->
        config
    end
  end

  defp apply_config_value(config, :table_name, val), do: %{config | table_name: val}
  defp apply_config_value(config, :connection_string, val), do: %{config | connection_string: val}
  defp apply_config_value(config, :duration, val), do: %{config | duration: String.to_integer(val)}
  defp apply_config_value(config, :batch_size, val), do: %{config | batch_size: String.to_integer(val)}
  defp apply_config_value(config, :batch_interval, val), do: %{config | batch_interval: String.to_integer(val)}
  defp apply_config_value(config, :threads, val), do: %{config | threads: String.to_integer(val)}
  defp apply_config_value(config, :objects, val), do: %{config | objects: String.to_integer(val)}
  defp apply_config_value(config, :shards, val), do: %{config | shards: String.to_integer(val)}
  defp apply_config_value(config, :replicas, val), do: %{config | replicas: String.to_integer(val)}
  defp apply_config_value(config, :log_level, val), do: %{config | log_level: val}
  defp apply_config_value(config, _key, _val), do: config
end
