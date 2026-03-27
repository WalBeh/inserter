defmodule CrateWrite.Cluster do
  @moduledoc "CrateDB cluster operations: table creation, sys.nodes queries, verification."

  alias CrateWrite.Client

  def create_table(client, table_name, objects, shards, replicas) do
    base_columns = [
      "id TEXT PRIMARY KEY",
      "timestamp TIMESTAMP WITH TIME ZONE",
      "region TEXT",
      "product_category TEXT",
      "event_type TEXT",
      "user_id INTEGER",
      "user_segment TEXT",
      "amount DOUBLE PRECISION",
      "quantity INTEGER",
      "metadata OBJECT(DYNAMIC)"
    ]

    obj_columns = if objects > 0, do: (for i <- 0..(objects - 1), do: "obj_#{i} TEXT"), else: []
    all_columns = base_columns ++ obj_columns

    sql = """
    CREATE TABLE IF NOT EXISTS #{table_name} (#{Enum.join(all_columns, ", ")})
    CLUSTERED INTO #{shards} SHARDS
    WITH (number_of_replicas = #{replicas})
    """

    Client.execute(client, sql)
  end

  def build_insert_sql(table_name, objects) do
    base_fields = "id, timestamp, region, product_category, event_type, user_id, user_segment, amount, quantity, metadata"
    obj_fields = if objects > 0, do: ", " <> Enum.map_join(0..(objects - 1), ", ", &"obj_#{&1}"), else: ""
    placeholders = Enum.map_join(1..(10 + objects), ", ", fn _ -> "?" end)

    "INSERT INTO #{table_name} (#{base_fields}#{obj_fields}) VALUES (#{placeholders})"
  end

  def query_cluster_info(client) do
    info = %{}

    info =
      case Client.execute_query(client, "SELECT os_info['available_processors'] FROM sys.nodes") do
        {:ok, rows} ->
          cpus = Enum.map(rows, fn [v] -> v end)

          info
          |> Map.put("cpus_per_node", cpus)
          |> Map.put("total_cpus", Enum.sum(cpus))
          |> Map.put("nodes", length(cpus))

        _ ->
          info
      end

    info =
      case Client.execute_query(client, "SELECT mem['used'] FROM sys.nodes") do
        {:ok, rows} -> Map.put(info, "memory_used_bytes", Enum.map(rows, fn [v] -> v end))
        _ -> info
      end

    info =
      case Client.execute_query(client, "SELECT fs['total']['size'] FROM sys.nodes") do
        {:ok, rows} -> Map.put(info, "disk_total_bytes", Enum.map(rows, fn [v] -> v end))
        _ -> info
      end

    info =
      case Client.execute_query(client, "SELECT heap['max'], version['number'] FROM sys.nodes LIMIT 1") do
        {:ok, [[heap, version] | _]} ->
          info
          |> Map.put("heap_max_bytes", heap)
          |> Map.put("version", version)

        _ ->
          info
      end

    info
  end

  def get_record_count(client, table_name) do
    Client.execute(client, "REFRESH TABLE #{table_name}")

    case Client.execute_query(client, "SELECT COUNT(*) FROM #{table_name}") do
      {:ok, [[count] | _]} -> count
      _ -> 0
    end
  end

  def get_rejected_writes(client) do
    sql = """
    SELECT SUM(pool['rejected'])
    FROM (SELECT UNNEST(thread_pools) AS pool FROM sys.nodes) x
    WHERE pool['name'] = 'write'
    """

    case Client.execute_query(client, sql) do
      {:ok, [[count] | _]} -> count || 0
      _ -> 0
    end
  end
end
