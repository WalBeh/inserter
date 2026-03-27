defmodule CrateWrite.Client do
  @moduledoc "Finch-based HTTP client for CrateDB with optional gzip compression."

  defstruct [:base_url, :sql_url, :auth_header, :compress]

  def new(connection_string, opts \\ []) do
    uri = URI.parse(connection_string)
    base_url = "#{uri.scheme}://#{uri.host}:#{uri.port || 4200}"

    auth_header =
      if uri.userinfo do
        "Basic " <> Base.encode64(uri.userinfo)
      else
        nil
      end

    %__MODULE__{
      base_url: base_url,
      sql_url: "#{base_url}/_sql",
      auth_header: auth_header,
      compress: Keyword.get(opts, :compress, true)
    }
  end

  def execute(%__MODULE__{} = client, sql) do
    payload = Jason.encode!(%{"stmt" => sql})
    headers = build_headers(client.auth_header, false)

    request = Finch.build(:post, client.sql_url, headers, payload)

    case Finch.request(request, CrateWrite.Finch, receive_timeout: 30_000) do
      {:ok, %Finch.Response{status: status, body: body}} when status in 200..299 ->
        {:ok, Jason.decode!(body)}

      {:ok, %Finch.Response{status: status, body: body}} ->
        {:error, "HTTP #{status}: #{body}"}

      {:error, reason} ->
        {:error, "Request failed: #{inspect(reason)}"}
    end
  end

  def execute_query(%__MODULE__{} = client, sql) do
    case execute(client, sql) do
      {:ok, %{"rows" => rows}} -> {:ok, rows}
      {:ok, _} -> {:ok, []}
      error -> error
    end
  end

  @doc "Execute bulk insert. Returns {:ok, bytes_sent, latency_ms} or {:error, reason}."
  def execute_bulk(%__MODULE__{} = client, sql, bulk_args) do
    payload = Jason.encode!(%{"stmt" => sql, "bulk_args" => bulk_args})

    {body, content_encoding} =
      if client.compress do
        {:zlib.gzip(payload), "gzip"}
      else
        {payload, nil}
      end

    bytes_sent = byte_size(body)
    headers = build_headers(client.auth_header, content_encoding)

    request = Finch.build(:post, client.sql_url, headers, body)
    start = System.monotonic_time(:microsecond)

    case Finch.request(request, CrateWrite.Finch, receive_timeout: 60_000) do
      {:ok, %Finch.Response{status: status}} when status in 200..299 ->
        latency_ms = (System.monotonic_time(:microsecond) - start) / 1000.0
        {:ok, bytes_sent, latency_ms}

      {:ok, %Finch.Response{status: status, body: body}} ->
        {:error, "HTTP #{status}: #{body}"}

      {:error, reason} ->
        {:error, "Request failed: #{inspect(reason)}"}
    end
  end

  defp build_headers(auth_header, content_encoding) do
    headers = [{"content-type", "application/json"}]
    headers = if auth_header, do: [{"authorization", auth_header} | headers], else: headers
    headers = if content_encoding, do: [{"content-encoding", content_encoding} | headers], else: headers
    headers
  end
end
