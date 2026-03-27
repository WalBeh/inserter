defmodule CrateWrite.BatchBuffer do
  @moduledoc """
  Bounded buffer between generator and sender processes.
  Generators push batches, senders pull them. Uses ETS for lock-free access.
  """
  use GenServer

  @table :batch_buffer
  @max_buffered 128

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc "Push a batch into the buffer. Blocks if buffer is full."
  def push(batch) do
    GenServer.call(__MODULE__, {:push, batch}, :infinity)
  end

  @doc "Pull a batch from the buffer. Blocks until one is available."
  def pull do
    GenServer.call(__MODULE__, :pull, :infinity)
  end

  def buffer_size do
    GenServer.call(__MODULE__, :size)
  end

  # --- GenServer ---

  @impl true
  def init(_) do
    {:ok, %{queue: :queue.new(), waiting_pushers: :queue.new(), waiting_pullers: :queue.new()}}
  end

  @impl true
  def handle_call({:push, batch}, from, state) do
    if :queue.len(state.queue) >= @max_buffered do
      # Buffer full — park the pusher until a puller drains
      {:noreply, %{state | waiting_pushers: :queue.in({from, batch}, state.waiting_pushers)}}
    else
      # Check if any puller is waiting
      case :queue.out(state.waiting_pullers) do
        {{:value, puller_from}, rest} ->
          GenServer.reply(puller_from, batch)
          {:reply, :ok, %{state | waiting_pullers: rest}}

        {:empty, _} ->
          {:reply, :ok, %{state | queue: :queue.in(batch, state.queue)}}
      end
    end
  end

  def handle_call(:pull, from, state) do
    case :queue.out(state.queue) do
      {{:value, batch}, rest} ->
        # Got a batch — also unblock a waiting pusher if any
        state = %{state | queue: rest}
        state = unblock_pusher(state)
        {:reply, batch, state}

      {:empty, _} ->
        # Nothing in buffer — park the puller
        {:noreply, %{state | waiting_pullers: :queue.in(from, state.waiting_pullers)}}
    end
  end

  def handle_call(:size, _from, state) do
    {:reply, :queue.len(state.queue), state}
  end

  defp unblock_pusher(state) do
    case :queue.out(state.waiting_pushers) do
      {{:value, {pusher_from, batch}}, rest} ->
        # Check if a puller is waiting
        case :queue.out(state.waiting_pullers) do
          {{:value, puller_from}, puller_rest} ->
            GenServer.reply(pusher_from, :ok)
            GenServer.reply(puller_from, batch)
            %{state | waiting_pushers: rest, waiting_pullers: puller_rest}

          {:empty, _} ->
            GenServer.reply(pusher_from, :ok)
            %{state | queue: :queue.in(batch, state.queue), waiting_pushers: rest}
        end

      {:empty, _} ->
        state
    end
  end
end
