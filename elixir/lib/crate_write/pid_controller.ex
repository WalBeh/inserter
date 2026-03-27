defmodule CrateWrite.PIDController do
  @moduledoc """
  PID controller that auto-tunes sender count and batch size to find
  the cluster's ingestion ceiling without overloading.

  Setpoint: P95 latency target (default 2s)
  Process variable: rolling P95 latency from last window
  Control outputs: sender count, batch size
  Emergency brake: rejected writes → halve senders immediately
  """
  use GenServer
  require Logger

  defstruct [
    :client,
    :insert_sql,
    :generator,
    :max_senders,
    :max_batch_size,
    :latency_target_ms,
    :batch_interval,
    # PID state
    :last_error,
    :integral,
    # Current tuning
    :current_senders,
    :current_batch_size,
    :sender_pids,
    # Tracking
    :last_rejected,
    :adjustments,
    :emergency_brakes,
    :sender_history,
    :peak_senders,
    :initial_senders
  ]

  # PID gains — tuned for 10-second control interval
  @kp 0.3    # Proportional: respond to current error
  @ki 0.05   # Integral: correct persistent offset
  @kd 0.1    # Derivative: dampen oscillation

  @initial_senders 6
  @initial_batch_size 500
  @control_interval_ms 10_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_state do
    GenServer.call(__MODULE__, :get_state)
  end

  # --- GenServer ---

  @impl true
  def init(opts) do
    client = opts[:client]
    insert_sql = opts[:insert_sql]
    generator = opts[:generator]
    max_senders = opts[:max_senders]
    max_batch_size = opts[:max_batch_size]
    latency_target_ms = opts[:latency_target_ms]
    batch_interval = opts[:batch_interval]

    # Get initial rejected writes baseline
    last_rejected = CrateWrite.Cluster.get_rejected_writes(client)

    state = %__MODULE__{
      client: client,
      insert_sql: insert_sql,
      generator: generator,
      max_senders: max_senders,
      max_batch_size: max_batch_size,
      latency_target_ms: latency_target_ms,
      batch_interval: batch_interval,
      last_error: 0.0,
      integral: 0.0,
      current_senders: @initial_senders,
      current_batch_size: @initial_batch_size,
      sender_pids: [],
      last_rejected: last_rejected,
      adjustments: 0,
      emergency_brakes: 0,
      sender_history: [@initial_senders],
      peak_senders: @initial_senders,
      initial_senders: @initial_senders
    }

    # Spawn initial senders
    state = spawn_senders(state, @initial_senders)

    # Start control loop
    Process.send_after(self(), :control_tick, @control_interval_ms)

    {:ok, state}
  end

  @impl true
  def handle_info(:control_tick, state) do
    state = run_control_cycle(state)
    Process.send_after(self(), :control_tick, @control_interval_ms)
    {:noreply, state}
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    result = %{
      enabled: true,
      latency_target_ms: state.latency_target_ms,
      initial_senders: state.initial_senders,
      final_senders: state.current_senders,
      final_batch_size: state.current_batch_size,
      peak_senders: state.peak_senders,
      adjustments: state.adjustments,
      emergency_brakes: state.emergency_brakes,
      sender_history: Enum.reverse(state.sender_history)
    }
    {:reply, result, state}
  end

  # --- Control Logic ---

  defp run_control_cycle(state) do
    # 1. Check rejected writes (emergency brake)
    current_rejected = CrateWrite.Cluster.get_rejected_writes(state.client)
    new_rejections = current_rejected - state.last_rejected
    state = %{state | last_rejected: current_rejected}

    if new_rejections > 0 do
      # EMERGENCY: halve senders and reduce batch size
      new_senders = max(div(state.current_senders, 2), 2)
      new_batch = max(div(state.current_batch_size, 2), 100)

      Logger.warning(
        "AUTO-TUNE EMERGENCY: #{new_rejections} rejected writes! " <>
        "Senders: #{state.current_senders}→#{new_senders}, " <>
        "Batch: #{state.current_batch_size}→#{new_batch}"
      )

      state = adjust_senders(state, new_senders)
      update_batch_size(new_batch)
      %{state |
        current_batch_size: new_batch,
        emergency_brakes: state.emergency_brakes + 1,
        integral: 0.0,
        adjustments: state.adjustments + 1
      }
    else
      # Normal PID control based on latency
      run_pid(state)
    end
  end

  defp run_pid(state) do
    # Get current P95 latency from monitor
    latency_stats = CrateWrite.Monitor.get_latency_stats()
    current_p95 = latency_stats.p95

    # Skip if no data yet
    if current_p95 == 0 do
      # No data — ramp up aggressively
      new_senders = min(state.current_senders + 4, state.max_senders)
      new_batch = min(state.current_batch_size + 200, state.max_batch_size)

      state = adjust_senders(state, new_senders)
      %{state |
        current_batch_size: new_batch,
        adjustments: state.adjustments + 1
      }
    else
      # PID calculation
      error = state.latency_target_ms - current_p95
      derivative = error - state.last_error
      integral = state.integral + error

      # Clamp integral to prevent windup
      integral = max(min(integral, 10_000), -10_000)

      output = @kp * error + @ki * integral + @kd * derivative

      # Convert PID output to sender and batch adjustments
      # Positive output = headroom → add capacity
      # Negative output = over target → reduce capacity
      sender_delta = round(output / 200)  # Scale: 200ms error = 1 sender
      batch_delta = round(output / 4) |> max(-200) |> min(200)  # Scale batch changes

      new_senders = state.current_senders + sender_delta
      new_senders = new_senders |> max(2) |> min(state.max_senders)

      new_batch = state.current_batch_size + batch_delta
      new_batch = new_batch |> max(100) |> min(state.max_batch_size)

      # Only log and count if something changed
      if new_senders != state.current_senders or new_batch != state.current_batch_size do
        Logger.info(
          "AUTO-TUNE: p95=#{round(current_p95)}ms target=#{state.latency_target_ms}ms " <>
          "error=#{round(error)}ms | " <>
          "Senders: #{state.current_senders}→#{new_senders}, " <>
          "Batch: #{state.current_batch_size}→#{new_batch}"
        )

        state = adjust_senders(state, new_senders)
        update_batch_size(new_batch)
        %{state |
          current_batch_size: new_batch,
          last_error: error,
          integral: integral,
          adjustments: state.adjustments + 1
        }
      else
        %{state | last_error: error, integral: integral}
      end
    end
  end

  # --- Sender Management ---

  defp spawn_senders(state, count) do
    new_pids =
      for i <- 0..(count - 1) do
        {:ok, pid} = CrateWrite.Sender.start_link(
          sender_id: length(state.sender_pids) + i,
          client: state.client,
          insert_sql: state.insert_sql,
          batch_interval: state.batch_interval,
          batch_size: state.current_batch_size
        )
        pid
      end

    %{state | sender_pids: state.sender_pids ++ new_pids}
  end

  defp adjust_senders(state, target) do
    current = length(state.sender_pids)

    state =
      cond do
        target > current ->
          # Spawn more senders
          spawn_senders(state, target - current)

        target < current ->
          # Kill excess senders (from the end)
          {keep, kill} = Enum.split(state.sender_pids, target)
          for pid <- kill, Process.alive?(pid), do: Process.exit(pid, :shutdown)
          %{state | sender_pids: keep}

        true ->
          state
      end

    peak = max(state.peak_senders, target)
    %{state |
      current_senders: target,
      peak_senders: peak,
      sender_history: [target | state.sender_history]
    }
  end

  defp update_batch_size(batch_size) do
    CrateWrite.GeneratorWorker.set_batch_size(batch_size)
  end
end
