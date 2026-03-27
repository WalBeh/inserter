defmodule CrateWrite.PIDController do
  @moduledoc """
  PID controller that auto-tunes sender count and batch size.

  Strategy:
  - Ramp up aggressively (multiplicative increase) to find ceiling fast
  - Back off gently (25% reduction) on overload
  - Dead zone: don't adjust if latency is 70-100% of target
  - Emergency brake on rejected writes: reduce 25%, reset integral
  """
  use GenServer
  require Logger

  defstruct [
    :client,
    :insert_sql,
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
    :batch_history,
    :peak_senders,
    :phase  # :ramp_up | :stable | :recovery
  ]

  @kp 0.15
  @ki 0.03
  @kd 0.05

  @initial_senders 12
  @initial_batch_size 750
  @control_interval_ms 5_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_state do
    GenServer.call(__MODULE__, :get_state, 10_000)
  end

  # --- GenServer ---

  @impl true
  def init(opts) do
    last_rejected = CrateWrite.Cluster.get_rejected_writes(opts[:client])

    state = %__MODULE__{
      client: opts[:client],
      insert_sql: opts[:insert_sql],
      max_senders: opts[:max_senders],
      max_batch_size: opts[:max_batch_size],
      latency_target_ms: opts[:latency_target_ms],
      batch_interval: opts[:batch_interval],
      last_error: 0.0,
      integral: 0.0,
      current_senders: @initial_senders,
      current_batch_size: @initial_batch_size,
      sender_pids: [],
      last_rejected: last_rejected,
      adjustments: 0,
      emergency_brakes: 0,
      sender_history: [@initial_senders],
      batch_history: [@initial_batch_size],
      peak_senders: @initial_senders,
      phase: :ramp_up
    }

    # Initialize dynamic batch size
    CrateWrite.GeneratorWorker.set_batch_size(@initial_batch_size)

    # Spawn initial senders
    state = spawn_n_senders(state, @initial_senders)

    IO.write(:stderr, "AUTO-TUNE: start senders=#{@initial_senders} batch=#{@initial_batch_size} target=#{state.latency_target_ms}ms\n")

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
      initial_senders: @initial_senders,
      initial_batch_size: @initial_batch_size,
      final_senders: state.current_senders,
      final_batch_size: state.current_batch_size,
      peak_senders: state.peak_senders,
      adjustments: state.adjustments,
      emergency_brakes: state.emergency_brakes,
      sender_history: Enum.reverse(state.sender_history),
      batch_history: Enum.reverse(state.batch_history)
    }
    {:reply, result, state}
  end

  # --- Control Logic ---

  defp run_control_cycle(state) do
    # Check rejected writes
    current_rejected = CrateWrite.Cluster.get_rejected_writes(state.client)
    new_rejections = current_rejected - state.last_rejected
    state = %{state | last_rejected: current_rejected}

    if new_rejections > 0 do
      emergency_brake(state, new_rejections)
    else
      latency_stats = CrateWrite.Monitor.get_latency_stats()
      current_p95 = latency_stats.p95

      if current_p95 == 0 do
        # No data yet — ramp up
        ramp_up(state)
      else
        ratio = current_p95 / state.latency_target_ms

        cond do
          ratio > 1.0 ->
            # Over target — back off gently
            back_off(state, current_p95, ratio)

          ratio < 0.7 ->
            # Under 70% of target — ramp up
            ramp_up_pid(state, current_p95)

          true ->
            # Dead zone (70-100% of target) — stable, don't adjust
            if state.phase != :stable do
              IO.write(:stderr, "AUTO-TUNE: STABLE p95=#{round(current_p95)}ms senders=#{state.current_senders} batch=#{state.current_batch_size}\n")
            end
            %{state | phase: :stable, last_error: state.latency_target_ms - current_p95}
        end
      end
    end
  end

  defp emergency_brake(state, new_rejections) do
    # Reduce by 25% (not 50% — avoid traffic jam)
    new_senders = max(round(state.current_senders * 0.75), 2)
    new_batch = max(round(state.current_batch_size * 0.75), 100)

    IO.write(:stderr, "AUTO-TUNE: BRAKE rejected=#{new_rejections} senders=#{state.current_senders}→#{new_senders} batch=#{state.current_batch_size}→#{new_batch}\n")

    state = set_senders(state, new_senders)
    CrateWrite.GeneratorWorker.set_batch_size(new_batch)

    %{state |
      current_batch_size: new_batch,
      emergency_brakes: state.emergency_brakes + 1,
      adjustments: state.adjustments + 1,
      integral: 0.0,
      phase: :recovery,
      batch_history: [new_batch | state.batch_history]
    }
  end

  defp ramp_up(state) do
    # No latency data yet — double everything aggressively
    new_senders = min(state.current_senders * 2, state.max_senders)
    new_batch = min(round(state.current_batch_size * 1.5), state.max_batch_size)

    IO.write(:stderr, "AUTO-TUNE: RAMP senders=#{state.current_senders}→#{new_senders} batch=#{state.current_batch_size}→#{new_batch}\n")

    state = set_senders(state, new_senders)
    CrateWrite.GeneratorWorker.set_batch_size(new_batch)

    %{state |
      current_batch_size: new_batch,
      adjustments: state.adjustments + 1,
      phase: :ramp_up,
      batch_history: [new_batch | state.batch_history]
    }
  end

  defp ramp_up_pid(state, current_p95) do
    # If we've never backed off, keep doubling (fast discovery)
    # Once we've backed off, switch to additive (fine-tuning)
    {new_senders, new_batch} =
      if state.phase == :ramp_up and state.emergency_brakes == 0 do
        # Multiplicative: double senders, +50% batch
        s = min(state.current_senders * 2, state.max_senders)
        b = min(round(state.current_batch_size * 1.5), state.max_batch_size)
        {s, b}
      else
        # Additive: PID-controlled fine-tuning
        error = state.latency_target_ms - current_p95
        derivative = error - state.last_error
        integral = clamp(state.integral + error, -10_000, 10_000)

        output = @kp * error + @ki * integral + @kd * derivative

        sender_add = max(round(output / 300), 1)
        batch_add = max(round(output / 5), 50)

        s = min(state.current_senders + sender_add, state.max_senders)
        b = min(state.current_batch_size + batch_add, state.max_batch_size)
        {s, b}
      end

    error = state.latency_target_ms - current_p95
    integral = clamp(state.integral + error, -10_000, 10_000)

    if new_senders != state.current_senders or new_batch != state.current_batch_size do
      IO.write(:stderr, "AUTO-TUNE: UP p95=#{round(current_p95)}ms senders=#{state.current_senders}→#{new_senders} batch=#{state.current_batch_size}→#{new_batch}\n")

      state = set_senders(state, new_senders)
      CrateWrite.GeneratorWorker.set_batch_size(new_batch)

      %{state |
        current_batch_size: new_batch,
        last_error: error,
        integral: integral,
        adjustments: state.adjustments + 1,
        phase: :ramp_up,
        batch_history: [new_batch | state.batch_history]
      }
    else
      %{state | last_error: error, integral: integral}
    end
  end

  defp back_off(state, current_p95, ratio) do
    # Gentle: reduce by (ratio - 1) * 15%, minimum 1 sender removed
    reduction = max(round(state.current_senders * (ratio - 1.0) * 0.15), 1)
    new_senders = max(state.current_senders - reduction, 2)
    # Don't reduce batch on gentle back-off — only on emergency
    new_batch = state.current_batch_size

    IO.write(:stderr, "AUTO-TUNE: DOWN p95=#{round(current_p95)}ms senders=#{state.current_senders}→#{new_senders}\n")

    state = set_senders(state, new_senders)

    %{state |
      current_batch_size: new_batch,
      adjustments: state.adjustments + 1,
      integral: 0.0,  # Reset to avoid overshoot on recovery
      phase: :recovery
    }
  end

  # --- Sender Management ---

  defp spawn_n_senders(state, count) do
    new_pids =
      for i <- 0..(count - 1) do
        {:ok, pid} = CrateWrite.Sender.start_link(
          sender_id: length(state.sender_pids) + i,
          client: state.client,
          insert_sql: state.insert_sql,
          batch_interval: state.batch_interval
        )
        pid
      end

    %{state | sender_pids: state.sender_pids ++ new_pids}
  end

  defp set_senders(state, target) do
    current = length(Enum.filter(state.sender_pids, &Process.alive?/1))
    alive_pids = Enum.filter(state.sender_pids, &Process.alive?/1)

    state =
      cond do
        target > current ->
          spawn_n_senders(%{state | sender_pids: alive_pids}, target - current)

        target < current ->
          {keep, kill} = Enum.split(alive_pids, target)
          for pid <- kill, do: Process.exit(pid, :kill)
          %{state | sender_pids: keep}

        true ->
          %{state | sender_pids: alive_pids}
      end

    peak = max(state.peak_senders, target)
    %{state |
      current_senders: target,
      peak_senders: peak,
      sender_history: [target | state.sender_history]
    }
  end

  defp clamp(val, min_val, max_val), do: max(min(val, max_val), min_val)
end
