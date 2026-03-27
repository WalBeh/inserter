defmodule CrateWrite.PIDController do
  @moduledoc """
  Binary search controller that finds the optimal sender count.

  Phase 1: PROBE — ramp up aggressively until P95 latency exceeds target
  Phase 2: BISECT — binary search between last-good and first-bad
  Phase 3: HOLD — lock at optimal, re-probe if throughput drops
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
    # State
    :current_senders,
    :current_batch_size,
    :sender_pids,
    :phase,  # :probe | :bisect | :hold
    # Bisection bounds
    :good,   # highest sender count with P95 < target
    :bad,    # lowest sender count with P95 > target
    # Hold phase
    :hold_throughput,  # throughput when we locked in
    # Tracking
    :last_rejected,
    :adjustments,
    :emergency_brakes,
    :sender_history,
    :batch_history,
    :peak_senders
  ]

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
      current_senders: @initial_senders,
      current_batch_size: @initial_batch_size,
      sender_pids: [],
      phase: :probe,
      good: @initial_senders,
      bad: nil,
      hold_throughput: 0,
      last_rejected: last_rejected,
      adjustments: 0,
      emergency_brakes: 0,
      sender_history: [@initial_senders],
      batch_history: [@initial_batch_size],
      peak_senders: @initial_senders
    }

    CrateWrite.GeneratorWorker.set_batch_size(@initial_batch_size)
    state = spawn_n_senders(state, @initial_senders)

    IO.write(:stderr, "AUTO-TUNE: PROBE start senders=#{@initial_senders} batch=#{@initial_batch_size} target=#{state.latency_target_ms}ms\n")

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
      algorithm: "bisect",
      latency_target_ms: state.latency_target_ms,
      initial_senders: @initial_senders,
      initial_batch_size: @initial_batch_size,
      final_senders: state.current_senders,
      final_batch_size: state.current_batch_size,
      final_phase: Atom.to_string(state.phase),
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
    # Check rejected writes first (always)
    current_rejected = CrateWrite.Cluster.get_rejected_writes(state.client)
    new_rejections = current_rejected - state.last_rejected
    state = %{state | last_rejected: current_rejected}

    if new_rejections > 0 do
      emergency_brake(state, new_rejections)
    else
      case state.phase do
        :probe -> probe(state)
        :bisect -> bisect(state)
        :hold -> hold(state)
      end
    end
  end

  # --- Phase 1: PROBE ---
  # Ramp up aggressively until P95 exceeds target. This finds the cliff.

  defp probe(state) do
    latency = CrateWrite.Monitor.get_window_latency_stats()
    p95 = latency.p95

    if p95 == 0 do
      # No data yet — keep ramping
      ramp_up(state)
    else
      if p95 > state.latency_target_ms do
        # Found the cliff! Current count is bad, previous was good
        bad = state.current_senders
        good = state.good

        IO.write(:stderr, "AUTO-TUNE: CLIFF at #{bad} senders (p95=#{round(p95)}ms) — bisecting [#{good}, #{bad}]\n")

        # Enter bisection, try the midpoint
        mid = div(good + bad, 2)
        IO.write(:stderr, "AUTO-TUNE: BISECT next=#{mid} (bounds=[#{good}, #{bad}])\n")

        state = set_senders(%{state | phase: :bisect, bad: bad, good: good}, mid)
        %{state | adjustments: state.adjustments + 1}
      else
        # Under target — this count is good, keep probing
        state = %{state | good: state.current_senders}
        ramp_up(state)
      end
    end
  end

  defp ramp_up(state) do
    # Double until 24, then +50%
    multiplier = if state.current_senders < 24, do: 2.0, else: 1.5
    new_senders = min(round(state.current_senders * multiplier), state.max_senders)

    # Also ramp batch during probe
    new_batch = min(round(state.current_batch_size * 1.3), state.max_batch_size)

    IO.write(:stderr, "AUTO-TUNE: PROBE senders=#{state.current_senders}→#{new_senders} batch=#{state.current_batch_size}→#{new_batch} (p95 ok)\n")

    state = set_senders(state, new_senders)
    CrateWrite.GeneratorWorker.set_batch_size(new_batch)

    %{state |
      current_batch_size: new_batch,
      adjustments: state.adjustments + 1,
      batch_history: [new_batch | state.batch_history]
    }
  end

  # --- Phase 2: BISECT ---
  # Binary search between good (under target) and bad (over target).

  defp bisect(state) do
    latency = CrateWrite.Monitor.get_window_latency_stats()
    p95 = latency.p95

    if p95 == 0 do
      state
    else
      # Classify current sender count
      too_high = p95 > state.latency_target_ms * 1.2

      {good, bad} =
        if too_high do
          # Current count is too many — it becomes the new upper bound
          {state.good, min(state.current_senders, state.bad || state.current_senders)}
        else
          # Current count is fine — it becomes the new lower bound
          {max(state.current_senders, state.good), state.bad}
        end

      label = if too_high, do: "HIGH", else: "OK"
      IO.write(:stderr, "AUTO-TUNE: BISECT #{state.current_senders} #{label} (p95=#{round(p95)}ms) bounds=[#{good}, #{bad}]\n")

      range = bad - good

      if range <= 2 do
        # Converged
        IO.write(:stderr, "AUTO-TUNE: CONVERGED → #{good} senders (batch=#{state.current_batch_size})\n")
        stats = CrateWrite.Monitor.get_current_stats()
        state = set_senders(%{state | good: good, bad: bad}, good)
        %{state | phase: :hold, hold_throughput: stats.current_rate, adjustments: state.adjustments + 1}
      else
        # Try midpoint
        mid = div(good + bad, 2)
        IO.write(:stderr, "AUTO-TUNE: BISECT next=#{mid} (bounds=[#{good}, #{bad}])\n")
        state = set_senders(%{state | good: good, bad: bad}, mid)
        %{state | adjustments: state.adjustments + 1}
      end
    end
  end

  # --- Phase 3: HOLD ---
  # Stay at optimal. Re-probe if throughput drops significantly.

  defp hold(state) do
    stats = CrateWrite.Monitor.get_current_stats()
    current_rate = stats.current_rate

    # Check if throughput dropped >30% from when we locked in
    if state.hold_throughput > 0 and current_rate > 0 and
       current_rate < state.hold_throughput * 0.7 do
      IO.write(:stderr, "AUTO-TUNE: HOLD throughput dropped (#{round(current_rate)} < #{round(state.hold_throughput * 0.7)}) — re-probing\n")

      # Re-enter probe from current position
      %{state |
        phase: :probe,
        good: max(div(state.current_senders, 2), 2),
        bad: nil,
        adjustments: state.adjustments + 1
      }
    else
      # Update hold throughput to track gradual changes
      new_hold = if current_rate > 0, do: current_rate, else: state.hold_throughput
      %{state | hold_throughput: new_hold}
    end
  end

  # --- Emergency Brake ---

  defp emergency_brake(state, new_rejections) do
    new_senders = max(round(state.current_senders * 0.75), 2)
    new_batch = max(round(state.current_batch_size * 0.75), 100)

    IO.write(:stderr, "AUTO-TUNE: BRAKE rejected=#{new_rejections} senders=#{state.current_senders}→#{new_senders} batch=#{state.current_batch_size}→#{new_batch}\n")

    state = set_senders(state, new_senders)
    CrateWrite.GeneratorWorker.set_batch_size(new_batch)

    # Re-enter bisection with current as bad, reduced as potential good
    %{state |
      current_batch_size: new_batch,
      phase: :bisect,
      bad: state.current_senders,
      good: new_senders,
      emergency_brakes: state.emergency_brakes + 1,
      adjustments: state.adjustments + 1,
      batch_history: [new_batch | state.batch_history]
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
    alive_pids = Enum.filter(state.sender_pids, &Process.alive?/1)
    current = length(alive_pids)

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
end
