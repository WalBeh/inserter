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
    :mode,  # "latency" or "rejections"
    # State
    :current_senders,
    :current_batch_size,
    :sender_pids,
    :phase,  # :probe | :probe_batch | :bisect | :bisect_batch | :hold
    # Bisection bounds (for senders or batch depending on phase)
    :good,   # highest value without issues
    :bad,    # lowest value with issues
    :good_batch,  # for batch bisection
    :bad_batch,
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
    mode = opts[:mode] || "latency"

    # In rejections mode: --batch-size is the starting batch, grows until rejections
    # In latency mode: start small (750), --batch-size is the max
    initial_batch = if mode == "rejections", do: opts[:max_batch_size], else: @initial_batch_size
    # Max batch: in rejections mode, no hard cap (use 50K as safety); in latency mode, use --batch-size
    max_batch = if mode == "rejections", do: 50_000, else: opts[:max_batch_size]

    state = %__MODULE__{
      client: opts[:client],
      insert_sql: opts[:insert_sql],
      max_senders: opts[:max_senders],
      max_batch_size: max_batch,
      latency_target_ms: opts[:latency_target_ms],
      batch_interval: opts[:batch_interval],
      mode: mode,
      current_senders: @initial_senders,
      current_batch_size: initial_batch,
      sender_pids: [],
      phase: :probe,
      good: @initial_senders,
      bad: nil,
      good_batch: initial_batch,
      bad_batch: nil,
      last_rejected: last_rejected,
      adjustments: 0,
      emergency_brakes: 0,
      sender_history: [@initial_senders],
      batch_history: [initial_batch],
      peak_senders: @initial_senders
    }

    CrateWrite.GeneratorWorker.set_batch_size(initial_batch)
    state = spawn_n_senders(state, @initial_senders)

    mode_label = if state.mode == "rejections", do: "rejections-only", else: "latency target=#{state.latency_target_ms}ms"
    IO.write(:stderr, "AUTO-TUNE: PROBE start senders=#{@initial_senders} batch=#{initial_batch} mode=#{mode_label}\n")

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
      mode: state.mode,
      latency_target_ms: state.latency_target_ms,
      initial_senders: @initial_senders,
      initial_batch_size: hd(Enum.reverse(state.batch_history)),
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
        :probe_batch -> probe_batch(state)
        :bisect -> bisect(state)
        :bisect_batch -> bisect_batch(state)
        :hold -> hold(state)
      end
    end
  end

  # --- Phase 1: PROBE ---
  # Ramp up aggressively until P95 exceeds target. This finds the cliff.

  defp probe(state) do
    if state.mode == "rejections" do
      # Rejections mode: just keep ramping, cliff is found only via emergency_brake
      state = %{state | good: state.current_senders}
      ramp_up(state)
    else
      # Latency mode: check P95 against target
      latency = CrateWrite.Monitor.get_window_latency_stats()
      p95 = latency.p95

      if p95 == 0 do
        ramp_up(state)
      else
        if p95 > state.latency_target_ms do
          enter_bisect(state, p95)
        else
          state = %{state | good: state.current_senders}
          ramp_up(state)
        end
      end
    end
  end

  defp enter_bisect(state, p95) do
    bad = state.current_senders
    good = state.good

    IO.write(:stderr, "AUTO-TUNE: CLIFF at #{bad} senders (p95=#{round(p95)}ms) — bisecting [#{good}, #{bad}]\n")

    mid = div(good + bad, 2)
    IO.write(:stderr, "AUTO-TUNE: BISECT next=#{mid} (bounds=[#{good}, #{bad}])\n")

    state = set_senders(%{state | phase: :bisect, bad: bad, good: good}, mid)
    %{state | adjustments: state.adjustments + 1}
  end

  defp ramp_up(state) do
    multiplier = if state.current_senders < 24, do: 2.0, else: 1.5
    new_senders = min(round(state.current_senders * multiplier), state.max_senders)

    # In rejections mode: keep batch fixed (user sets the batch they want to test)
    # In latency mode: ramp both senders and batch
    new_batch =
      if state.mode == "rejections" do
        state.current_batch_size
      else
        min(round(state.current_batch_size * 1.3), state.max_batch_size)
      end

    if new_senders == state.current_senders do
      if state.mode == "rejections" do
        # Senders maxed — now probe batch size
        IO.write(:stderr, "AUTO-TUNE: MAX SENDERS=#{new_senders} — now probing batch size\n")
        %{state | phase: :probe_batch, good_batch: state.current_batch_size}
      else
        IO.write(:stderr, "AUTO-TUNE: MAX REACHED senders=#{new_senders} batch=#{new_batch} — no rejections at max capacity, holding\n")
        %{state | phase: :hold}
      end
    else
      IO.write(:stderr, "AUTO-TUNE: PROBE senders=#{state.current_senders}→#{new_senders} batch=#{new_batch}\n")

      state = set_senders(state, new_senders)

      if new_batch != state.current_batch_size do
        CrateWrite.GeneratorWorker.set_batch_size(new_batch)
      end

      %{state |
        current_batch_size: new_batch,
        adjustments: state.adjustments + 1,
        batch_history: [new_batch | state.batch_history]
      }
    end
  end

  # --- Phase 2: BISECT ---
  # Binary search between good (under target) and bad (over target).

  defp bisect(state) do
    if state.mode == "rejections" do
      # In rejections mode, bisect just waits — the emergency_brake sets bounds
      # If no rejections, current count is OK
      good = max(state.current_senders, state.good)
      IO.write(:stderr, "AUTO-TUNE: BISECT #{state.current_senders} OK (no rejections) bounds=[#{good}, #{state.bad}]\n")

      range = state.bad - good
      if range <= 2 do
        IO.write(:stderr, "AUTO-TUNE: CONVERGED → #{good} senders (batch=#{state.current_batch_size})\n")
        state = set_senders(%{state | good: good}, good)
        %{state | phase: :hold, adjustments: state.adjustments + 1}
      else
        mid = div(good + state.bad, 2)
        IO.write(:stderr, "AUTO-TUNE: BISECT next=#{mid} (bounds=[#{good}, #{state.bad}])\n")
        state = set_senders(%{state | good: good}, mid)
        %{state | adjustments: state.adjustments + 1}
      end
    else
      bisect_latency(state)
    end
  end

  defp bisect_latency(state) do
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
        state = set_senders(%{state | good: good, bad: bad}, good)
        %{state | phase: :hold, adjustments: state.adjustments + 1}
      else
        # Try midpoint
        mid = div(good + bad, 2)
        IO.write(:stderr, "AUTO-TUNE: BISECT next=#{mid} (bounds=[#{good}, #{bad}])\n")
        state = set_senders(%{state | good: good, bad: bad}, mid)
        %{state | adjustments: state.adjustments + 1}
      end
    end
  end

  # --- Phase 2b: PROBE_BATCH (rejections mode only) ---
  # Senders are maxed. Now grow batch size until rejections appear.

  defp probe_batch(state) do
    new_batch = round(state.current_batch_size * 1.3)

    if new_batch > state.max_batch_size do
      IO.write(:stderr, "AUTO-TUNE: MAX BATCH=#{state.current_batch_size} — no rejections, holding\n")
      %{state | phase: :hold}
    else
      IO.write(:stderr, "AUTO-TUNE: PROBE BATCH #{state.current_batch_size}→#{new_batch} senders=#{state.current_senders}\n")

      CrateWrite.GeneratorWorker.set_batch_size(new_batch)

      %{state |
        current_batch_size: new_batch,
        good_batch: state.current_batch_size,
        adjustments: state.adjustments + 1,
        batch_history: [new_batch | state.batch_history]
      }
    end
  end

  # --- Phase 2c: BISECT_BATCH (after rejections during batch probing) ---

  defp bisect_batch(state) do
    # No rejections at this batch size — it's good
    good = max(state.current_batch_size, state.good_batch)
    bad = state.bad_batch

    IO.write(:stderr, "AUTO-TUNE: BISECT BATCH #{state.current_batch_size} OK bounds=[#{good}, #{bad}]\n")

    range = bad - good

    if range <= 100 do
      IO.write(:stderr, "AUTO-TUNE: CONVERGED → senders=#{state.current_senders} batch=#{good}\n")
      CrateWrite.GeneratorWorker.set_batch_size(good)
      %{state | current_batch_size: good, good_batch: good, phase: :hold, adjustments: state.adjustments + 1}
    else
      mid = div(good + bad, 2)
      IO.write(:stderr, "AUTO-TUNE: BISECT BATCH next=#{mid} (bounds=[#{good}, #{bad}])\n")
      CrateWrite.GeneratorWorker.set_batch_size(mid)
      %{state | current_batch_size: mid, good_batch: good, adjustments: state.adjustments + 1, batch_history: [mid | state.batch_history]}
    end
  end

  # --- Phase 3: HOLD ---
  # Stay at the converged optimal. Just hold steady.
  # No re-probing — the bisection found the ceiling, trust it.

  defp hold(state) do
    # Nothing to do — just stay at the converged sender count
    state
  end

  # --- Emergency Brake ---

  defp emergency_brake(state, new_rejections) do
    case state.phase do
      phase when phase in [:probe_batch, :bisect_batch] ->
        # Rejections during batch probing — bisect batch, keep senders fixed
        good_batch = state.good_batch
        bad_batch = state.current_batch_size
        mid = div(good_batch + bad_batch, 2)

        IO.write(:stderr, "AUTO-TUNE: BRAKE rejected=#{new_rejections} at batch=#{bad_batch} — bisecting batch [#{good_batch}, #{bad_batch}]\n")
        CrateWrite.GeneratorWorker.set_batch_size(mid)

        %{state |
          current_batch_size: mid,
          phase: :bisect_batch,
          good_batch: good_batch,
          bad_batch: bad_batch,
          emergency_brakes: state.emergency_brakes + 1,
          adjustments: state.adjustments + 1,
          batch_history: [mid | state.batch_history]
        }

      phase when phase in [:probe, :bisect] ->
        # Rejections during sender probing — bisect senders, keep batch fixed
        new_senders = max(round(state.current_senders * 0.75), 2)

        IO.write(:stderr, "AUTO-TUNE: BRAKE rejected=#{new_rejections} at senders=#{state.current_senders} — bisecting senders [#{new_senders}, #{state.current_senders}]\n")
        state = set_senders(state, new_senders)

        %{state |
          phase: :bisect,
          bad: state.current_senders,
          good: new_senders,
          emergency_brakes: state.emergency_brakes + 1,
          adjustments: state.adjustments + 1
        }

      :hold ->
        # Rejections while holding — reduce batch first (less disruptive)
        new_batch = max(round(state.current_batch_size * 0.75), 100)

        IO.write(:stderr, "AUTO-TUNE: BRAKE rejected=#{new_rejections} in HOLD — reducing batch #{state.current_batch_size}→#{new_batch}\n")
        CrateWrite.GeneratorWorker.set_batch_size(new_batch)

        %{state |
          current_batch_size: new_batch,
          good_batch: new_batch,
          bad_batch: state.current_batch_size,
          phase: :bisect_batch,
          emergency_brakes: state.emergency_brakes + 1,
          adjustments: state.adjustments + 1,
          batch_history: [new_batch | state.batch_history]
        }
    end
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
