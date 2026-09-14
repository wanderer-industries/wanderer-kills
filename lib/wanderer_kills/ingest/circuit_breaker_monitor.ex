defmodule WandererKills.Ingest.CircuitBreakerMonitor do
  @moduledoc """
  Monitors the R2Z2 circuit breaker and triggers alerts or recovery actions
  when the circuit remains open for too long.
  """

  use GenServer
  require Logger

  alias WandererKills.Config

  # Configuration constants
  @check_interval_ms Application.compile_env(
                       :wanderer_kills,
                       [:circuit_breaker_monitor, :check_interval_ms],
                       60_000
                     )
  @alert_threshold_ms Application.compile_env(
                        :wanderer_kills,
                        [:circuit_breaker_monitor, :alert_threshold_ms],
                        600_000
                      )

  defmodule State do
    @moduledoc false
    defstruct [
      :circuit_opened_at,
      :circuit_opened_at_wall,
      :last_alert_sent_at,
      :last_alert_sent_at_wall,
      :consecutive_open_checks,
      :ingest_source
    ]
  end

  # Public API

  @doc """
  Starts the circuit breaker monitor.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Gets the current monitor status.
  """
  def get_status do
    GenServer.call(__MODULE__, :get_status)
  end

  # GenServer callbacks

  @impl true
  def init(_opts) do
    # Determine which ingest source is active
    ingest_source = determine_ingest_source()
    Logger.info("[CircuitBreakerMonitor] Starting circuit breaker monitor for #{ingest_source}")

    # Schedule first check
    schedule_check()

    state = %State{
      circuit_opened_at: nil,
      circuit_opened_at_wall: nil,
      last_alert_sent_at: nil,
      last_alert_sent_at_wall: nil,
      consecutive_open_checks: 0,
      ingest_source: ingest_source
    }

    {:ok, state}
  end

  @impl true
  def handle_info(:check_circuit, state) do
    new_state = check_circuit_status(state)

    # Schedule next check
    schedule_check()

    {:noreply, new_state}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    now = System.monotonic_time(:millisecond)

    # Calculate numeric durations in milliseconds
    circuit_open_duration_ms =
      if state.circuit_opened_at do
        now - state.circuit_opened_at
      else
        nil
      end

    time_since_last_alert_ms =
      if state.last_alert_sent_at do
        now - state.last_alert_sent_at
      else
        nil
      end

    # Calculate human-readable durations
    circuit_open_duration =
      if circuit_open_duration_ms do
        format_duration(circuit_open_duration_ms)
      else
        nil
      end

    time_since_last_alert =
      if time_since_last_alert_ms do
        format_duration(time_since_last_alert_ms)
      else
        nil
      end

    status = %{
      ingest_source: state.ingest_source,
      circuit_opened_at_wall: state.circuit_opened_at_wall,
      circuit_open_duration: circuit_open_duration,
      circuit_open_duration_ms: circuit_open_duration_ms,
      last_alert_sent_at_wall: state.last_alert_sent_at_wall,
      time_since_last_alert: time_since_last_alert,
      time_since_last_alert_ms: time_since_last_alert_ms,
      consecutive_open_checks: state.consecutive_open_checks,
      monitoring: true
    }

    {:reply, status, state}
  end

  # Private functions

  defp determine_ingest_source do
    if Config.start_r2z2?(), do: :r2z2, else: :none
  end

  defp schedule_check do
    Process.send_after(self(), :check_circuit, @check_interval_ms)
  end

  defp check_circuit_status(%State{ingest_source: :r2z2} = state) do
    case WandererKills.Ingest.R2Z2.get_circuit_status_cached() do
      {:ok, %{circuit_state: :open} = circuit_status} ->
        handle_open_circuit(state, circuit_status)

      {:ok, %{circuit_state: circuit_state}} when circuit_state in [:closed, :half_open] ->
        handle_closed_circuit(state)

      _ ->
        state
    end
  end

  defp check_circuit_status(state), do: state

  defp handle_open_circuit(%State{} = state, circuit_status) do
    now = System.monotonic_time(:millisecond)
    now_wall = DateTime.utc_now() |> DateTime.to_iso8601()

    source_label = source_label(state.ingest_source)

    # Track when circuit first opened
    circuit_opened_at = state.circuit_opened_at || circuit_status.circuit_opened_at || now
    circuit_opened_at_wall = state.circuit_opened_at_wall || now_wall

    # Calculate how long circuit has been open
    open_duration_ms = now - circuit_opened_at

    # Update consecutive open checks
    consecutive_open_checks = state.consecutive_open_checks + 1

    Logger.warning(
      "[CircuitBreakerMonitor] #{source_label} circuit breaker is OPEN",
      consecutive_errors: circuit_status.consecutive_errors,
      open_duration_ms: open_duration_ms,
      consecutive_open_checks: consecutive_open_checks
    )

    # Check if we should send an alert
    new_state = %State{
      state
      | circuit_opened_at: circuit_opened_at,
        circuit_opened_at_wall: circuit_opened_at_wall,
        consecutive_open_checks: consecutive_open_checks
    }

    if should_send_alert?(new_state, open_duration_ms) do
      send_alert(new_state, open_duration_ms)
      %State{new_state | last_alert_sent_at: now, last_alert_sent_at_wall: now_wall}
    else
      new_state
    end
  end

  defp handle_closed_circuit(state) do
    if state.circuit_opened_at do
      source_label = source_label(state.ingest_source)

      Logger.info(
        "[CircuitBreakerMonitor] #{source_label} circuit breaker has recovered to CLOSED state",
        recovery_after_checks: state.consecutive_open_checks
      )
    end

    # Reset state (preserve ingest_source)
    %State{
      circuit_opened_at: nil,
      circuit_opened_at_wall: nil,
      last_alert_sent_at: nil,
      last_alert_sent_at_wall: nil,
      consecutive_open_checks: 0,
      ingest_source: state.ingest_source
    }
  end

  defp should_send_alert?(state, open_duration_ms) do
    open_duration_ms >= @alert_threshold_ms and
      (state.last_alert_sent_at == nil or
         System.monotonic_time(:millisecond) - state.last_alert_sent_at >= @alert_threshold_ms)
  end

  defp send_alert(state, open_duration_ms) do
    open_duration_minutes = div(open_duration_ms, 60_000)
    source_label = source_label(state.ingest_source)

    Logger.error(
      "[CircuitBreakerMonitor] ALERT: #{source_label} circuit breaker has been open for #{open_duration_minutes} minutes!",
      consecutive_open_checks: state.consecutive_open_checks,
      action: "Manual intervention may be required"
    )

    :telemetry.execute(
      [:wanderer_kills, :circuit_breaker, :alert],
      %{duration_ms: open_duration_ms},
      %{
        consecutive_checks: state.consecutive_open_checks,
        ingest_source: state.ingest_source
      }
    )
  end

  defp source_label(:r2z2), do: "R2Z2"

  defp format_duration(ms) do
    seconds = div(ms, 1000)
    minutes = div(seconds, 60)
    hours = div(minutes, 60)

    cond do
      hours > 0 ->
        "#{hours}h #{rem(minutes, 60)}m"

      minutes > 0 ->
        "#{minutes}m #{rem(seconds, 60)}s"

      true ->
        "#{seconds}s"
    end
  end
end
