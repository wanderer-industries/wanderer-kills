defmodule WandererKills.Ingest.RedisQ do
  @moduledoc """
  Client for interacting with the zKillboard RedisQ API.

  • Idle (no kills):    poll every `:idle_interval_ms`
  • On kill (new):      poll again after `:fast_interval_ms`
  • On kill_older:      poll again after `:idle_interval_ms` (reset backoff)
  • On kill_skipped:    poll again after `:idle_interval_ms` (reset backoff)
  • On error:           exponential backoff up to `:max_backoff_ms`
  """

  use GenServer
  require Logger

  alias WandererKills.Core.EtsOwner
  alias WandererKills.Core.Support.Error
  alias WandererKills.Core.Support.Utils
  alias WandererKills.Domain.Killmail
  alias WandererKills.Http.Client, as: HttpClient
  alias WandererKills.Ingest.ESI.Client, as: EsiClient
  alias WandererKills.Ingest.Killmails.UnifiedProcessor
  alias WandererKills.Subs.Broadcaster
  alias WandererKills.Subs.SimpleSubscriptionManager, as: SubscriptionManager

  @user_agent "(wanderer-kills@proton.me; +https://github.com/wanderer-industries/wanderer-kills)"

  # Compile-time configuration
  @redisq_base_url Application.compile_env(
                     :wanderer_kills,
                     [:redisq, :base_url],
                     "https://zkillredisq.stream/listen.php"
                   )
  @fast_interval_ms Application.compile_env(:wanderer_kills, [:redisq, :fast_interval_ms], 1_000)
  @idle_interval_ms Application.compile_env(:wanderer_kills, [:redisq, :idle_interval_ms], 5_000)
  @initial_backoff_ms Application.compile_env(
                        :wanderer_kills,
                        [:redisq, :initial_backoff_ms],
                        1_000
                      )
  @max_backoff_ms Application.compile_env(:wanderer_kills, [:redisq, :max_backoff_ms], 30_000)
  @backoff_factor Application.compile_env(:wanderer_kills, [:redisq, :backoff_factor], 2)
  @task_timeout_ms Application.compile_env(:wanderer_kills, [:redisq, :task_timeout_ms], 10_000)

  # Circuit breaker configuration
  @max_consecutive_errors Application.compile_env(
                            :wanderer_kills,
                            [:redisq, :max_consecutive_errors],
                            10
                          )
  # 5 minutes
  @circuit_reset_timeout_ms Application.compile_env(
                              :wanderer_kills,
                              [:redisq, :circuit_reset_timeout_ms],
                              300_000
                            )

  defmodule State do
    @moduledoc false
    defstruct [
      :queue_id,
      :backoff_ms,
      :stats,
      :consecutive_errors,
      :circuit_state,
      :circuit_opened_at
    ]
  end

  #
  # Public API
  #

  @doc """
  Gets the base URL for RedisQ API calls.
  """
  @spec base_url() :: String.t()
  def base_url do
    @redisq_base_url
  end

  @doc """
  Starts the RedisQ worker as a GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    Logger.info("[RedisQ] Starting RedisQ worker")
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Force a synchronous poll & process. Returns one of:
    - `{:ok, :kill_received}`
    - `{:ok, :no_kills}`
    - `{:ok, :kill_older}`
    - `{:ok, :kill_skipped}`
    - `{:error, reason}`
  """
  @spec poll_and_process(keyword()) ::
          {:ok, :kill_received | :no_kills | :kill_older | :kill_skipped} | {:error, term()}
  def poll_and_process(opts \\ []) do
    GenServer.call(__MODULE__, {:poll_and_process, opts})
  end

  @doc """
  Gets current RedisQ statistics.
  """
  @spec get_stats() :: map()
  def get_stats do
    GenServer.call(__MODULE__, :get_stats)
  end

  @doc """
  Gets current circuit breaker status.
  """
  @spec get_circuit_status() :: {:ok, map()}
  def get_circuit_status do
    get_circuit_status(5000)
  end

  @doc """
  Gets current circuit breaker status with custom timeout.
  """
  @spec get_circuit_status(timeout()) :: {:ok, map()}
  def get_circuit_status(timeout) do
    GenServer.call(__MODULE__, :get_circuit_status, timeout)
  end

  @doc """
  Gets circuit breaker status from ETS without blocking.
  Returns cached status or default if not available.
  """
  @spec get_circuit_status_cached() :: {:ok, map()}
  def get_circuit_status_cached do
    case :ets.lookup(EtsOwner.wanderer_kills_stats_table(), :redisq_circuit_status) do
      [{:redisq_circuit_status, status}] ->
        {:ok, status}

      _ ->
        # Return default status if not in ETS
        {:ok,
         %{
           circuit_state: :unknown,
           consecutive_errors: 0,
           max_consecutive_errors: @max_consecutive_errors,
           circuit_opened_at: nil,
           circuit_reset_timeout_ms: @circuit_reset_timeout_ms,
           cached: true
         }}
    end
  end

  @doc """
  Starts listening to RedisQ killmail stream.
  """
  @spec start_listening() :: :ok | {:error, term()}
  def start_listening do
    url = "#{base_url()}?queueID=wanderer-kills"

    case HttpClient.get_redisq(url) do
      {:ok, %{body: body}} ->
        handle_response(body)

      {:error, reason} ->
        Logger.error("Failed to get RedisQ response: #{inspect(reason)}")
    end
  end

  #
  # Server Callbacks
  #

  @impl true
  def init(_opts) do
    queue_id = build_queue_id()
    initial_backoff = @initial_backoff_ms

    # Initialize statistics tracking
    stats = %{
      kills_received: 0,
      kills_older: 0,
      kills_skipped: 0,
      errors: 0,
      no_kills_count: 0,
      circuit_open_skips: 0,
      last_reset: DateTime.utc_now(),
      last_kill_received_at: nil,
      systems_active: MapSet.new(),
      # Cumulative stats that don't reset
      total_kills_received: 0,
      total_kills_older: 0,
      total_kills_skipped: 0,
      total_errors: 0,
      total_no_kills_count: 0,
      total_circuit_open_skips: 0
    }

    state = %State{
      queue_id: queue_id,
      backoff_ms: initial_backoff,
      stats: stats,
      consecutive_errors: 0,
      circuit_state: :closed,
      circuit_opened_at: nil
    }

    {:ok, state, {:continue, :start_polling}}
  end

  @impl true
  def handle_continue(:start_polling, state) do
    Logger.info("[RedisQ] Starting polling with queue ID: #{state.queue_id}")

    # Initialize circuit status in ETS
    update_circuit_status_ets(
      state.circuit_state,
      state.consecutive_errors,
      state.circuit_opened_at
    )

    # Schedule the very first poll after the idle interval
    schedule_poll(@idle_interval_ms)
    # Schedule the first summary log
    schedule_summary_log()
    {:noreply, state}
  end

  @impl true
  def handle_info(:poll_kills, %State{} = state) do
    # Check circuit breaker state
    case check_circuit_breaker(state) do
      {:ok, state} ->
        # Circuit is closed or half-open, proceed with polling
        Logger.debug("[RedisQ] Polling RedisQ (queue ID: #{state.queue_id})")
        result = do_poll(state.queue_id)

        # Update statistics based on result
        new_stats = update_stats(state.stats, result)

        # Update ETS immediately for real-time dashboard metrics
        update_ets_stats(new_stats)

        # Update error counter and circuit state
        {new_consecutive_errors, new_circuit_state, new_circuit_opened_at} =
          update_circuit_state(
            result,
            state.consecutive_errors,
            state.circuit_state,
            state.circuit_opened_at
          )

        {delay_ms, new_backoff} = next_schedule(result, state.backoff_ms)
        schedule_poll(delay_ms)

        # Update circuit breaker status in ETS for non-blocking access
        update_circuit_status_ets(
          new_circuit_state,
          new_consecutive_errors,
          new_circuit_opened_at
        )

        {:noreply,
         %State{
           state
           | backoff_ms: new_backoff,
             stats: new_stats,
             consecutive_errors: new_consecutive_errors,
             circuit_state: new_circuit_state,
             circuit_opened_at: new_circuit_opened_at
         }}

      {:circuit_open, state} ->
        # Circuit is open, skip polling and schedule retry
        Logger.warning("[RedisQ] Circuit breaker is OPEN - skipping poll")

        # Update stats to track circuit open skips
        new_stats = %{
          state.stats
          | circuit_open_skips: state.stats.circuit_open_skips + 1,
            total_circuit_open_skips: state.stats.total_circuit_open_skips + 1
        }

        # Update ETS immediately for real-time dashboard metrics
        update_ets_stats(new_stats)

        # Update circuit breaker status in ETS
        update_circuit_status_ets(
          state.circuit_state,
          state.consecutive_errors,
          state.circuit_opened_at
        )

        # Schedule a retry after circuit reset timeout
        schedule_poll(@circuit_reset_timeout_ms)

        {:noreply, %State{state | stats: new_stats}}
    end
  end

  @impl true
  def handle_info(:log_summary, %State{stats: stats} = state) do
    log_summary(stats)

    # Reset stats and schedule next summary
    reset_stats = %{
      stats
      | kills_received: 0,
        kills_older: 0,
        kills_skipped: 0,
        errors: 0,
        no_kills_count: 0,
        last_reset: DateTime.utc_now(),
        systems_active: MapSet.new()
        # Preserve last_kill_received_at - it should persist across resets
    }

    schedule_summary_log()
    {:noreply, %State{state | stats: reset_stats}}
  end

  @impl true
  def handle_info({:track_system, system_id}, %State{stats: stats} = state) do
    new_stats = track_system_activity(stats, system_id)
    {:noreply, %State{state | stats: new_stats}}
  end

  @impl true
  def handle_call({:poll_and_process, _opts}, _from, %State{queue_id: qid} = state) do
    Logger.debug("[RedisQ] Manual poll requested (queue ID: #{qid})")
    reply = do_poll(qid)
    {:reply, reply, state}
  end

  @impl true
  def handle_call(:get_stats, _from, state) do
    stats = %{
      # Use cumulative stats for the 5-minute report
      kills_processed: state.stats.total_kills_received,
      kills_older: state.stats.total_kills_older,
      kills_skipped: state.stats.total_kills_skipped,
      errors: state.stats.total_errors,
      no_kills_polls: state.stats.total_no_kills_count,
      active_systems: MapSet.size(state.stats.systems_active),
      total_polls:
        state.stats.total_kills_received + state.stats.total_kills_older +
          state.stats.total_kills_skipped + state.stats.total_no_kills_count +
          state.stats.total_errors,
      last_reset: state.stats.last_reset,
      last_kill_received_at: state.stats.last_kill_received_at
    }

    {:reply, {:ok, stats}, state}
  end

  @impl true
  def handle_call(:get_circuit_status, _from, state) do
    status = %{
      circuit_state: state.circuit_state,
      consecutive_errors: state.consecutive_errors,
      max_consecutive_errors: @max_consecutive_errors,
      circuit_opened_at: state.circuit_opened_at,
      circuit_reset_timeout_ms: @circuit_reset_timeout_ms
    }

    {:reply, {:ok, status}, state}
  end

  @impl true
  def terminate(_reason, _state), do: :ok

  #
  # Private Helpers
  #

  # Schedules the next :poll_kills message in `ms` milliseconds.
  defp schedule_poll(ms) do
    Process.send_after(self(), :poll_kills, ms)
  end

  # Schedules the next :log_summary message in 60 seconds.
  defp schedule_summary_log do
    Process.send_after(self(), :log_summary, 60_000)
  end

  # Updates statistics based on poll result
  defp update_stats(stats, {:ok, :kill_received}) do
    %{
      stats
      | kills_received: stats.kills_received + 1,
        total_kills_received: stats.total_kills_received + 1,
        last_kill_received_at: System.system_time(:second)
    }
  end

  defp update_stats(stats, {:ok, :kill_older}) do
    %{stats | kills_older: stats.kills_older + 1, total_kills_older: stats.total_kills_older + 1}
  end

  defp update_stats(stats, {:ok, :kill_skipped}) do
    %{
      stats
      | kills_skipped: stats.kills_skipped + 1,
        total_kills_skipped: stats.total_kills_skipped + 1
    }
  end

  defp update_stats(stats, {:ok, :no_kills}) do
    %{
      stats
      | no_kills_count: stats.no_kills_count + 1,
        total_no_kills_count: stats.total_no_kills_count + 1
    }
  end

  defp update_stats(stats, {:error, _reason}) do
    %{stats | errors: stats.errors + 1, total_errors: stats.total_errors + 1}
  end

  # Track active systems
  defp track_system_activity(stats, system_id) when is_integer(system_id) do
    %{stats | systems_active: MapSet.put(stats.systems_active, system_id)}
  end

  defp track_system_activity(stats, _), do: stats

  # Update ETS with current stats for real-time dashboard access
  defp update_ets_stats(stats) do
    if :ets.info(EtsOwner.wanderer_kills_stats_table()) != :undefined do
      :ets.insert(EtsOwner.wanderer_kills_stats_table(), {:redisq_stats, stats})
    end
  end

  # Update circuit breaker status in ETS for non-blocking access
  defp update_circuit_status_ets(circuit_state, consecutive_errors, circuit_opened_at) do
    if :ets.info(EtsOwner.wanderer_kills_stats_table()) != :undefined do
      status = %{
        circuit_state: circuit_state,
        consecutive_errors: consecutive_errors,
        max_consecutive_errors: @max_consecutive_errors,
        circuit_opened_at: circuit_opened_at,
        circuit_reset_timeout_ms: @circuit_reset_timeout_ms,
        last_updated: System.system_time(:millisecond)
      }

      :ets.insert(EtsOwner.wanderer_kills_stats_table(), {:redisq_circuit_status, status})
    end
  end

  # Log summary of activity over the past minute
  defp log_summary(stats) do
    duration = DateTime.diff(DateTime.utc_now(), stats.last_reset, :second)

    # Store stats in ETS for unified status reporter (this is now also done in real-time)
    update_ets_stats(stats)

    # Note: Summary logging now handled by UnifiedStatus module
    # Only log if there's significant error activity
    if stats.errors > 10 do
      Logger.warning(
        "[RedisQ] High error rate detected",
        redisq_errors: stats.errors,
        redisq_duration_s: duration
      )
    end
  end

  # Perform the actual HTTP GET + parsing and return one of:
  #   - {:ok, :kill_received}
  #   - {:ok, :no_kills}
  #   - {:ok, :kill_older}
  #   - {:ok, :kill_skipped}
  #   - {:error, reason}
  defp do_poll(queue_id) do
    # Use 10 second long-polling for more efficient operation
    url = "#{base_url()}?queueID=#{queue_id}&ttw=10"
    Logger.debug("[RedisQ] Starting poll request to: #{url}")

    headers = [{"user-agent", @user_agent}]
    start_time = System.monotonic_time(:millisecond)

    Logger.debug("[RedisQ] Making HTTP request...")
    result = HttpClient.get_redisq(url, headers)
    elapsed_ms = System.monotonic_time(:millisecond) - start_time

    Logger.debug("[RedisQ] HTTP request completed in #{elapsed_ms}ms")

    case result do
      # No package → no new kills
      {:ok, %{body: %{"package" => nil}}} ->
        Logger.debug("[RedisQ] No package received.")
        {:ok, :no_kills}

      # Standard format: package contains killID and zkb metadata
      # Full killmail data is fetched from ESI using the hash
      {:ok, %{body: %{"package" => %{"killID" => id, "zkb" => zkb}}}} ->
        process_kill_package(id, zkb, queue_id)

      # Anything else is unexpected
      {:ok, resp} ->
        Logger.warning("[RedisQ] Unexpected response shape: #{inspect(resp)}")

        {:error,
         Error.invalid_format_error("Unexpected RedisQ response format", %{response: resp})}

      {:error, reason} ->
        Logger.warning("[RedisQ] HTTP request failed: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Process a kill package from RedisQ.
  # Fetches full killmail data from ESI using the hash, then processes it.
  #
  # Returns one of:
  #   {:ok, :kill_received}   if successfully processed
  #   {:ok, :kill_older}      if killmail is older than cutoff
  #   {:ok, :kill_skipped}    if already ingested
  #   {:error, reason}        on failure
  defp process_kill_package(id, zkb, queue_id) do
    Logger.debug("[RedisQ] Processing kill package",
      kill_id: id,
      queue_id: queue_id,
      zkb_keys: Map.keys(zkb || %{})
    )

    task =
      Task.Supervisor.async(WandererKills.TaskSupervisor, fn ->
        fetch_and_process_killmail(id, zkb)
      end)

    task
    |> Task.await(@task_timeout_ms)
    |> case do
      {:ok, :kill_received} ->
        Logger.debug("[RedisQ] Successfully processed kill", kill_id: id)
        {:ok, :kill_received}

      {:ok, :kill_older} ->
        Logger.debug("[RedisQ] Kill ID=#{id} is older than cutoff → skipping.")
        {:ok, :kill_older}

      {:ok, :kill_skipped} ->
        Logger.debug("[RedisQ] Kill ID=#{id} already ingested → skipping.")
        {:ok, :kill_skipped}

      {:error, reason} ->
        Logger.error("[RedisQ] Kill #{id} processing failed: #{inspect(reason)}")
        {:error, reason}

      other ->
        Logger.error("[RedisQ] Unexpected task result for kill #{id}: #{inspect(other)}")

        {:error,
         Error.system_error(
           :unexpected_task_result,
           "Unexpected task result for kill processing",
           false,
           %{
             kill_id: id,
             result: other,
             queue_id: queue_id
           }
         )}
    end
  end

  # Fetch the full killmail from ESI and process it through the pipeline.
  defp fetch_and_process_killmail(id, zkb) do
    Logger.debug("[RedisQ] Fetching killmail from ESI", kill_id: id)

    case EsiClient.get_killmail_raw(id, zkb["hash"]) do
      {:ok, full_killmail} ->
        Logger.debug("[RedisQ] Fetched killmail from ESI, processing", kill_id: id)
        process_fetched_killmail(full_killmail, zkb)

      {:error, reason} ->
        Logger.warning("[RedisQ] ESI fetch failed for ID=#{id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Process a killmail that has been fetched from ESI.
  defp process_fetched_killmail(killmail, zkb) do
    cutoff = get_cutoff_time()
    merged = Map.merge(killmail, %{"zkb" => zkb})

    case UnifiedProcessor.process_killmail(merged, cutoff) do
      {:ok, :kill_older} ->
        {:ok, :kill_older}

      {:ok, enriched_killmail} ->
        broadcast_killmail_update_enriched(enriched_killmail)
        {:ok, :kill_received}

      {:error, reason} ->
        Logger.error("[RedisQ] Failed to process killmail: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # Decide the next polling interval and updated backoff based on the last result.
  # Returns: {next_delay_ms, updated_backoff_ms}
  defp next_schedule({:ok, :kill_received}, _old_backoff) do
    fast = @fast_interval_ms
    Logger.debug("[RedisQ] Kill received → scheduling next poll in #{fast}ms; resetting backoff.")
    {fast, @initial_backoff_ms}
  end

  defp next_schedule({:ok, :no_kills}, _old_backoff) do
    idle = @idle_interval_ms
    Logger.debug("[RedisQ] No kills → scheduling next poll in #{idle}ms; resetting backoff.")
    {idle, @initial_backoff_ms}
  end

  defp next_schedule({:ok, :kill_older}, _old_backoff) do
    idle = @idle_interval_ms

    Logger.debug(
      "[RedisQ] Older kill detected → scheduling next poll in #{idle}ms; resetting backoff."
    )

    {idle, @initial_backoff_ms}
  end

  defp next_schedule({:ok, :kill_skipped}, _old_backoff) do
    idle = @idle_interval_ms

    Logger.debug(
      "[RedisQ] Skipped kill detected → scheduling next poll in #{idle}ms; resetting backoff."
    )

    {idle, @initial_backoff_ms}
  end

  defp next_schedule({:error, reason}, old_backoff) do
    factor = @backoff_factor
    max_back = @max_backoff_ms
    next_back = min(old_backoff * factor, max_back)

    Logger.warning(
      "[RedisQ] Poll error, retrying",
      error_type: error_type(reason),
      backoff_ms: next_back,
      max_backoff_ms: max_back,
      reason: String.slice(format_error(reason), 0, 512)
    )

    {next_back, next_back}
  end

  defp error_type(%Error{type: type}), do: type
  defp error_type(_), do: :unknown

  defp format_error(%Error{} = error) do
    "#{error.domain}: #{error.type} - #{error.message}"
  end

  defp format_error(reason) do
    inspect(reason)
  end

  # Build a unique queue ID: "wanderer_kills_<16_char_string>"
  # Uses a mix of timestamp and random characters for uniqueness
  defp build_queue_id do
    # Generate 16 character random string using alphanumeric characters
    random_chars =
      for _ <- 1..16,
          into: "",
          do: <<Enum.random(~c"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")>>

    "wanderer_kills_#{random_chars}"
  end

  # Returns cutoff DateTime (e.g. "24 hours ago")
  defp get_cutoff_time do
    Utils.hours_ago(1)
  end

  defp handle_response(%{"package" => package}) do
    # Process the killmail package
    Logger.debug("Received killmail package: #{inspect(package)}")
  end

  defp handle_response(_) do
    # No package in response, continue listening
    start_listening()
  end

  # Broadcast killmail update to PubSub subscribers using enriched killmail
  defp broadcast_killmail_update_enriched(%Killmail{} = killmail) do
    system_id = killmail.system_id

    Logger.info("[RedisQ] Broadcasting kill #{killmail.killmail_id} to system #{system_id}")

    # Track system activity for statistics
    send(self(), {:track_system, system_id})

    # Broadcast detailed kill update - convert to map for compatibility
    killmail_map = Killmail.to_map(killmail)

    # Send to subscription workers
    SubscriptionManager.broadcast_killmail_update_async(system_id, [
      killmail_map
    ])

    # Also broadcast to PubSub topics for SSE and WebSocket channels
    Broadcaster.broadcast_killmail_update(system_id, [killmail_map])

    # Also broadcast kill count update (increment by 1)
    SubscriptionManager.broadcast_killmail_count_update_async(system_id, 1)
  end

  # Circuit breaker implementation
  defp check_circuit_breaker(%State{circuit_state: :open, circuit_opened_at: opened_at} = state) do
    # Check if enough time has passed to attempt reset
    elapsed_ms = System.monotonic_time(:millisecond) - opened_at

    if elapsed_ms >= @circuit_reset_timeout_ms do
      Logger.info(
        "[RedisQ] Circuit breaker timeout expired - attempting to reset to half-open state"
      )

      {:ok, %State{state | circuit_state: :half_open}}
    else
      {:circuit_open, state}
    end
  end

  defp check_circuit_breaker(state) do
    {:ok, state}
  end

  # Explicitly handle half-open failure: re-open immediately
  defp update_circuit_state({:error, _reason}, consecutive_errors, :half_open, _opened_at) do
    Logger.error("[RedisQ] Circuit breaker re-opening after failure in half-open state")
    {consecutive_errors + 1, :open, System.monotonic_time(:millisecond)}
  end

  defp update_circuit_state(
         {:error, _reason},
         consecutive_errors,
         circuit_state,
         circuit_opened_at
       ) do
    new_consecutive_errors = consecutive_errors + 1

    if new_consecutive_errors >= @max_consecutive_errors do
      case circuit_state do
        :open ->
          # Circuit already open, keep existing timestamp
          {new_consecutive_errors, :open, circuit_opened_at}

        _ ->
          # Transitioning to open state, set new timestamp
          Logger.error(
            "[RedisQ] Circuit breaker opening after #{new_consecutive_errors} consecutive errors"
          )

          {new_consecutive_errors, :open, System.monotonic_time(:millisecond)}
      end
    else
      {new_consecutive_errors, :closed, circuit_opened_at}
    end
  end

  defp update_circuit_state({:ok, _}, _consecutive_errors, circuit_state, _circuit_opened_at) do
    # Success - reset error counter
    if circuit_state == :half_open do
      Logger.info("[RedisQ] Circuit breaker reset to closed state after successful request")
    end

    {0, :closed, nil}
  end
end
