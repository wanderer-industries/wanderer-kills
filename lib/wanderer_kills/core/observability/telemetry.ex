defmodule WandererKills.Core.Observability.Telemetry do
  @moduledoc """
  Consolidated telemetry system for WandererKills.

  This module combines telemetry event handling, metrics aggregation, batch processing
  telemetry, and task tracking from telemetry_metrics.ex into a single cohesive system.
  It provides:

  - Event execution and handler management
  - Metrics aggregation and storage
  - Batch processing telemetry
  - Task event tracking and metrics
  - Centralized event logging

  ## Architecture

  The telemetry system uses:
  - ETS table for metrics storage
  - GenServer for metrics aggregation
  - Telemetry handlers for event processing
  - Task-specific event handlers for compatibility

  ## Events

  The system handles various event categories:
  - Cache events (hit, miss, error)
  - HTTP events (request lifecycle)
  - Fetch events (killmail, system)
  - Parser events (stored, skipped, failed)
  - WebSocket events (connections, subscriptions)
  - SSE events (connections, events)
  - Character/System subscription events
  - Batch processing events
  - Task lifecycle events (start, stop, error)
  """

  use GenServer
  require Logger

  @table :telemetry_metrics

  # Event definitions
  @batch_events [
    [:wanderer_kills, :batch_processor, :extract_characters],
    [:wanderer_kills, :batch_processor, :match_killmails],
    [:wanderer_kills, :batch_processor, :find_subscriptions],
    [:wanderer_kills, :batch_processor, :group_killmails],
    [:wanderer_kills, :character_cache, :hit],
    [:wanderer_kills, :character_cache, :miss],
    [:wanderer_kills, :character_cache, :batch]
  ]

  # ============================================================================
  # Client API
  # ============================================================================

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Execute a telemetry event with measurements and metadata.
  """
  def execute(event, measurements, metadata \\ %{}) do
    :telemetry.execute(event, measurements, metadata)
  end

  # ============================================================================
  # Helper Functions for Common Events
  # ============================================================================

  @doc "Executes HTTP request start telemetry."
  @spec http_request_start(String.t(), String.t()) :: :ok
  def http_request_start(method, url) do
    :telemetry.execute(
      [:wanderer_kills, :http, :request, :start],
      %{system_time: System.system_time(:native)},
      %{method: method, url: url, service: determine_service(url)}
    )
  end

  @doc "Executes HTTP request stop telemetry."
  @spec http_request_stop(String.t(), String.t(), integer(), integer()) :: :ok
  def http_request_stop(method, url, duration, status_code) do
    :telemetry.execute(
      [:wanderer_kills, :http, :request, :stop],
      %{duration: duration},
      %{method: method, url: url, status_code: status_code, service: determine_service(url)}
    )
  end

  @doc "Executes HTTP request error telemetry."
  @spec http_request_error(String.t(), String.t(), integer(), term()) :: :ok
  def http_request_error(method, url, duration, error) do
    :telemetry.execute(
      [:wanderer_kills, :http, :request, :stop],
      %{duration: duration},
      %{method: method, url: url, error: error, service: determine_service(url)}
    )
  end

  @doc "Executes fetch system start telemetry."
  @spec fetch_system_start(integer(), integer(), atom()) :: :ok
  def fetch_system_start(system_id, limit, source) do
    :telemetry.execute(
      [:wanderer_kills, :fetch, :system, :start],
      %{system_id: system_id, limit: limit},
      %{source: source}
    )
  end

  @doc "Executes fetch system complete telemetry."
  @spec fetch_system_complete(integer(), atom()) :: :ok
  def fetch_system_complete(system_id, result) do
    :telemetry.execute(
      [:wanderer_kills, :fetch, :system, :complete],
      %{system_id: system_id},
      %{result: result}
    )
  end

  @doc "Executes fetch system success telemetry."
  @spec fetch_system_success(integer(), integer(), atom()) :: :ok
  def fetch_system_success(system_id, count, source) do
    :telemetry.execute(
      [:wanderer_kills, :fetch, :system, :success],
      %{system_id: system_id, count: count},
      %{source: source}
    )
  end

  @doc "Executes fetch system error telemetry."
  @spec fetch_system_error(integer(), term(), atom()) :: :ok
  def fetch_system_error(system_id, error, source) do
    :telemetry.execute(
      [:wanderer_kills, :fetch, :system, :error],
      %{system_id: system_id},
      %{error: error, source: source}
    )
  end

  @doc "Cache hit event"
  @spec cache_hit(term()) :: :ok
  def cache_hit(key) do
    :telemetry.execute(
      [:wanderer_kills, :cache, :hit],
      %{count: 1},
      %{key: key}
    )
  end

  @doc "Cache miss event"
  @spec cache_miss(term()) :: :ok
  def cache_miss(key) do
    :telemetry.execute(
      [:wanderer_kills, :cache, :miss],
      %{count: 1},
      %{key: key}
    )
  end

  @doc "Cache error event"
  @spec cache_error(term(), term()) :: :ok
  def cache_error(key, error) do
    :telemetry.execute(
      [:wanderer_kills, :cache, :error],
      %{count: 1},
      %{key: key, error: error}
    )
  end

  @doc "Parser stored event"
  @spec parser_stored :: :ok
  def parser_stored do
    :telemetry.execute(
      [:wanderer_kills, :parser, :stored],
      %{count: 1},
      %{}
    )
  end

  @doc "Parser skipped event"
  @spec parser_skipped :: :ok
  def parser_skipped do
    :telemetry.execute(
      [:wanderer_kills, :parser, :skipped],
      %{count: 1},
      %{}
    )
  end

  @doc "Parser failed event"
  @spec parser_failed :: :ok
  def parser_failed do
    :telemetry.execute(
      [:wanderer_kills, :parser, :failed],
      %{count: 1},
      %{}
    )
  end

  @doc "WebSocket kills sent event"
  @spec websocket_kills_sent(atom(), integer()) :: :ok
  def websocket_kills_sent(type, count) do
    :telemetry.execute(
      [:wanderer_kills, :websocket, :kills_sent],
      %{count: count},
      %{type: type}
    )
  end

  @doc "WebSocket connection event"
  @spec websocket_connection(atom(), map()) :: :ok
  def websocket_connection(event, metadata) do
    :telemetry.execute(
      [:wanderer_kills, :websocket, :connection],
      %{count: 1},
      Map.put(metadata, :event, event)
    )
  end

  @doc "WebSocket subscription event"
  @spec websocket_subscription(atom(), String.t(), map()) :: :ok
  def websocket_subscription(event, subscription_id, metadata) do
    :telemetry.execute(
      [:wanderer_kills, :websocket, :subscription],
      %{count: 1},
      Map.merge(metadata, %{event: event, subscription_id: subscription_id})
    )
  end

  @doc "Rate limiter token consumption event"
  @spec rate_limiter_consumed(atom(), integer(), integer(), integer()) :: :ok
  def rate_limiter_consumed(service, tokens_consumed, tokens_remaining, status_code) do
    :telemetry.execute(
      [:wanderer_kills, :rate_limiter, :token_consumed],
      %{
        tokens_consumed: tokens_consumed,
        tokens_remaining: tokens_remaining
      },
      %{
        service: service,
        status_code: status_code
      }
    )
  end

  @doc "Rate limiter exceeded event"
  @spec rate_limiter_exceeded(atom(), float()) :: :ok
  def rate_limiter_exceeded(service, tokens_available) do
    :telemetry.execute(
      [:wanderer_kills, :rate_limiter, :rate_limited],
      %{tokens_available: tokens_available},
      %{service: service}
    )
  end

  @doc "Rate limiter reservation event"
  @spec rate_limiter_reserved(atom(), String.t(), float()) :: :ok
  def rate_limiter_reserved(service, reservation_id, reserved_tokens) do
    :telemetry.execute(
      [:wanderer_kills, :rate_limiter, :token_reserved],
      %{reserved_tokens: reserved_tokens},
      %{service: service, reservation_id: reservation_id}
    )
  end

  @doc "Character match event"
  @spec character_match(integer(), term(), integer()) :: :ok
  def character_match(duration, result, character_count) do
    :telemetry.execute(
      [:wanderer_kills, :character, :match],
      %{duration: duration, character_count: character_count},
      %{result: result}
    )
  end

  @doc "Character filter event"
  @spec character_filter(integer(), integer(), integer()) :: :ok
  def character_filter(duration, killmail_count, result_count) do
    :telemetry.execute(
      [:wanderer_kills, :character, :filter],
      %{duration: duration, killmail_count: killmail_count, result_count: result_count},
      %{}
    )
  end

  @doc "Character index event"
  @spec character_index(atom(), integer(), map()) :: :ok
  def character_index(operation, duration, metadata) do
    :telemetry.execute(
      [:wanderer_kills, :character, :index],
      %{duration: duration},
      Map.put(metadata, :operation, operation)
    )
  end

  @doc "Character cache event"
  @spec character_cache(atom(), term(), map()) :: :ok
  def character_cache(event, killmail_id, metadata) do
    :telemetry.execute(
      [:wanderer_kills, :character_cache, event],
      %{count: 1},
      Map.put(metadata, :killmail_id, killmail_id)
    )
  end

  @doc "System filter event"
  @spec system_filter(integer(), integer(), integer()) :: :ok
  def system_filter(duration, killmail_count, result_count) do
    :telemetry.execute(
      [:wanderer_kills, :system, :filter],
      %{duration: duration, killmail_count: killmail_count, result_count: result_count},
      %{}
    )
  end

  @doc "System index event"
  @spec system_index(atom(), integer(), map()) :: :ok
  def system_index(operation, duration, metadata) do
    :telemetry.execute(
      [:wanderer_kills, :system, :index],
      %{duration: duration},
      Map.put(metadata, :operation, operation)
    )
  end

  @doc "ZKB format event"
  @spec zkb_format(atom(), map()) :: :ok
  def zkb_format(format_type, metadata) do
    :telemetry.execute(
      [:wanderer_kills, :zkb, :format],
      %{count: 1},
      Map.put(metadata, :format_type, format_type)
    )
  end

  # Helper to determine service from URL
  defp determine_service(url) when is_binary(url) do
    url_lower = String.downcase(url)

    cond do
      String.contains?(url_lower, "zkillboard.com") -> :zkillboard
      String.contains?(url_lower, "esi.evetech.net") -> :esi
      true -> :unknown
    end
  end

  defp determine_service(_), do: :unknown

  @doc """
  Execute a telemetry span (start/stop events).
  """
  def span(event, metadata, fun) do
    :telemetry.span(event, metadata, fun)
  end

  @doc """
  Attach batch processing telemetry handlers.
  """
  def attach_batch_handlers do
    :telemetry.attach_many(
      "wanderer-kills-batch-telemetry",
      @batch_events,
      &__MODULE__.handle_batch_event/4,
      nil
    )
  end

  @doc """
  Attach a single telemetry handler.
  """
  def attach(handler_id, events, handler_fun, config \\ nil) do
    :telemetry.attach(handler_id, events, handler_fun, config)
  end

  @doc """
  Attach multiple telemetry handlers.
  """
  def attach_many(handler_id, events, handler_fun, config \\ nil) do
    :telemetry.attach_many(handler_id, events, handler_fun, config)
  end

  @doc """
  Detach a telemetry handler.
  """
  def detach(handler_id) do
    :telemetry.detach(handler_id)
  end

  @doc """
  Get current metric values.
  """
  def get_metrics do
    case :ets.info(@table) do
      :undefined ->
        %{}

      _ ->
        :ets.tab2list(@table)
        |> Enum.into(%{})
    end
  end

  @doc """
  Get a specific metric value.
  """
  def get_metric(key, default \\ 0) do
    case :ets.lookup(@table, key) do
      [{^key, value}] -> value
      [] -> default
    end
  end

  @doc """
  Reset all metrics to zero.
  """
  def reset_metrics do
    GenServer.call(__MODULE__, :reset_metrics)
  end

  @doc """
  Reset all metrics to zero with task-specific initialization.
  Provides compatibility with telemetry_metrics.ex.
  """
  def reset_metrics_with_tasks do
    reset_metrics()

    # Initialize task-specific counters
    set_gauge(:tasks_started, 0)
    set_gauge(:tasks_completed, 0)
    set_gauge(:tasks_failed, 0)
    set_gauge(:preload_tasks_started, 0)
    set_gauge(:preload_tasks_completed, 0)
    set_gauge(:preload_tasks_failed, 0)
    set_gauge(:webhook_tasks_started, 0)
    set_gauge(:webhook_tasks_completed, 0)
    set_gauge(:webhook_tasks_failed, 0)
    set_gauge(:webhooks_sent, 0)
    set_gauge(:webhooks_failed, 0)
  end

  @doc """
  Increment a counter metric.
  """
  def increment_counter(key, value \\ 1) do
    GenServer.cast(__MODULE__, {:increment, key, value})
  end

  @doc """
  Set a gauge metric.
  """
  def set_gauge(key, value) do
    GenServer.cast(__MODULE__, {:set, key, value})
  end

  @doc """
  Record a timing metric.
  """
  def record_timing(key, duration) do
    GenServer.cast(__MODULE__, {:timing, key, duration})
  end

  # ============================================================================
  # GenServer Callbacks
  # ============================================================================

  @impl true
  def init(_opts) do
    # Create ETS table for metrics
    :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])

    # Attach telemetry handlers
    attach_default_handlers()
    attach_task_handlers()

    {:ok, %{}}
  end

  @impl true
  def handle_call(:reset_metrics, _from, state) do
    :ets.delete_all_objects(@table)
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:increment, key, value}, state) do
    :ets.update_counter(@table, key, value, {key, 0})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:set, key, value}, state) do
    :ets.insert(@table, {key, value})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:timing, key, duration}, state) do
    # Store latest timing and update average
    :ets.insert(@table, {"#{key}_latest", duration})

    # Update running average
    case :ets.lookup(@table, "#{key}_avg") do
      [] ->
        :ets.insert(@table, {"#{key}_avg", duration})
        :ets.insert(@table, {"#{key}_count", 1})

      [{_, avg}] ->
        [{_, count}] = :ets.lookup(@table, "#{key}_count")
        new_count = count + 1
        new_avg = (avg * count + duration) / new_count
        :ets.insert(@table, {"#{key}_avg", new_avg})
        :ets.insert(@table, {"#{key}_count", new_count})
    end

    {:noreply, state}
  end

  # ============================================================================
  # Event Handlers
  # ============================================================================

  defp attach_default_handlers do
    # Attach metrics collection handlers - using correct arity
    attach_many(
      "wanderer-kills-metrics",
      [
        [:wanderer_kills, :cache, :hit],
        [:wanderer_kills, :cache, :miss],
        [:wanderer_kills, :http, :request, :stop],
        [:wanderer_kills, :fetch, :killmail, :success],
        [:wanderer_kills, :fetch, :killmail, :error],
        [:wanderer_kills, :parser, :stored],
        [:wanderer_kills, :parser, :skipped],
        [:wanderer_kills, :websocket, :kills_sent],
        [:wanderer_kills, :sse, :event, :sent],
        [:wanderer_kills, :rate_limiter, :token_consumed],
        [:wanderer_kills, :rate_limiter, :rate_limited],
        [:wanderer_kills, :rate_limiter, :token_reserved]
      ],
      &__MODULE__.handle_metric_event/4,
      nil
    )
  end

  @doc """
  Attach task event handlers for compatibility with telemetry_metrics.ex.
  """
  def attach_task_handlers do
    handler_id = "telemetry-task-handler"

    # Detach existing handler if it exists
    try do
      :telemetry.detach(handler_id)
    rescue
      _ -> :ok
    end

    # Attach handler for task events - using correct arity
    attach_many(
      handler_id,
      [
        [:wanderer_kills, :task, :start],
        [:wanderer_kills, :task, :stop],
        [:wanderer_kills, :task, :error]
      ],
      &__MODULE__.handle_task_event/4,
      nil
    )
  end

  def handle_metric_event(event, measurements, metadata, config) do
    case event do
      [:wanderer_kills, :cache | _rest] = cache_event ->
        handle_cache_event(cache_event, measurements, metadata, config)

      [:wanderer_kills, :http | _rest] = http_event ->
        handle_http_event(http_event, measurements, metadata, config)

      [:wanderer_kills, :fetch | _rest] = fetch_event ->
        handle_fetch_event(fetch_event, measurements, metadata, config)

      [:wanderer_kills, :parser | _rest] = parser_event ->
        handle_parser_event(parser_event, measurements, metadata, config)

      [:wanderer_kills, :websocket | _rest] = ws_event ->
        handle_websocket_event(ws_event, measurements, metadata, config)

      [:wanderer_kills, :sse | _rest] = sse_event ->
        handle_sse_event(sse_event, measurements, metadata, config)

      [:wanderer_kills, :rate_limiter | _rest] = rl_event ->
        handle_rate_limiter_event(rl_event, measurements, metadata, config)

      _ ->
        :ok
    end
  end

  defp handle_cache_event(event, _measurements, _metadata, _config) do
    case event do
      [:wanderer_kills, :cache, :hit] ->
        increment_counter(:cache_hits)

      [:wanderer_kills, :cache, :miss] ->
        increment_counter(:cache_misses)

      _ ->
        :ok
    end
  end

  defp handle_http_event(event, measurements, _metadata, _config) do
    case event do
      [:wanderer_kills, :http, :request, :stop] ->
        if duration = measurements[:duration] do
          duration_ms = System.convert_time_unit(duration, :native, :millisecond)
          record_timing(:http_request_duration, duration_ms)
        end

      _ ->
        :ok
    end
  end

  defp handle_fetch_event(event, _measurements, _metadata, _config) do
    case event do
      [:wanderer_kills, :fetch, :killmail, :success] ->
        increment_counter(:killmails_fetched)

      [:wanderer_kills, :fetch, :killmail, :error] ->
        increment_counter(:killmail_fetch_errors)

      _ ->
        :ok
    end
  end

  defp handle_parser_event(event, measurements, _metadata, _config) do
    case event do
      [:wanderer_kills, :parser, :stored] ->
        if count = measurements[:count] do
          increment_counter(:killmails_stored, count)
        end

      [:wanderer_kills, :parser, :skipped] ->
        if count = measurements[:count] do
          increment_counter(:killmails_skipped, count)
        end

      _ ->
        :ok
    end
  end

  defp handle_websocket_event(event, measurements, _metadata, _config) do
    case event do
      [:wanderer_kills, :websocket, :kills_sent] ->
        if count = measurements[:count] do
          increment_counter(:websocket_kills_sent, count)
        end

      _ ->
        :ok
    end
  end

  defp handle_sse_event(event, _measurements, _metadata, _config) do
    case event do
      [:wanderer_kills, :sse, :event, :sent] ->
        increment_counter(:sse_events_sent)

      _ ->
        :ok
    end
  end

  defp handle_rate_limiter_event(event, measurements, metadata, _config) do
    case event do
      [:wanderer_kills, :rate_limiter, :token_consumed] ->
        service = metadata[:service]
        status_code = metadata[:status_code]
        tokens_consumed = measurements[:tokens_consumed]

        # Track tokens consumed by service and status code
        increment_counter("rate_limiter_#{service}_tokens_consumed", tokens_consumed)
        increment_counter("rate_limiter_#{service}_#{status_code}_requests")

        # Update remaining tokens gauge
        if tokens_remaining = measurements[:tokens_remaining] do
          set_gauge("rate_limiter_#{service}_tokens_remaining", tokens_remaining)
        end

      [:wanderer_kills, :rate_limiter, :rate_limited] ->
        service = metadata[:service]
        increment_counter("rate_limiter_#{service}_violations")

      [:wanderer_kills, :rate_limiter, :token_reserved] ->
        service = metadata[:service]
        increment_counter("rate_limiter_#{service}_reservations")

      _ ->
        :ok
    end
  end

  # Batch event handlers
  def handle_batch_event(
        [:wanderer_kills, :batch_processor, :extract_characters],
        measurements,
        metadata,
        _config
      ) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    chars_per_killmail =
      if measurements.killmail_count > 0 do
        measurements.character_count / measurements.killmail_count
      else
        0.0
      end

    Logger.info("Batch character extraction completed",
      duration_ms: duration_ms,
      killmail_count: measurements.killmail_count,
      character_count: measurements.character_count,
      avg_chars_per_killmail: Float.round(chars_per_killmail, 2),
      operation: metadata.operation
    )

    record_timing(:batch_extract_characters, duration_ms)
  end

  def handle_batch_event(
        [:wanderer_kills, :batch_processor, :match_killmails],
        measurements,
        metadata,
        _config
      ) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    match_rate =
      if measurements.total > 0, do: measurements.matched / measurements.total * 100, else: 0

    Logger.info("Batch killmail matching completed",
      duration_ms: duration_ms,
      total_killmails: measurements.total,
      matched_killmails: measurements.matched,
      match_rate: Float.round(match_rate, 2),
      operation: metadata.operation
    )

    record_timing(:batch_match_killmails, duration_ms)
    increment_counter(:killmails_matched, measurements.matched)
  end

  def handle_batch_event(
        [:wanderer_kills, :batch_processor, :find_subscriptions],
        measurements,
        metadata,
        _config
      ) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.debug("Batch subscription lookup completed",
      duration_ms: duration_ms,
      character_count: measurements.character_count,
      subscription_count: measurements.subscription_count,
      operation: metadata.operation
    )

    record_timing(:batch_find_subscriptions, duration_ms)
  end

  def handle_batch_event(
        [:wanderer_kills, :batch_processor, :group_killmails],
        measurements,
        metadata,
        _config
      ) do
    duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)

    Logger.info("Batch killmail grouping completed",
      duration_ms: duration_ms,
      killmail_count: measurements.killmail_count,
      subscription_count: measurements.subscription_count,
      operation: metadata.operation
    )

    record_timing(:batch_group_killmails, duration_ms)
  end

  def handle_batch_event(
        [:wanderer_kills, :character_cache, :hit],
        measurements,
        _metadata,
        _config
      ) do
    increment_counter(:character_cache_hits, measurements.count || 1)
  end

  def handle_batch_event(
        [:wanderer_kills, :character_cache, :miss],
        measurements,
        _metadata,
        _config
      ) do
    increment_counter(:character_cache_misses, measurements.count || 1)
  end

  def handle_batch_event(
        [:wanderer_kills, :character_cache, :batch],
        measurements,
        _metadata,
        _config
      ) do
    record_timing(:character_cache_batch_duration, measurements.duration)
    increment_counter(:character_cache_batch_operations)
  end

  def handle_batch_event(_event, _measurements, _metadata, _config) do
    # Ignore other events
    :ok
  end

  # Task event handlers (from telemetry_metrics.ex)
  def handle_task_event(event, _measurements, metadata, _config) do
    task_name = Map.get(metadata, :task_name, "")
    task_type = determine_task_type(task_name)

    case event do
      [:wanderer_kills, :task, :start] ->
        handle_task_start(task_type)

      [:wanderer_kills, :task, :stop] ->
        handle_task_stop(task_type)

      [:wanderer_kills, :task, :error] ->
        handle_task_error(task_type)
    end
  end

  defp determine_task_type(task_name) do
    cond do
      String.contains?(task_name, "preload") -> :preload
      String.contains?(task_name, "webhook") -> :webhook
      true -> :generic
    end
  end

  defp handle_task_start(task_type) do
    increment_counter(:tasks_started)

    case task_type do
      :preload -> increment_counter(:preload_tasks_started)
      :webhook -> increment_counter(:webhook_tasks_started)
      :generic -> :ok
    end
  end

  defp handle_task_stop(task_type) do
    increment_counter(:tasks_completed)

    case task_type do
      :preload ->
        increment_counter(:preload_tasks_completed)

      :webhook ->
        increment_counter(:webhook_tasks_completed)
        increment_counter(:webhooks_sent)

      :generic ->
        :ok
    end
  end

  defp handle_task_error(task_type) do
    increment_counter(:tasks_failed)

    case task_type do
      :preload ->
        increment_counter(:preload_tasks_failed)

      :webhook ->
        increment_counter(:webhook_tasks_failed)
        increment_counter(:webhooks_failed)

      :generic ->
        :ok
    end
  end
end
