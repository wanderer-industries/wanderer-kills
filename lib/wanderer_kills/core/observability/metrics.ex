defmodule WandererKills.Core.Observability.Metrics do
  @moduledoc """
  Consolidated metrics collection and management for WandererKills.

  This module consolidates all metric collection, aggregation, and reporting
  functionality from api_tracker.ex, websocket_stats.ex, statistics.ex, and
  the original metrics.ex. It provides a unified interface for:

  - API request tracking (zkillboard, ESI)
  - WebSocket connection and subscription metrics
  - Cache operation metrics
  - Killmail processing metrics
  - System resource metrics
  - Statistical calculations and aggregations

  All metrics are emitted as telemetry events for easy integration with
  monitoring tools like Prometheus, StatsD, or custom reporters.

  ## Usage

  ```elixir
  # API tracking
  Metrics.track_api_request(:zkillboard, endpoint: "/api/kills/", duration_ms: 45, status_code: 200)

  # WebSocket events
  Metrics.track_websocket_connection(:connected)
  Metrics.increment_kills_sent(:realtime, 5)

  # Cache operations
  Metrics.record_cache_operation(:wanderer_cache, :hit)

  # Get current metrics
  {:ok, metrics} = Metrics.get_all_metrics()
  ```
  """

  use GenServer
  require Logger

  alias WandererKills.Core.EtsOwner
  alias WandererKills.Core.Observability.Telemetry
  alias WandererKills.Core.Support.Utils

  # Metric types
  @type metric_name :: atom()
  @type metric_value :: number() | map()
  @type metric_metadata :: map()
  @type service :: :zkillboard | :esi
  @type cache_operation :: :hit | :miss | :put | :eviction | :expired
  @type killmail_result :: :stored | :skipped | :failed | :enriched | :validated

  @type state :: %{
          start_time: DateTime.t(),
          metrics: map(),
          counters: map(),
          gauges: map(),
          histograms: map(),
          # API tracking state (from api_tracker.ex)
          api_metrics: map(),
          # WebSocket state (from websocket_stats.ex)
          websocket_stats: map()
        }

  # Configuration
  @table_name :consolidated_metrics
  @window_size_ms :timer.minutes(5)
  @cleanup_interval_ms :timer.minutes(1)
  @stats_summary_interval :timer.minutes(5)
  @health_check_interval :timer.minutes(1)
  @max_metric_atoms 1000

  # API services we track
  @services [:zkillboard, :esi]

  # Allowed metric patterns to prevent atom table exhaustion
  @metric_patterns [
    # HTTP metrics
    ~r/^http_(zkillboard|esi)_requests$/,
    ~r/^http_(zkillboard|esi)_(get|post|put|delete)$/,
    ~r/^http_(zkillboard|esi)_status_[1-5]xx$/,
    ~r/^http_(zkillboard|esi)_duration_ms$/,
    # Cache metrics
    ~r/^cache\.[a-zA-Z_]+\.(hit|miss|put|eviction|expired|total)$/,
    # Killmail metrics
    ~r/^killmail\.(stored|skipped|failed|enriched|validated)$/,
    # System metrics
    ~r/^system\.[a-zA-Z_]+$/,
    # Rate metrics
    ~r/^[a-zA-Z_]+_per_second$/
  ]

  # ============================================================================
  # Client API
  # ============================================================================

  @doc """
  Starts the consolidated metrics GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # ============================================================================
  # API Tracking Functions (from api_tracker.ex)
  # ============================================================================

  @doc """
  Records an API request.
  """
  @spec track_api_request(service(), keyword()) :: :ok
  def track_api_request(service, opts) when service in @services do
    metric = %{
      timestamp: System.monotonic_time(:millisecond),
      service: service,
      endpoint: Keyword.get(opts, :endpoint),
      duration_ms: Keyword.get(opts, :duration_ms, 0),
      status_code: Keyword.get(opts, :status_code),
      error: Keyword.get(opts, :error, false)
    }

    GenServer.cast(__MODULE__, {:track_api_request, metric})
  end

  @doc """
  Gets current API statistics for all services.
  """
  @spec get_api_stats() :: map()
  def get_api_stats do
    GenServer.call(__MODULE__, :get_api_stats)
  end

  @doc """
  Gets statistics for a specific API service.
  """
  @spec get_api_service_stats(service()) :: map()
  def get_api_service_stats(service) when service in @services do
    GenServer.call(__MODULE__, {:get_api_service_stats, service})
  end

  # ============================================================================
  # WebSocket Functions (from websocket_stats.ex)
  # ============================================================================

  @doc """
  Increments the count of kills sent to WebSocket clients.
  """
  @spec increment_kills_sent(:realtime | :preload, pos_integer()) :: :ok
  def increment_kills_sent(type, count \\ 1)
      when type in [:realtime, :preload] and is_integer(count) and count > 0 do
    Telemetry.websocket_kills_sent(type, count)
    GenServer.cast(__MODULE__, {:increment_kills_sent, type, count})
  end

  @doc """
  Tracks WebSocket connection events.
  """
  @spec track_websocket_connection(:connected | :disconnected, map()) :: :ok
  def track_websocket_connection(event, metadata \\ %{})
      when event in [:connected, :disconnected] do
    Telemetry.websocket_connection(event, metadata)
    GenServer.cast(__MODULE__, {:track_websocket_connection, event, metadata})
  end

  @doc """
  Tracks WebSocket subscription changes.
  """
  @spec track_websocket_subscription(:added | :updated | :removed, non_neg_integer(), map()) ::
          :ok
  def track_websocket_subscription(event, system_count, metadata \\ %{})
      when event in [:added, :updated, :removed] and is_integer(system_count) do
    character_count = Map.get(metadata, :character_count, 0)
    subscription_id = Map.get(metadata, :subscription_id, "unknown")

    Telemetry.websocket_subscription(
      event,
      subscription_id,
      Map.put(metadata, :system_count, system_count)
    )

    GenServer.cast(
      __MODULE__,
      {:track_websocket_subscription, event, system_count, character_count, metadata}
    )
  end

  @doc """
  Gets current WebSocket statistics.
  """
  @spec get_websocket_stats() :: {:ok, map()} | {:error, term()}
  def get_websocket_stats do
    GenServer.call(__MODULE__, :get_websocket_stats)
  end

  @doc """
  Resets WebSocket statistics counters.
  """
  @spec reset_websocket_stats() :: :ok
  def reset_websocket_stats do
    GenServer.call(__MODULE__, :reset_websocket_stats)
  end

  @doc """
  Records an HTTP request metric.

  ## Parameters
  - `service` - The service making the request (:esi, :zkb, etc.)
  - `method` - HTTP method (:get, :post, etc.)
  - `status_code` - HTTP status code
  - `duration_ms` - Request duration in milliseconds
  - `metadata` - Additional metadata
  """
  @spec record_http_request(atom(), atom(), integer(), number(), map()) :: :ok
  def record_http_request(service, method, status_code, duration_ms, metadata \\ %{}) do
    GenServer.cast(
      __MODULE__,
      {:record_http, service, method, status_code, duration_ms, metadata}
    )
  end

  @doc """
  Records a cache operation metric.

  ## Parameters
  - `cache_name` - Name of the cache
  - `operation` - Type of operation (:hit, :miss, :put, :eviction)
  - `metadata` - Additional metadata
  """
  @spec record_cache_operation(atom(), cache_operation(), map()) :: :ok
  def record_cache_operation(cache_name, operation, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:record_cache, cache_name, operation, metadata})
  end

  @doc """
  Records a killmail processing metric.

  ## Parameters
  - `result` - Processing result (:stored, :skipped, :failed, etc.)
  - `killmail_id` - The killmail ID
  - `metadata` - Additional metadata
  """
  @spec record_killmail_processed(killmail_result(), integer(), map()) :: :ok
  def record_killmail_processed(result, killmail_id, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:record_killmail, result, killmail_id, metadata})
  end

  @doc """
  Records a WebSocket event metric.

  ## Parameters
  - `event` - Event type (:connection, :subscription, :kills_sent, etc.)
  - `value` - Numeric value (count, duration, etc.)
  - `metadata` - Additional metadata
  """
  @spec record_websocket_event(atom(), number(), map()) :: :ok
  def record_websocket_event(event, value, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:record_websocket, event, value, metadata})
  end

  # ============================================================================
  # Statistical Functions (from statistics.ex)
  # ============================================================================

  @doc """
  Calculates a rate per specified time period.
  """
  @spec calculate_rate(non_neg_integer(), number(), keyword()) :: float()
  def calculate_rate(count, duration_seconds, opts \\ [])
      when is_integer(count) and is_number(duration_seconds) and duration_seconds > 0 do
    period = Keyword.get(opts, :period, :minute)
    precision = Keyword.get(opts, :precision, 2)

    period_multiplier =
      case period do
        :second -> 1
        :minute -> 60
        :hour -> 3600
        :day -> 86_400
      end

    rate = count * period_multiplier / duration_seconds
    Float.round(rate, precision)
  end

  @doc """
  Calculates a percentage with proper handling of edge cases.
  """
  @spec calculate_percentage(number(), number(), non_neg_integer()) :: float()
  def calculate_percentage(numerator, denominator, precision \\ 2)
      when is_number(numerator) and is_number(denominator) do
    case denominator do
      0 -> 0.0
      _ -> Float.round(numerator / denominator * 100, precision)
    end
  end

  @doc """
  Calculates hit rate percentage from hits and total operations.
  """
  @spec calculate_hit_rate(non_neg_integer(), non_neg_integer()) :: float()
  def calculate_hit_rate(hits, total) do
    calculate_percentage(hits, total)
  end

  @doc """
  Calculates success rate from successful and total operations.
  """
  @spec calculate_success_rate(non_neg_integer(), non_neg_integer()) :: float()
  def calculate_success_rate(successful, total) do
    calculate_percentage(successful, total)
  end

  @doc """
  Records a system resource metric.

  ## Parameters
  - `resource` - Resource type (:memory, :cpu, :processes, etc.)
  - `value` - Metric value
  - `metadata` - Additional metadata
  """
  @spec record_system_metric(atom(), number(), map()) :: :ok
  def record_system_metric(resource, value, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:record_system, resource, value, metadata})
  end

  @doc """
  Updates a gauge metric.

  ## Parameters
  - `name` - Gauge name
  - `value` - New value
  """
  @spec set_gauge(atom(), number()) :: :ok
  def set_gauge(name, value) when is_number(value) do
    GenServer.cast(__MODULE__, {:set_gauge, name, value})
  end

  @doc """
  Increments a counter metric.

  ## Parameters
  - `name` - Counter name
  - `amount` - Amount to increment (default: 1)
  """
  @spec increment_counter(atom(), number()) :: :ok
  def increment_counter(name, amount \\ 1) when is_number(amount) do
    GenServer.cast(__MODULE__, {:increment_counter, name, amount})
  end

  @doc """
  Records a value in a histogram.

  ## Parameters
  - `name` - Histogram name
  - `value` - Value to record
  """
  @spec record_histogram(atom(), number()) :: :ok
  def record_histogram(name, value) when is_number(value) do
    GenServer.cast(__MODULE__, {:record_histogram, name, value})
  end

  @doc """
  Gets all current metrics.

  ## Returns
  - `{:ok, metrics}` - All metrics as a map
  """
  @spec get_all_metrics() :: {:ok, map()}
  def get_all_metrics do
    GenServer.call(__MODULE__, :get_all_metrics)
  end

  @doc """
  Gets metrics for a specific service.

  ## Parameters
  - `service` - Service name

  ## Returns
  - `{:ok, metrics}` - Service-specific metrics
  """
  @spec get_service_metrics(service()) :: {:ok, map()}
  def get_service_metrics(service) do
    GenServer.call(__MODULE__, {:get_service_metrics, service})
  end

  @doc """
  Resets all metrics.
  """
  @spec reset_metrics() :: :ok
  def reset_metrics do
    GenServer.call(__MODULE__, :reset_metrics)
  end

  # ============================================================================
  # Convenience Functions for Common Metrics
  # ============================================================================

  @doc """
  Records a successful operation.
  """
  @spec record_success(service(), atom()) :: :ok
  def record_success(service, operation) do
    case build_metric_name(service, operation, "success") do
      {:ok, success_name} -> increment_counter(success_name)
      {:error, reason} -> Logger.warning("Failed to build success metric name", reason: reason)
    end

    case build_metric_name(service, operation, "total") do
      {:ok, total_name} -> increment_counter(total_name)
      {:error, reason} -> Logger.warning("Failed to build total metric name", reason: reason)
    end
  end

  @doc """
  Records a failed operation.
  """
  @spec record_failure(service(), atom(), atom()) :: :ok
  def record_failure(service, operation, reason) do
    case build_metric_name(service, operation, "failure") do
      {:ok, failure_name} ->
        increment_counter(failure_name)

      {:error, error_reason} ->
        Logger.warning("Failed to build failure metric name", reason: error_reason)
    end

    case build_metric_name(service, operation, "failure.#{reason}") do
      {:ok, failure_reason_name} ->
        increment_counter(failure_reason_name)

      {:error, error_reason} ->
        Logger.warning("Failed to build failure reason metric name", reason: error_reason)
    end

    case build_metric_name(service, operation, "total") do
      {:ok, total_name} ->
        increment_counter(total_name)

      {:error, error_reason} ->
        Logger.warning("Failed to build total metric name", reason: error_reason)
    end
  end

  @doc """
  Records operation duration.
  """
  @spec record_duration(service(), atom(), number()) :: :ok
  def record_duration(service, operation, duration_ms) do
    case build_metric_name(service, operation, "duration_ms") do
      {:ok, duration_name} -> record_histogram(duration_name, duration_ms)
      {:error, reason} -> Logger.warning("Failed to build duration metric name", reason: reason)
    end
  end

  # Helper function to safely build metric names
  defp build_metric_name(service, operation, suffix)
       when is_atom(service) and is_atom(operation) do
    metric_string = "#{service}_#{operation}_#{suffix}"

    case safe_metric_atom(metric_string) do
      :invalid_metric -> {:error, :invalid_metric_name}
      atom_name -> {:ok, atom_name}
    end
  end

  # ============================================================================
  # Parser Compatibility Functions
  # ============================================================================

  @doc """
  Increments the count of successfully stored killmails.
  """
  @spec increment_stored() :: :ok
  def increment_stored do
    increment_counter(:killmail_stored)
    Telemetry.parser_stored()
  end

  @doc """
  Increments the count of skipped killmails.
  """
  @spec increment_skipped() :: :ok
  def increment_skipped do
    increment_counter(:killmail_skipped)
    Telemetry.parser_skipped()
  end

  @doc """
  Increments the count of failed killmails.
  """
  @spec increment_failed() :: :ok
  def increment_failed do
    increment_counter(:killmail_failed)
    Telemetry.parser_failed()
  end

  # ============================================================================
  # GenServer Callbacks
  # ============================================================================

  @impl true
  def init(opts) do
    Logger.info("[Metrics] Starting consolidated metrics collection")

    # Create ETS table for API metrics
    :ets.new(@table_name, [:set, :public, :named_table, {:write_concurrency, true}])

    # Schedule periodic cleanup and health checks
    if !Keyword.get(opts, :disable_periodic_cleanup, false) do
      schedule_cleanup()
    end

    if !Keyword.get(opts, :disable_periodic_summary, false) do
      schedule_stats_summary()
    end

    if !Keyword.get(opts, :disable_health_check, false) do
      schedule_health_check()
    end

    # Attach telemetry handlers
    schedule_telemetry_attachment()

    state = %{
      start_time: DateTime.utc_now(),
      metrics: %{},
      counters: %{},
      gauges: %{},
      histograms: %{},
      # API tracking state
      api_metrics: %{},
      # WebSocket state
      websocket_stats: %{
        kills_sent: %{
          realtime: 0,
          preload: 0
        },
        connections: %{
          total_connected: 0,
          total_disconnected: 0,
          active: 0
        },
        subscriptions: %{
          total_added: 0,
          total_removed: 0,
          active: 0,
          total_systems: 0,
          total_characters: 0
        },
        rates: %{
          last_measured: DateTime.utc_now(),
          kills_per_minute: 0.0,
          connections_per_minute: 0.0
        },
        started_at: DateTime.utc_now(),
        last_reset: DateTime.utc_now()
      }
    }

    {:ok, state}
  end

  # API tracking handlers
  @impl true
  def handle_cast({:track_api_request, metric}, state) do
    # Store metric in ETS with unique key
    key = {metric.service, metric.timestamp, :rand.uniform(1_000_000)}
    :ets.insert(@table_name, {key, metric})

    {:noreply, state}
  end

  @impl true
  def handle_cast({:record_http, service, method, status_code, duration_ms, metadata}, state) do
    # Emit telemetry event
    :telemetry.execute(
      [:wanderer_kills, :http, :request],
      %{duration_ms: duration_ms, status_code: status_code},
      Map.merge(metadata, %{service: service, method: method})
    )

    # Track API request directly (avoid double-casting)
    api_metric = %{
      timestamp: System.monotonic_time(:millisecond),
      service: service,
      endpoint: Map.get(metadata, :url),
      duration_ms: duration_ms,
      status_code: status_code,
      error: status_code >= 400
    }

    # Update state with API request tracking directly
    key = {api_metric.service, api_metric.timestamp, :rand.uniform(1_000_000)}
    :ets.insert(@table_name, {key, api_metric})

    # Update counters
    status_class = div(status_code, 100)

    new_state =
      state
      |> increment_counter_internal(safe_metric_atom("http_#{service}_requests"))
      |> increment_counter_internal(safe_metric_atom("http_#{service}_#{method}"))
      |> increment_counter_internal(safe_metric_atom("http_#{service}_status_#{status_class}xx"))
      |> record_histogram_internal(safe_metric_atom("http_#{service}_duration_ms"), duration_ms)

    {:noreply, new_state}
  end

  # WebSocket handlers
  @impl true
  def handle_cast({:increment_kills_sent, type, count}, state) do
    new_kills_sent = Map.update!(state.websocket_stats.kills_sent, type, &(&1 + count))
    new_websocket_stats = %{state.websocket_stats | kills_sent: new_kills_sent}
    new_state = %{state | websocket_stats: new_websocket_stats}
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:track_websocket_connection, :connected, _metadata}, state) do
    new_connections = %{
      state.websocket_stats.connections
      | total_connected: state.websocket_stats.connections.total_connected + 1,
        active: state.websocket_stats.connections.active + 1
    }

    new_websocket_stats = %{state.websocket_stats | connections: new_connections}
    new_state = %{state | websocket_stats: new_websocket_stats}

    # Immediately persist WebSocket stats to ETS so dashboard shows current data
    websocket_stats_response = build_websocket_stats_response(new_state)

    if :ets.info(EtsOwner.wanderer_kills_stats_table()) != :undefined do
      :ets.insert(
        EtsOwner.wanderer_kills_stats_table(),
        {:websocket_stats, websocket_stats_response}
      )
    end

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:track_websocket_connection, :disconnected, _metadata}, state) do
    new_connections = %{
      state.websocket_stats.connections
      | total_disconnected: state.websocket_stats.connections.total_disconnected + 1,
        active: max(0, state.websocket_stats.connections.active - 1)
    }

    new_websocket_stats = %{state.websocket_stats | connections: new_connections}
    new_state = %{state | websocket_stats: new_websocket_stats}

    # Immediately persist WebSocket stats to ETS so dashboard shows current data
    websocket_stats_response = build_websocket_stats_response(new_state)

    if :ets.info(EtsOwner.wanderer_kills_stats_table()) != :undefined do
      :ets.insert(
        EtsOwner.wanderer_kills_stats_table(),
        {:websocket_stats, websocket_stats_response}
      )
    end

    {:noreply, new_state}
  end

  @impl true
  def handle_cast(
        {:track_websocket_subscription, :added, system_count, character_count, _metadata},
        state
      ) do
    new_subscriptions = %{
      state.websocket_stats.subscriptions
      | total_added: state.websocket_stats.subscriptions.total_added + 1,
        active: state.websocket_stats.subscriptions.active + 1,
        total_systems: state.websocket_stats.subscriptions.total_systems + system_count,
        total_characters: state.websocket_stats.subscriptions.total_characters + character_count
    }

    new_websocket_stats = %{state.websocket_stats | subscriptions: new_subscriptions}
    new_state = %{state | websocket_stats: new_websocket_stats}
    {:noreply, new_state}
  end

  @impl true
  def handle_cast(
        {
          :track_websocket_subscription,
          :removed,
          system_count,
          character_count,
          _metadata
        },
        state
      ) do
    new_subscriptions = %{
      state.websocket_stats.subscriptions
      | total_removed: state.websocket_stats.subscriptions.total_removed + 1,
        active: max(0, state.websocket_stats.subscriptions.active - 1),
        total_systems: max(0, state.websocket_stats.subscriptions.total_systems - system_count),
        total_characters:
          max(0, state.websocket_stats.subscriptions.total_characters - character_count)
    }

    new_websocket_stats = %{state.websocket_stats | subscriptions: new_subscriptions}
    new_state = %{state | websocket_stats: new_websocket_stats}
    {:noreply, new_state}
  end

  @impl true
  def handle_cast(
        {
          :track_websocket_subscription,
          :updated,
          system_count_delta,
          character_count_delta,
          _metadata
        },
        state
      ) do
    # For updates, adjust the total system and character counts by the deltas
    new_subscriptions = %{
      state.websocket_stats.subscriptions
      | total_systems:
          max(0, state.websocket_stats.subscriptions.total_systems + system_count_delta),
        total_characters:
          max(0, state.websocket_stats.subscriptions.total_characters + character_count_delta)
    }

    new_websocket_stats = %{state.websocket_stats | subscriptions: new_subscriptions}
    new_state = %{state | websocket_stats: new_websocket_stats}
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:record_cache, cache_name, operation, metadata}, state) do
    # Emit telemetry event
    :telemetry.execute(
      [:wanderer_kills, :cache, operation],
      %{count: 1},
      Map.merge(metadata, %{cache: cache_name})
    )

    # Update counters
    new_state =
      state
      |> increment_counter_internal(safe_metric_atom("cache.#{cache_name}.#{operation}"))
      |> increment_counter_internal(safe_metric_atom("cache.#{cache_name}.total"))

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:record_killmail, result, killmail_id, metadata}, state) do
    # Emit telemetry event
    :telemetry.execute(
      [:wanderer_kills, :killmail, result],
      %{killmail_id: killmail_id},
      metadata
    )

    # Update counters
    new_state =
      state
      |> increment_counter_internal(safe_metric_atom("killmail.#{result}"))
      |> increment_counter_internal(:killmail_total)

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:record_websocket, event, value, metadata}, state) do
    # Emit telemetry event
    :telemetry.execute(
      [:wanderer_kills, :websocket, event],
      %{value: value},
      metadata
    )

    # Update appropriate metric type
    new_state =
      case event do
        :connection -> increment_counter_internal(state, :websocket_connections)
        :disconnection -> increment_counter_internal(state, :websocket_disconnections)
        :kills_sent -> increment_counter_internal(state, :websocket_kills_sent, value)
        _ -> state
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:record_system, resource, value, metadata}, state) do
    # Emit telemetry event
    :telemetry.execute(
      [:wanderer_kills, :system, resource],
      %{value: value},
      metadata
    )

    # Update gauge
    new_state = set_gauge_internal(state, safe_metric_atom("system.#{resource}"), value)

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:set_gauge, name, value}, state) do
    new_state = set_gauge_internal(state, name, value)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:increment_counter, name, amount}, state) do
    new_state = increment_counter_internal(state, name, amount)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:record_histogram, name, value}, state) do
    new_state = record_histogram_internal(state, name, value)
    {:noreply, new_state}
  end

  # API stats calls
  @impl true
  def handle_call(:get_api_stats, _from, state) do
    stats = %{
      zkillboard: calculate_api_service_stats(:zkillboard),
      esi: calculate_api_service_stats(:esi),
      tracking_duration_minutes: tracking_duration_minutes(state)
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_call({:get_api_service_stats, service}, _from, state) do
    stats = calculate_api_service_stats(service)
    {:reply, stats, state}
  end

  # WebSocket stats calls
  @impl true
  def handle_call(:get_websocket_stats, _from, state) do
    stats = build_websocket_stats_response(state)
    {:reply, {:ok, stats}, state}
  end

  @impl true
  def handle_call(:reset_websocket_stats, _from, state) do
    new_websocket_stats = %{
      state.websocket_stats
      | kills_sent: %{realtime: 0, preload: 0},
        last_reset: DateTime.utc_now()
    }

    new_state = %{state | websocket_stats: new_websocket_stats}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:get_all_metrics, _from, state) do
    metrics = build_all_metrics(state)
    {:reply, {:ok, metrics}, state}
  end

  @impl true
  def handle_call({:get_service_metrics, service}, _from, state) do
    metrics = build_service_metrics(state, service)
    {:reply, {:ok, metrics}, state}
  end

  @impl true
  def handle_call(:reset_metrics, _from, state) do
    new_state = %{state | counters: %{}, gauges: %{}, histograms: %{}, metrics: %{}}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_info(:attach_telemetry, state) do
    attach_telemetry_handlers()
    {:noreply, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    # Remove API metrics older than window size
    cutoff = System.monotonic_time(:millisecond) - @window_size_ms
    :ets.select_delete(@table_name, cleanup_match_spec(cutoff))

    # Schedule next cleanup
    schedule_cleanup()
    {:noreply, state}
  end

  @impl true
  def handle_info(:stats_summary, state) do
    # Store stats in ETS for unified status reporter
    websocket_stats = build_websocket_stats_response(state)

    api_stats = %{
      zkillboard: calculate_api_service_stats(:zkillboard),
      esi: calculate_api_service_stats(:esi)
    }

    if :ets.info(EtsOwner.wanderer_kills_stats_table()) != :undefined do
      :ets.insert(EtsOwner.wanderer_kills_stats_table(), {:websocket_stats, websocket_stats})
      :ets.insert(EtsOwner.wanderer_kills_stats_table(), {:api_stats, api_stats})
    end

    # Emit telemetry for the summary
    :telemetry.execute(
      [:wanderer_kills, :metrics, :summary],
      %{
        websocket_active_connections: websocket_stats.connections.active,
        websocket_total_kills_sent: websocket_stats.kills_sent.total,
        api_zkb_requests: Map.get(api_stats.zkillboard, :total_requests, 0),
        api_esi_requests: Map.get(api_stats.esi, :total_requests, 0)
      },
      %{period: "5_minutes"}
    )

    schedule_stats_summary()
    {:noreply, state}
  end

  @impl true
  def handle_info(:health_check, state) do
    # Reconcile WebSocket connection stats if needed
    new_state = reconcile_websocket_connection_stats(state)
    schedule_health_check()
    {:noreply, new_state}
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp schedule_telemetry_attachment do
    Process.send_after(self(), :attach_telemetry, 100)
  end

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @cleanup_interval_ms)
  end

  defp schedule_stats_summary do
    Process.send_after(self(), :stats_summary, @stats_summary_interval)
  end

  defp schedule_health_check do
    Process.send_after(self(), :health_check, @health_check_interval)
  end

  defp increment_counter_internal(state, name, amount \\ 1) do
    counters = Map.update(state.counters, name, amount, &(&1 + amount))
    %{state | counters: counters}
  end

  defp set_gauge_internal(state, name, value) do
    gauges = Map.put(state.gauges, name, value)
    %{state | gauges: gauges}
  end

  defp record_histogram_internal(state, name, value) do
    histogram = Map.get(state.histograms, name, [])
    histograms = Map.put(state.histograms, name, [value | histogram])
    %{state | histograms: histograms}
  end

  defp build_all_metrics(state) do
    uptime_seconds = DateTime.diff(DateTime.utc_now(), state.start_time)

    %{
      timestamp: Utils.now_iso8601(),
      uptime_seconds: uptime_seconds,
      counters: state.counters,
      gauges: state.gauges,
      histograms: calculate_histogram_stats(state.histograms),
      rates: calculate_rates(state.counters, uptime_seconds)
    }
  end

  defp build_service_metrics(state, service) do
    service_prefix = Atom.to_string(service)

    counters =
      state.counters
      |> Enum.filter(fn {key, _} -> String.starts_with?(Atom.to_string(key), service_prefix) end)
      |> Map.new()

    gauges =
      state.gauges
      |> Enum.filter(fn {key, _} -> String.starts_with?(Atom.to_string(key), service_prefix) end)
      |> Map.new()

    histograms =
      state.histograms
      |> Enum.filter(fn {key, _} -> String.starts_with?(Atom.to_string(key), service_prefix) end)
      |> calculate_histogram_stats()

    %{
      service: service,
      timestamp: Utils.now_iso8601(),
      counters: counters,
      gauges: gauges,
      histograms: histograms
    }
  end

  defp calculate_histogram_stats(histograms) do
    histograms
    |> Enum.map(fn {name, values} ->
      sorted = Enum.sort(values)
      count = length(values)

      stats =
        if count > 0 do
          %{
            count: count,
            min: List.first(sorted),
            max: List.last(sorted),
            mean: Enum.sum(values) / count,
            p50: percentile(sorted, 0.5),
            p95: percentile(sorted, 0.95),
            p99: percentile(sorted, 0.99)
          }
        else
          %{count: 0}
        end

      {name, stats}
    end)
    |> Map.new()
  end

  defp calculate_rates(counters, duration_seconds) do
    counters
    |> Enum.map(fn {name, count} ->
      rate = if duration_seconds > 0, do: count / duration_seconds, else: 0
      {safe_metric_atom("#{name}_per_second"), Float.round(rate, 2)}
    end)
    |> Map.new()
  end

  defp percentile(sorted_list, p) when is_list(sorted_list) and p >= 0 and p <= 1 do
    count = length(sorted_list)
    k = (count - 1) * p
    f = :math.floor(k)
    c = :math.ceil(k)

    if f == c do
      Enum.at(sorted_list, trunc(k))
    else
      d0 = Enum.at(sorted_list, trunc(f)) * (c - k)
      d1 = Enum.at(sorted_list, trunc(c)) * (k - f)
      d0 + d1
    end
  end

  # API tracking functions (from api_tracker.ex)
  defp attach_telemetry_handlers do
    :telemetry.attach_many(
      "consolidated-metrics-http-events",
      [
        [:wanderer_kills, :http, :request, :stop],
        [:wanderer_kills, :http, :request, :error]
      ],
      &__MODULE__.handle_http_telemetry/4,
      nil
    )
  end

  @doc false
  def handle_http_telemetry(_event_name, measurements, metadata, _config) do
    service = determine_service_from_metadata(metadata)

    if service do
      track_api_request(service,
        endpoint: extract_endpoint(metadata),
        duration_ms: div(measurements[:duration] || 0, 1_000_000),
        status_code: metadata[:status_code],
        error: metadata[:error] != nil
      )
    end
  end

  defp determine_service_from_metadata(%{url: url}) when is_binary(url) do
    cond do
      String.contains?(url, "zkillboard.com") -> :zkillboard
      String.contains?(url, "esi.evetech.net") -> :esi
      true -> nil
    end
  end

  defp determine_service_from_metadata(_), do: nil

  defp extract_endpoint(%{url: url}) when is_binary(url) do
    case URI.parse(url) do
      %{path: path} when is_binary(path) ->
        normalize_endpoint_path(path)

      _ ->
        nil
    end
  end

  defp extract_endpoint(_), do: nil

  defp normalize_endpoint_path(path) do
    path
    |> String.split("/")
    |> Enum.map(&replace_id_segment/1)
    |> Enum.join("/")
  end

  defp replace_id_segment(segment) do
    if String.match?(segment, ~r/^\d+$/), do: "{id}", else: segment
  end

  defp calculate_api_service_stats(service) do
    now = System.monotonic_time(:millisecond)
    window_start = now - :timer.minutes(5)
    one_minute_ago = now - :timer.minutes(1)

    # Get all metrics for this service within window
    match_spec = [
      {
        {{:"$1", :"$2", :_}, :"$3"},
        [
          {:==, :"$1", service},
          {:>, :"$2", window_start}
        ],
        [:"$3"]
      }
    ]

    metrics = :ets.select(@table_name, match_spec)

    # Calculate statistics
    total_count = length(metrics)
    recent_metrics = Enum.filter(metrics, &(&1.timestamp > one_minute_ago))
    recent_count = length(recent_metrics)

    error_count = Enum.count(metrics, & &1.error)
    successful_metrics = Enum.filter(metrics, &(!&1.error && &1.duration_ms > 0))

    durations = Enum.map(successful_metrics, & &1.duration_ms)

    avg_duration =
      if durations != [] do
        Enum.sum(durations) / length(durations)
      else
        0.0
      end

    # Group by endpoint
    endpoint_stats =
      metrics
      |> Enum.group_by(& &1.endpoint)
      |> Enum.map(fn {endpoint, endpoint_metrics} ->
        {endpoint || "unknown",
         %{
           count: length(endpoint_metrics),
           errors: Enum.count(endpoint_metrics, & &1.error)
         }}
      end)
      |> Enum.into(%{})

    %{
      total_requests: total_count,
      requests_per_minute: recent_count,
      error_count: error_count,
      error_rate: if(total_count > 0, do: error_count / total_count * 100, else: 0.0),
      avg_duration_ms: Float.round(avg_duration, 1),
      min_duration_ms: Enum.min(durations, fn -> 0 end),
      max_duration_ms: Enum.max(durations, fn -> 0 end),
      endpoints: endpoint_stats
    }
  end

  defp tracking_duration_minutes(state) do
    now = System.monotonic_time(:millisecond)
    start_time_ms = DateTime.to_unix(state.start_time, :millisecond)
    div(now - start_time_ms, :timer.minutes(1))
  end

  defp cleanup_match_spec(cutoff) do
    [
      {
        {{:"$1", :"$2", :_}, %{timestamp: :"$2"}},
        [{:<, :"$2", cutoff}],
        [true]
      }
    ]
  end

  # WebSocket functions (from websocket_stats.ex)
  defp build_websocket_stats_response(state) do
    ws_stats = state.websocket_stats
    total_kills = ws_stats.kills_sent.realtime + ws_stats.kills_sent.preload
    uptime_seconds = DateTime.diff(DateTime.utc_now(), ws_stats.started_at)

    %{
      kills_sent: %{
        realtime: ws_stats.kills_sent.realtime,
        preload: ws_stats.kills_sent.preload,
        total: total_kills
      },
      connections: %{
        active: ws_stats.connections.active,
        total_connected: ws_stats.connections.total_connected,
        total_disconnected: ws_stats.connections.total_disconnected
      },
      subscriptions: %{
        active: ws_stats.subscriptions.active,
        total_added: ws_stats.subscriptions.total_added,
        total_removed: ws_stats.subscriptions.total_removed,
        total_systems: ws_stats.subscriptions.total_systems,
        total_characters: ws_stats.subscriptions.total_characters
      },
      rates: calculate_websocket_current_rates(ws_stats),
      uptime_seconds: uptime_seconds,
      started_at: DateTime.to_iso8601(ws_stats.started_at),
      last_reset: DateTime.to_iso8601(ws_stats.last_reset),
      timestamp: Utils.now_iso8601()
    }
  end

  defp calculate_websocket_current_rates(ws_stats) do
    uptime_minutes = max(1, DateTime.diff(DateTime.utc_now(), ws_stats.started_at) / 60)
    reset_minutes = max(1, DateTime.diff(DateTime.utc_now(), ws_stats.last_reset) / 60)

    total_kills = ws_stats.kills_sent.realtime + ws_stats.kills_sent.preload

    %{
      kills_per_minute: total_kills / reset_minutes,
      connections_per_minute: ws_stats.connections.total_connected / uptime_minutes,
      average_systems_per_subscription:
        if ws_stats.subscriptions.active > 0 do
          ws_stats.subscriptions.total_systems / ws_stats.subscriptions.active
        else
          0.0
        end
    }
  end

  defp reconcile_websocket_connection_stats(state) do
    try do
      # Get actual monitored connections from HeartbeatMonitor
      heartbeat_stats = WandererKillsWeb.Channels.HeartbeatMonitor.get_stats()
      actual_connections = heartbeat_stats.monitored_connections

      # Get tracked connection count from our metrics
      current_ws_stats = state.websocket_stats
      tracked_active = current_ws_stats.connections.active

      # Check for discrepancies
      if actual_connections != tracked_active do
        Logger.warning("[Metrics] WebSocket connection reconciliation found discrepancy",
          tracked: tracked_active,
          actual: actual_connections,
          difference: actual_connections - tracked_active
        )

        # Emit telemetry for monitoring
        :telemetry.execute(
          [:wanderer_kills, :websocket, :reconciliation],
          %{
            tracked_connections: tracked_active,
            actual_connections: actual_connections,
            discrepancy: actual_connections - tracked_active
          },
          %{source: :heartbeat_monitor}
        )

        # Update the active connection count to match reality
        updated_connections =
          put_in(
            current_ws_stats.connections.active,
            actual_connections
          )

        updated_ws_stats = %{current_ws_stats | connections: updated_connections}

        # Update ETS table with corrected stats
        :ets.insert(EtsOwner.wanderer_kills_stats_table(), {:websocket_stats, updated_ws_stats})

        %{state | websocket_stats: updated_ws_stats}
      else
        # Connections are in sync
        state
      end
    rescue
      error ->
        Logger.error("[Metrics] Failed to reconcile WebSocket connections",
          error: inspect(error),
          stacktrace: __STACKTRACE__
        )

        # Return original state on error to avoid crashes
        state
    end
  end

  # Safe metric atom creation with pattern validation to prevent atom table exhaustion
  @spec safe_metric_atom(String.t()) :: atom() | :invalid_metric | :table_size_limit_reached
  defp safe_metric_atom(string) when is_binary(string) do
    if valid_metric_name?(string) do
      lookup_or_create_metric_atom(string)
    else
      Logger.warning("Invalid metric name attempted", metric: string)
      :invalid_metric
    end
  end

  @spec lookup_or_create_metric_atom(String.t()) :: atom() | :table_size_limit_reached
  defp lookup_or_create_metric_atom(string) do
    case :ets.lookup(@table_name, {:metric_atom, string}) do
      [{_, atom}] ->
        atom

      [] ->
        case create_and_store_metric_atom(string) do
          :table_size_limit_reached -> :table_size_limit_reached
          atom when is_atom(atom) -> atom
        end
    end
  end

  @spec create_and_store_metric_atom(String.t()) :: atom() | :table_size_limit_reached
  defp create_and_store_metric_atom(string) do
    # Check current size of metric atoms in the table
    current_count = count_metric_atoms()

    if current_count >= @max_metric_atoms do
      Logger.warning("Metric atom table size limit reached",
        current_count: current_count,
        limit: @max_metric_atoms,
        attempted_metric: string
      )

      :table_size_limit_reached
    else
      atom = String.to_existing_atom(string)
      :ets.insert(@table_name, {{:metric_atom, string}, atom})
      atom
    end
  rescue
    ArgumentError ->
      # Check size limit again before creating new atom
      current_count = count_metric_atoms()

      if current_count >= @max_metric_atoms do
        Logger.warning("Metric atom table size limit reached",
          current_count: current_count,
          limit: @max_metric_atoms,
          attempted_metric: string
        )

        :table_size_limit_reached
      else
        # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
        atom = String.to_atom(string)
        :ets.insert(@table_name, {{:metric_atom, string}, atom})
        atom
      end
  end

  # Count the number of metric atoms currently stored in the table
  @spec count_metric_atoms() :: non_neg_integer()
  defp count_metric_atoms do
    case :ets.info(@table_name) do
      :undefined ->
        0

      _ ->
        # Count only metric_atom entries using match
        pattern = {{:metric_atom, :"$1"}, :"$2"}
        :ets.match(@table_name, pattern) |> length()
    end
  end

  # Check if a metric name matches any of our allowed patterns
  defp valid_metric_name?(string) do
    Enum.any?(@metric_patterns, fn pattern ->
      Regex.match?(pattern, string)
    end)
  end

  @impl true
  def terminate(_reason, _state) do
    # Clean up ETS table on termination
    if :ets.info(@table_name) != :undefined do
      :ets.delete(@table_name)
    end

    :ok
  end
end
