defmodule WandererKills.Core.Storage.KillmailStore do
  @moduledoc """
  Unified ETS-backed killmail storage with optional event streaming support.

  This module consolidates the functionality of both the basic Store and
  the event-streaming KillStore into a single implementation. Event streaming
  features can be enabled/disabled via configuration.

  ## Features

  - Core killmail storage and retrieval
  - System-based killmail organization
  - Optional event streaming for real-time updates
  - Client offset tracking for event consumption
  - Fetch timestamp management
  - Kill count statistics

  ## Configuration

  ```elixir
  config :wanderer_kills, :storage,
    enable_event_streaming: true  # Default: true
  ```

  ## Environment Variables

  - `KILLMAIL_RETENTION_DAYS` - Number of days to retain killmail data (default: 2)
  """

  @behaviour WandererKills.Core.Storage.Behaviour

  require Logger
  alias WandererKills.Core.Support.Error

  # ETS tables
  @killmails_table :killmails
  @system_killmails_table :system_killmails
  @system_kill_counts_table :system_kill_counts
  @system_fetch_timestamps_table :system_fetch_timestamps

  # Event streaming tables (optional)
  @killmail_events_table :killmail_events
  @client_offsets_table :client_offsets
  @counters_table :counters

  @type killmail_id :: integer()
  @type system_id :: integer()
  @type killmail_data :: map()
  @type event_id :: integer()
  @type client_id :: term()
  @type client_offsets :: %{system_id() => event_id()}
  @type event_tuple :: {event_id(), system_id(), killmail_data()}

  # ============================================================================
  # Table Initialization
  # ============================================================================

  @doc """
  Initializes all required ETS tables at application start.
  """
  @impl true
  def init_tables! do
    # Core tables - create only if they don't exist
    ensure_table_exists(@killmails_table, [:set, :named_table, :public, {:read_concurrency, true}])

    ensure_table_exists(@system_killmails_table, [
      :set,
      :named_table,
      :public,
      {:read_concurrency, true}
    ])

    ensure_table_exists(@system_kill_counts_table, [
      :set,
      :named_table,
      :public,
      {:read_concurrency, true}
    ])

    ensure_table_exists(@system_fetch_timestamps_table, [
      :set,
      :named_table,
      :public,
      {:read_concurrency, true}
    ])

    tables = [
      @killmails_table,
      @system_killmails_table,
      @system_kill_counts_table,
      @system_fetch_timestamps_table
    ]

    # Event streaming tables (if enabled)
    if event_streaming_enabled?() do
      ensure_table_exists(@killmail_events_table, [
        :ordered_set,
        :named_table,
        :public,
        {:read_concurrency, true}
      ])

      ensure_table_exists(@client_offsets_table, [
        :set,
        :named_table,
        :public,
        {:read_concurrency, true}
      ])

      ensure_table_exists(@counters_table, [
        :set,
        :named_table,
        :public,
        {:read_concurrency, true}
      ])

      event_tables = [@killmail_events_table, @client_offsets_table, @counters_table]
      all_tables = tables ++ event_tables

      Logger.info("Initialized KillmailStore ETS tables: #{inspect(all_tables)}")
    else
      Logger.info("Initialized KillmailStore ETS tables: #{inspect(tables)}")
    end

    :ok
  end

  # ============================================================================
  # Core Storage Operations
  # ============================================================================

  @doc """
  Stores a killmail without system association.
  """
  @impl true
  def put(killmail_id, killmail_data) when is_integer(killmail_id) and is_map(killmail_data) do
    :ets.insert(@killmails_table, {killmail_id, killmail_data})
    :ok
  end

  @doc """
  Stores a killmail with system association.
  """
  @impl true
  def put(killmail_id, system_id, killmail_data)
      when is_integer(killmail_id) and is_integer(system_id) and is_map(killmail_data) do
    # Store the killmail
    :ets.insert(@killmails_table, {killmail_id, killmail_data})

    # Associate with system
    add_system_killmail(system_id, killmail_id)

    :ok
  end

  @doc """
  Retrieves a killmail by ID.
  """
  @impl true
  def get(killmail_id) when is_integer(killmail_id) do
    case :ets.lookup(@killmails_table, killmail_id) do
      [{^killmail_id, data}] ->
        {:ok, data}

      [] ->
        {:error, Error.not_found_error("Killmail not found", %{killmail_id: killmail_id})}
    end
  end

  @doc """
  Deletes a killmail from the store.
  """
  @impl true
  def delete(killmail_id) when is_integer(killmail_id) do
    # Delete from main table
    :ets.delete(@killmails_table, killmail_id)

    # Remove from all system associations
    :ets.foldl(
      fn {system_id, killmail_ids}, _acc ->
        remove_killmail_from_system(system_id, killmail_ids, killmail_id)
      end,
      :ok,
      @system_killmails_table
    )

    :ok
  end

  @doc """
  Lists all killmails for a specific system.
  """
  @impl true
  def list_by_system(system_id) when is_integer(system_id) do
    case :ets.lookup(@system_killmails_table, system_id) do
      [{^system_id, killmail_ids}] ->
        Enum.flat_map(killmail_ids, &get_killmail_data/1)

      [] ->
        []
    end
  end

  @doc """
  Get system killmails (alias for list_by_system for API compatibility).
  """
  def get_system_killmails(system_id) when is_integer(system_id) do
    list_by_system(system_id)
  end

  # ============================================================================
  # System Operations
  # ============================================================================

  @doc """
  Adds a killmail to a system's list.

  Optimized to minimize list traversal by checking existence only when the list is small.
  For larger lists, duplicates are rare so we skip the check.
  """
  @impl true
  def add_system_killmail(system_id, killmail_id)
      when is_integer(system_id) and is_integer(killmail_id) do
    case :ets.lookup(@system_killmails_table, system_id) do
      [] ->
        :ets.insert(@system_killmails_table, {system_id, [killmail_id]})

      [{^system_id, existing_ids}] when length(existing_ids) < 100 ->
        # For small lists, check for duplicates using simple list membership
        if killmail_id not in existing_ids do
          :ets.insert(@system_killmails_table, {system_id, [killmail_id | existing_ids]})
        end

      [{^system_id, existing_ids}] ->
        # For large lists, skip duplicate check as duplicates are rare
        # and the O(n) check becomes expensive
        :ets.insert(@system_killmails_table, {system_id, [killmail_id | existing_ids]})
    end

    :ok
  end

  @doc """
  Gets all killmail IDs for a system.
  """
  @impl true
  def get_killmails_for_system(system_id) when is_integer(system_id) do
    case :ets.lookup(@system_killmails_table, system_id) do
      [{^system_id, killmail_ids}] -> {:ok, killmail_ids}
      [] -> {:ok, []}
    end
  end

  @doc """
  Removes a killmail from a system's list.
  """
  @impl true
  def remove_system_killmail(system_id, killmail_id)
      when is_integer(system_id) and is_integer(killmail_id) do
    case :ets.lookup(@system_killmails_table, system_id) do
      [] ->
        :ok

      [{^system_id, existing_ids}] ->
        new_ids = List.delete(existing_ids, killmail_id)

        if Enum.empty?(new_ids) do
          :ets.delete(@system_killmails_table, system_id)
        else
          :ets.insert(@system_killmails_table, {system_id, new_ids})
        end
    end

    :ok
  end

  @doc """
  Increments the kill count for a system.
  """
  @impl true
  def increment_system_killmail_count(system_id) when is_integer(system_id) do
    :ets.update_counter(@system_kill_counts_table, system_id, {2, 1}, {system_id, 0})
    :ok
  end

  @doc """
  Gets the kill count for a system.
  """
  @impl true
  def get_system_killmail_count(system_id) when is_integer(system_id) do
    case :ets.lookup(@system_kill_counts_table, system_id) do
      [{^system_id, count}] -> {:ok, count}
      [] -> {:ok, 0}
    end
  end

  # ============================================================================
  # Timestamp Operations
  # ============================================================================

  @doc """
  Sets the fetch timestamp for a system.
  """
  @impl true
  def set_system_fetch_timestamp(system_id, timestamp)
      when is_integer(system_id) and is_struct(timestamp, DateTime) do
    :ets.insert(@system_fetch_timestamps_table, {system_id, timestamp})
    :ok
  end

  @doc """
  Gets the fetch timestamp for a system.
  """
  @impl true
  def get_system_fetch_timestamp(system_id) when is_integer(system_id) do
    case :ets.lookup(@system_fetch_timestamps_table, system_id) do
      [{^system_id, timestamp}] ->
        {:ok, timestamp}

      [] ->
        {:error,
         Error.not_found_error("No fetch timestamp found for system", %{system_id: system_id})}
    end
  end

  # ============================================================================
  # Event Streaming Operations
  # ============================================================================

  @doc """
  Inserts a new killmail event for streaming (if enabled).
  """
  @impl true
  def insert_event(system_id, killmail_map) when is_integer(system_id) and is_map(killmail_map) do
    if event_streaming_enabled?() do
      # Get next event ID
      event_id = get_next_event_id()

      # Store the killmail
      killmail_id = killmail_map["killmail_id"]
      :ets.insert(@killmails_table, {killmail_id, killmail_map})

      # Add to system killmails
      add_system_killmail(system_id, killmail_id)

      # Insert event for streaming
      :ets.insert(@killmail_events_table, {event_id, system_id, killmail_map})

      # Broadcast via PubSub
      Phoenix.PubSub.broadcast(
        WandererKills.PubSub,
        "system:#{system_id}",
        {:new_killmail, system_id, killmail_map}
      )
    else
      # Just store without event streaming
      killmail_id = killmail_map["killmail_id"]
      put(killmail_id, system_id, killmail_map)
    end

    :ok
  end

  @doc """
  Fetches all new events for a client (if event streaming enabled).
  """
  @impl true
  def fetch_for_client(client_id, system_ids) when is_list(system_ids) do
    if event_streaming_enabled?() do
      do_fetch_for_client(client_id, system_ids)
    else
      {:ok, []}
    end
  end

  @doc """
  Fetches the next single event for a client (if event streaming enabled).
  """
  @impl true
  def fetch_one_event(client_id, system_ids) when is_list(system_ids) do
    if event_streaming_enabled?() do
      do_fetch_one_event(client_id, system_ids)
    else
      :empty
    end
  end

  def fetch_one_event(client_id, system_id) when is_integer(system_id) do
    fetch_one_event(client_id, [system_id])
  end

  @doc """
  Gets client offsets for event streaming.
  """
  @impl true
  def get_client_offsets(client_id) do
    if event_streaming_enabled?() do
      case :ets.lookup(@client_offsets_table, client_id) do
        # Handle new format with timestamp
        [{^client_id, offsets, _timestamp}] when is_map(offsets) -> offsets
        # Handle old format for backwards compatibility
        [{^client_id, offsets}] when is_map(offsets) -> offsets
        [] -> %{}
      end
    else
      %{}
    end
  end

  @doc """
  Updates client offsets for event streaming.
  """
  @impl true
  def put_client_offsets(client_id, offsets) when is_map(offsets) do
    if event_streaming_enabled?() do
      # Store with last access timestamp
      :ets.insert(@client_offsets_table, {client_id, offsets, DateTime.utc_now()})
    end

    :ok
  end

  # ============================================================================
  # Maintenance Operations
  # ============================================================================

  @doc """
  Clears all data from all tables (for testing).
  """
  @impl true
  def clear do
    # Safely clear tables only if they exist
    if :ets.info(@killmails_table) != :undefined do
      :ets.delete_all_objects(@killmails_table)
    end

    if :ets.info(@system_killmails_table) != :undefined do
      :ets.delete_all_objects(@system_killmails_table)
    end

    if :ets.info(@system_kill_counts_table) != :undefined do
      :ets.delete_all_objects(@system_kill_counts_table)
    end

    if :ets.info(@system_fetch_timestamps_table) != :undefined do
      :ets.delete_all_objects(@system_fetch_timestamps_table)
    end

    if event_streaming_enabled?() do
      if :ets.info(@killmail_events_table) != :undefined do
        :ets.delete_all_objects(@killmail_events_table)
      end

      if :ets.info(@client_offsets_table) != :undefined do
        :ets.delete_all_objects(@client_offsets_table)
      end

      if :ets.info(@counters_table) != :undefined do
        :ets.delete_all_objects(@counters_table)
        # Always reinitialize counters after clearing
        :ets.insert(@counters_table, {:event_counter, 0})
        :ets.insert(@counters_table, {:killmail_seq, 0})
      end
    end

    :ok
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp event_streaming_enabled? do
    Application.get_env(:wanderer_kills, :storage, [])
    |> Keyword.get(:enable_event_streaming, true)
  end

  defp get_killmail_data(killmail_id) do
    case :ets.lookup(@killmails_table, killmail_id) do
      [{^killmail_id, killmail_data}] -> [killmail_data]
      [] -> []
    end
  end

  defp get_next_event_id do
    :ets.update_counter(@counters_table, :event_counter, 1)
  end

  defp get_offset_for_system(system_id, offsets) do
    Map.get(offsets, system_id, 0)
  end

  defp do_fetch_for_client(client_id, system_ids) do
    # Get client offsets
    client_offsets = get_client_offsets(client_id)

    # Handle empty system list
    if Enum.empty?(system_ids) do
      {:ok, []}
    else
      # Create conditions for each system
      conditions =
        Enum.map(system_ids, fn sys_id ->
          {:andalso, {:==, :"$2", sys_id},
           {:>, :"$1", get_offset_for_system(sys_id, client_offsets)}}
        end)

      # Build the match specification guard
      guard =
        case conditions do
          [single] -> single
          multiple -> List.to_tuple([:orelse | multiple])
        end

      # Create match specification for :ets.select
      match_spec = [
        {
          {:"$1", :"$2", :"$3"},
          [guard],
          [{{:"$1", :"$2", :"$3"}}]
        }
      ]

      # Get all matching events
      events = :ets.select(@killmail_events_table, match_spec)

      # Sort by event_id ascending
      sorted_events = Enum.sort_by(events, &elem(&1, 0))

      # Update client offsets for each system
      updated_offsets = update_client_offsets(sorted_events, client_offsets)

      # Store updated offsets
      :ets.insert(@client_offsets_table, {client_id, updated_offsets})

      {:ok, sorted_events}
    end
  end

  defp do_fetch_one_event(client_id, system_ids) do
    # Get client offsets
    client_offsets = get_client_offsets(client_id)

    # Handle empty system list
    if Enum.empty?(system_ids) do
      :empty
    else
      # Create conditions for each system
      conditions =
        Enum.map(system_ids, fn sys_id ->
          {:andalso, {:==, :"$2", sys_id},
           {:>, :"$1", get_offset_for_system(sys_id, client_offsets)}}
        end)

      # Build the match specification guard
      guard =
        case conditions do
          [single] -> single
          multiple -> List.to_tuple([:orelse | multiple])
        end

      # Create match specification for :ets.select
      match_spec = [
        {
          {:"$1", :"$2", :"$3"},
          [guard],
          [{{:"$1", :"$2", :"$3"}}]
        }
      ]

      # Use :ets.select to get matching events
      case :ets.select(@killmail_events_table, match_spec, 1) do
        {[{event_id, sys_id, km}], _continuation} ->
          # Update offset for this system only
          updated_offsets = Map.put(client_offsets, sys_id, event_id)
          # Store with last access timestamp
          :ets.insert(@client_offsets_table, {client_id, updated_offsets, DateTime.utc_now()})

          {:ok, {event_id, sys_id, km}}

        {[], _continuation} ->
          :empty

        :"$end_of_table" ->
          :empty
      end
    end
  end

  defp update_client_offsets(sorted_events, client_offsets) do
    Enum.reduce(sorted_events, client_offsets, &update_offset_for_event/2)
  end

  defp update_offset_for_event({event_id, sys_id, _}, acc) do
    current_offset = Map.get(acc, sys_id, 0)
    if event_id > current_offset, do: Map.put(acc, sys_id, event_id), else: acc
  end

  defp remove_killmail_from_system(system_id, killmail_ids, killmail_id) do
    if killmail_id in killmail_ids do
      updated_ids = List.delete(killmail_ids, killmail_id)

      if Enum.empty?(updated_ids) do
        :ets.delete(@system_killmails_table, system_id)
      else
        :ets.insert(@system_killmails_table, {system_id, updated_ids})
      end
    end

    :ok
  end

  # Helper function to ensure table exists before creation
  defp ensure_table_exists(table_name, options) do
    case :ets.info(table_name) do
      :undefined ->
        :ets.new(table_name, options)
        maybe_initialize_counters(table_name)
        table_name

      _ ->
        maybe_initialize_counters(table_name)
        table_name
    end
  end

  # Initialize counters if this is the counters table
  defp maybe_initialize_counters(table_name) do
    if table_name == @counters_table do
      ensure_counter_exists(:event_counter, 0)
      ensure_counter_exists(:killmail_seq, 0)
    end
  end

  # Ensure a specific counter exists with default value
  defp ensure_counter_exists(counter_key, default_value) do
    case :ets.lookup(@counters_table, counter_key) do
      [] -> :ets.insert(@counters_table, {counter_key, default_value})
      _ -> :ok
    end
  end

  # ============================================================================
  # Cleanup Operations
  # ============================================================================

  @doc """
  Performs cleanup of old data based on configured TTLs.

  TTL Configuration:
  - killmails: Configurable via KILLMAIL_RETENTION_DAYS env var (default: 2 days)
  - system_killmails: Same as killmails retention
  - system_kill_counts: Cleaned when system has no killmails
  - system_fetch_timestamps: 12 hours
  - killmail_events: Same as killmails retention
  - client_offsets: 1 day
  """
  def cleanup_old_data do
    Logger.info("[KillmailStore] Starting cleanup of old data")

    now = DateTime.utc_now()

    # Define TTLs in seconds - configurable via environment variables
    killmail_retention_days =
      case Integer.parse(System.get_env("KILLMAIL_RETENTION_DAYS", "2")) do
        {value, ""} when value > 0 -> value
        _ -> 2
      end

    killmail_ttl = killmail_retention_days * 24 * 60 * 60

    Logger.info(
      "[KillmailStore] Using retention settings: #{killmail_retention_days} days for killmails"
    )

    # Other TTLs remain hardcoded for now (12 hours)
    system_count_ttl = 12 * 60 * 60
    timestamp_ttl = 12 * 60 * 60
    # 1 day
    client_offset_ttl = 24 * 60 * 60

    # Track cleanup stats
    stats = %{
      killmails_removed: 0,
      system_killmails_cleaned: 0,
      system_counts_removed: 0,
      timestamps_removed: 0,
      events_removed: 0,
      client_offsets_removed: 0
    }

    # Cleanup killmails older than 7 days
    stats = cleanup_killmails(now, killmail_ttl, stats)

    # Cleanup system kill counts older than 1 day
    stats = cleanup_system_counts(now, system_count_ttl, stats)

    # Cleanup fetch timestamps older than 1 day
    stats = cleanup_fetch_timestamps(now, timestamp_ttl, stats)

    # Cleanup event streaming data if enabled
    final_stats =
      if event_streaming_enabled?() do
        stats
        |> cleanup_events(now, killmail_ttl)
        |> cleanup_client_offsets(now, client_offset_ttl)
      else
        stats
      end

    Logger.info("[KillmailStore] Cleanup completed", final_stats)

    {:ok, final_stats}
  rescue
    error ->
      Logger.error("[KillmailStore] Cleanup failed",
        error: inspect(error),
        stacktrace: __STACKTRACE__
      )

      {:error, error}
  end

  defp cleanup_killmails(now, ttl, stats) do
    cutoff = DateTime.add(now, -ttl, :second)

    # Count and remove old killmails
    removed_count =
      :ets.foldl(
        fn {killmail_id, killmail_data}, acc ->
          case get_killmail_time(killmail_data) do
            {:ok, kill_time} when kill_time < cutoff ->
              # Remove from main table
              :ets.delete(@killmails_table, killmail_id)
              # Also remove from system killmails
              remove_killmail_from_all_systems(killmail_id)
              acc + 1

            _ ->
              acc
          end
        end,
        0,
        @killmails_table
      )

    %{stats | killmails_removed: removed_count}
  end

  defp cleanup_system_counts(_now, _ttl, stats) do
    # System kill counts are running totals, so we only remove entries
    # for systems that have no killmails at all
    empty_systems =
      :ets.foldl(
        fn {system_id, _count}, acc ->
          case :ets.lookup(@system_killmails_table, system_id) do
            [] ->
              :ets.delete(@system_kill_counts_table, system_id)
              acc + 1

            _ ->
              acc
          end
        end,
        0,
        @system_kill_counts_table
      )

    %{stats | system_counts_removed: empty_systems}
  end

  defp cleanup_fetch_timestamps(now, ttl, stats) do
    cutoff = DateTime.add(now, -ttl, :second)

    removed =
      :ets.foldl(
        fn {system_id, timestamp}, acc ->
          if DateTime.compare(timestamp, cutoff) == :lt do
            :ets.delete(@system_fetch_timestamps_table, system_id)
            acc + 1
          else
            acc
          end
        end,
        0,
        @system_fetch_timestamps_table
      )

    %{stats | timestamps_removed: removed}
  end

  defp cleanup_events(stats, now, ttl) do
    cutoff = DateTime.add(now, -ttl, :second)

    # Since events table is ordered_set with event_id as key,
    # we need to check each event's killmail time
    removed =
      :ets.foldl(
        fn {event_id, _system_id, killmail_data}, acc ->
          case get_killmail_time(killmail_data) do
            {:ok, kill_time} when kill_time < cutoff ->
              :ets.delete(@killmail_events_table, event_id)
              acc + 1

            _ ->
              acc
          end
        end,
        0,
        @killmail_events_table
      )

    %{stats | events_removed: removed}
  end

  defp cleanup_client_offsets(stats, now, ttl) do
    cutoff = DateTime.add(now, -ttl, :second)

    # Remove client offsets older than TTL
    removed =
      :ets.foldl(
        fn entry, acc ->
          process_client_offset_entry(entry, acc, cutoff)
        end,
        0,
        @client_offsets_table
      )

    %{stats | client_offsets_removed: removed}
  end

  defp process_client_offset_entry({client_id, _offsets, last_access}, acc, cutoff) do
    # New format with timestamp
    if DateTime.compare(last_access, cutoff) == :lt do
      :ets.delete(@client_offsets_table, client_id)
      acc + 1
    else
      acc
    end
  end

  defp process_client_offset_entry({client_id, _offsets}, acc, _cutoff) do
    # Old format without timestamp - remove to migrate to new format
    :ets.delete(@client_offsets_table, client_id)
    acc + 1
  end

  defp get_killmail_time(killmail_data) when is_map(killmail_data) do
    cond do
      # Check for kill_time field (canonical)
      Map.has_key?(killmail_data, "kill_time") ->
        parse_time(killmail_data["kill_time"])

      Map.has_key?(killmail_data, :kill_time) ->
        parse_time(killmail_data[:kill_time])

      # Check for killmail_time field (ESI format)
      Map.has_key?(killmail_data, "killmail_time") ->
        parse_time(killmail_data["killmail_time"])

      Map.has_key?(killmail_data, :killmail_time) ->
        parse_time(killmail_data[:killmail_time])

      true ->
        {:error, :no_time_field}
    end
  end

  defp parse_time(%DateTime{} = dt), do: {:ok, dt}

  defp parse_time(time_string) when is_binary(time_string) do
    case DateTime.from_iso8601(time_string) do
      {:ok, dt, _offset} -> {:ok, dt}
      error -> error
    end
  end

  defp parse_time(_), do: {:error, :invalid_time_format}

  defp remove_killmail_from_all_systems(killmail_id) do
    :ets.foldl(
      fn {system_id, killmail_ids}, _acc ->
        remove_killmail_from_system_if_present(system_id, killmail_ids, killmail_id)
      end,
      :ok,
      @system_killmails_table
    )
  end

  defp remove_killmail_from_system_if_present(system_id, killmail_ids, killmail_id) do
    if killmail_id in killmail_ids do
      new_ids = List.delete(killmail_ids, killmail_id)
      update_system_killmails_table(system_id, new_ids)
    end
  end

  defp update_system_killmails_table(system_id, []) do
    :ets.delete(@system_killmails_table, system_id)
  end

  defp update_system_killmails_table(system_id, new_ids) do
    :ets.insert(@system_killmails_table, {system_id, new_ids})
  end
end
