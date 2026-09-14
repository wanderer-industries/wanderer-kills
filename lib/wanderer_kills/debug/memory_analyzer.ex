defmodule WandererKills.Debug.MemoryAnalyzer do
  @moduledoc """
  Comprehensive memory analysis tool for WandererKills application.

  Analyzes memory usage patterns across:
  - ETS tables (killmails, subscriptions, caches)
  - GenServer processes
  - Phoenix WebSocket connections
  - Cachex cache instances
  """

  require Logger

  @doc """
  Runs a comprehensive memory analysis and returns detailed report.
  """
  def analyze do
    %{
      timestamp: DateTime.utc_now(),
      system: analyze_system_memory(),
      ets_tables: analyze_ets_memory(),
      processes: analyze_process_memory(),
      caches: analyze_cache_memory(),
      subscriptions: analyze_subscription_memory(),
      recommendations: generate_recommendations()
    }
  end

  @doc """
  Prints a formatted memory analysis report.
  """
  def report do
    analysis = analyze()

    IO.puts("\n=== WandererKills Memory Analysis Report ===")
    IO.puts("Timestamp: #{analysis.timestamp}")

    print_system_summary(analysis.system)
    print_ets_summary(analysis.ets_tables)
    print_process_summary(analysis.processes)
    print_cache_summary(analysis.caches)
    print_subscription_summary(analysis.subscriptions)
    print_recommendations(analysis.recommendations)

    analysis
  end

  @doc """
  Compares current memory usage with estimated pre-simplification usage.
  """
  def compare_with_baseline do
    current = analyze()
    baseline = estimate_pre_simplification_memory()

    %{
      current: current,
      baseline: baseline,
      savings: calculate_savings(current, baseline),
      improvements: identify_improvements(current, baseline)
    }
  end

  # ============================================================================
  # Analysis Functions
  # ============================================================================

  defp analyze_system_memory do
    memory_data = :erlang.memory()

    %{
      total_mb: bytes_to_mb(memory_data[:total]),
      processes_mb: bytes_to_mb(memory_data[:processes]),
      ets_mb: bytes_to_mb(memory_data[:ets]),
      atom_mb: bytes_to_mb(memory_data[:atom]),
      binary_mb: bytes_to_mb(memory_data[:binary]),
      code_mb: bytes_to_mb(memory_data[:code]),
      system_mb: bytes_to_mb(memory_data[:system])
    }
  end

  defp analyze_ets_memory do
    tables = [
      # Core storage tables
      {:killmails, "Main killmail storage"},
      {:system_killmails, "System-killmail associations"},
      {:system_kill_counts, "Kill count tracking"},
      {:system_fetch_timestamps, "Fetch timestamp tracking"},

      # Event streaming tables (if enabled)
      {:killmail_events, "Event streaming"},
      {:client_offsets, "Client offset tracking"},
      {:counters, "Event counters"},

      # Subscription indexes
      {:character_subscription_index, "Character subscriptions"},
      {:system_subscription_index, "System subscriptions"},

      # WebSocket stats
      {:websocket_stats, "WebSocket connection stats"},

      # Ship types cache
      {:ship_types, "Ship type definitions"}
    ]

    Enum.map(tables, fn {table_name, description} ->
      case :ets.info(table_name) do
        :undefined ->
          %{
            name: table_name,
            description: description,
            exists: false
          }

        info ->
          %{
            name: table_name,
            description: description,
            exists: true,
            size: Keyword.get(info, :size, 0),
            memory_words: Keyword.get(info, :memory, 0),
            memory_mb:
              bytes_to_mb(Keyword.get(info, :memory, 0) * :erlang.system_info(:wordsize)),
            type: Keyword.get(info, :type),
            read_concurrency: Keyword.get(info, :read_concurrency, false),
            write_concurrency: Keyword.get(info, :write_concurrency, false)
          }
      end
    end)
  end

  defp analyze_process_memory do
    processes = Process.list()

    # Get top memory consuming processes
    process_info =
      processes
      |> Enum.map(fn pid ->
        case Process.info(pid, [:registered_name, :memory, :message_queue_len, :current_function]) do
          nil ->
            nil

          info ->
            %{
              pid: pid,
              name: Keyword.get(info, :registered_name, :unnamed),
              memory_mb: bytes_to_mb(Keyword.get(info, :memory, 0)),
              message_queue_len: Keyword.get(info, :message_queue_len, 0),
              current_function: Keyword.get(info, :current_function)
            }
        end
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.sort_by(& &1.memory_mb, :desc)
      |> Enum.take(20)

    # Categorize processes
    categorized = categorize_processes(process_info)

    %{
      total_count: length(processes),
      top_consumers: process_info,
      by_category: categorized,
      genserver_memory_mb: calculate_category_memory(categorized.genservers),
      websocket_memory_mb: calculate_category_memory(categorized.websockets),
      supervisor_memory_mb: calculate_category_memory(categorized.supervisors)
    }
  end

  defp analyze_cache_memory do
    # Analyze Cachex cache
    cache_stats =
      try do
        Cachex.stats(:wanderer_cache)
      rescue
        _ -> {:error, :not_available}
      end

    cache_info =
      case cache_stats do
        {:ok, stats} ->
          %{
            hits: Map.get(stats, :hits, 0),
            misses: Map.get(stats, :misses, 0),
            evictions: Map.get(stats, :evictions, 0),
            operations: Map.get(stats, :operations, 0)
          }

        _ ->
          %{error: "Cache stats not available"}
      end

    # Estimate cache memory from ETS tables used by Cachex
    cachex_tables =
      :ets.all()
      |> Enum.filter(fn table ->
        case :ets.info(table, :name) do
          name when is_atom(name) ->
            String.contains?(Atom.to_string(name), "wanderer_cache")

          _ ->
            false
        end
      end)

    cache_memory_mb =
      cachex_tables
      |> Enum.reduce(0, fn table, acc ->
        case :ets.info(table, :memory) do
          nil -> acc
          memory -> acc + memory * :erlang.system_info(:wordsize)
        end
      end)
      |> bytes_to_mb()

    %{
      stats: cache_info,
      estimated_memory_mb: cache_memory_mb,
      table_count: length(cachex_tables)
    }
  end

  defp analyze_subscription_memory do
    char_stats = get_index_stats(:character_subscription_index)
    sys_stats = get_index_stats(:system_subscription_index)

    %{
      character_index: char_stats,
      system_index: sys_stats,
      total_memory_mb: (char_stats[:memory_mb] || 0) + (sys_stats[:memory_mb] || 0),
      total_subscriptions: (char_stats[:entries] || 0) + (sys_stats[:entries] || 0)
    }
  end

  defp get_index_stats(table_name) do
    case :ets.info(table_name) do
      :undefined ->
        %{exists: false}

      info ->
        %{
          exists: true,
          entries: Keyword.get(info, :size, 0),
          memory_words: Keyword.get(info, :memory, 0),
          memory_mb: bytes_to_mb(Keyword.get(info, :memory, 0) * :erlang.system_info(:wordsize))
        }
    end
  end

  # ============================================================================
  # Baseline Estimation
  # ============================================================================

  defp estimate_pre_simplification_memory do
    # Based on the removed modules and consolidated functionality
    %{
      removed_modules: [
        # Pipeline modules that were consolidated
        {:coordinator, 2.5},
        {:data_builder, 1.8},
        {:enricher, 2.2},
        {:parser, 1.5},
        {:validator, 1.2},

        # Client modules that were consolidated
        {:client, 3.0},
        {:client_behaviour, 0.5},

        # Test support modules
        {:cache_helpers, 0.8},
        {:ets_helpers, 0.7},
        {:http_helpers, 1.0},
        {:shared_contexts, 0.6},
        {:simple_generators, 0.5},

        # Subscription test modules
        {:base_index_test_modules, 4.0}
      ],
      # MB from code duplication
      duplicate_code_overhead: 5.0,
      # MB from extra GenServer processes
      extra_process_overhead: 8.0,
      # MB from redundant ETS structures
      redundant_ets_tables: 3.0
    }
  end

  defp calculate_savings(current, baseline) do
    total_baseline_overhead =
      Enum.reduce(baseline.removed_modules, 0, fn {_, mb}, acc -> acc + mb end) +
        baseline.duplicate_code_overhead +
        baseline.extra_process_overhead +
        baseline.redundant_ets_tables

    current_total = current.system.total_mb

    baseline_total = current_total + total_baseline_overhead

    percentage =
      if baseline_total > 0 do
        Float.round(total_baseline_overhead / baseline_total * 100.0, 2)
      else
        0.0
      end

    %{
      estimated_baseline_mb: baseline_total,
      current_mb: current_total,
      saved_mb: total_baseline_overhead,
      percentage_saved: percentage
    }
  end

  defp identify_improvements(current, _baseline) do
    improvements = []

    # Check ETS memory efficiency
    improvements = improvements ++ check_ets_efficiency(current.ets_tables)

    # Check process memory patterns
    improvements = improvements ++ check_process_patterns(current.processes)

    # Check subscription index efficiency
    improvements = improvements ++ check_subscription_efficiency(current.subscriptions)

    improvements
  end

  # ============================================================================
  # Analysis Helpers
  # ============================================================================

  defp categorize_processes(processes) do
    Enum.reduce(processes, %{genservers: [], websockets: [], supervisors: [], other: []}, fn proc,
                                                                                             acc ->
      cond do
        genserver?(proc) -> %{acc | genservers: [proc | acc.genservers]}
        websocket?(proc) -> %{acc | websockets: [proc | acc.websockets]}
        supervisor?(proc) -> %{acc | supervisors: [proc | acc.supervisors]}
        true -> %{acc | other: [proc | acc.other]}
      end
    end)
  end

  defp genserver?(proc) do
    case proc.name do
      name when is_atom(name) ->
        name_str = Atom.to_string(name)

        String.contains?(name_str, "Server") or
          String.contains?(name_str, "Worker") or
          String.contains?(name_str, "Manager")

      _ ->
        false
    end
  end

  defp websocket?(proc) do
    case proc.current_function do
      {module, _, _} ->
        module_str = inspect(module)

        String.contains?(module_str, "Channel") or
          String.contains?(module_str, "Socket")

      _ ->
        false
    end
  end

  defp supervisor?(proc) do
    case proc.name do
      name when is_atom(name) ->
        String.contains?(Atom.to_string(name), "Supervisor")

      _ ->
        false
    end
  end

  defp calculate_category_memory(processes) do
    total =
      processes
      |> Enum.reduce(0, fn proc, acc -> acc + proc.memory_mb end)

    Float.round(total * 1.0, 2)
  end

  defp check_ets_efficiency(tables) do
    tables
    |> Enum.filter(& &1[:exists])
    |> Enum.flat_map(&analyze_table/1)
  end

  defp analyze_table(table) do
    oversized_warning = check_table_size(table)
    density_warning = check_table_density(table)

    Enum.reject([oversized_warning, density_warning], &is_nil/1)
  end

  defp check_table_size(table) do
    if table[:memory_mb] && table[:memory_mb] > 100 do
      "Table #{table.name} using #{table.memory_mb}MB - consider cleanup or partitioning"
    end
  end

  defp check_table_density(table) do
    with true <- table[:size] && table[:memory_mb] && table[:size] > 0,
         bytes_per_entry <- table[:memory_mb] * 1024 * 1024 / table[:size],
         true <- bytes_per_entry > 1024 do
      "Table #{table.name} has high memory per entry (#{round(bytes_per_entry)} bytes)"
    else
      _ -> nil
    end
  end

  defp check_process_patterns(process_data) do
    improvements = []

    # Check for high message queue processes
    high_queue =
      Enum.filter(process_data.top_consumers, fn proc ->
        proc.message_queue_len > 1000
      end)

    improvements =
      if high_queue != [] do
        improvements ++ ["#{length(high_queue)} processes have high message queues (>1000)"]
      else
        improvements
      end

    # Check WebSocket memory usage
    improvements =
      if process_data.websocket_memory_mb > 100 do
        improvements ++
          [
            "WebSocket processes using #{process_data.websocket_memory_mb}MB - consider connection limits"
          ]
      else
        improvements
      end

    improvements
  end

  defp check_subscription_efficiency(sub_data) do
    improvements = []

    # Check character index efficiency
    improvements =
      if sub_data.character_index[:exists] && sub_data.character_index[:entries] > 0 do
        bytes_per_char =
          sub_data.character_index[:memory_mb] * 1024 * 1024 / sub_data.character_index[:entries]

        if bytes_per_char > 100 do
          improvements ++
            ["Character index using #{round(bytes_per_char)} bytes per entry"]
        else
          improvements
        end
      else
        improvements
      end

    # Check system index efficiency
    improvements =
      if sub_data.system_index[:exists] && sub_data.system_index[:entries] > 0 do
        bytes_per_sys =
          sub_data.system_index[:memory_mb] * 1024 * 1024 / sub_data.system_index[:entries]

        if bytes_per_sys > 100 do
          improvements ++ ["System index using #{round(bytes_per_sys)} bytes per entry"]
        else
          improvements
        end
      else
        improvements
      end

    improvements
  end

  # ============================================================================
  # Recommendations
  # ============================================================================

  defp generate_recommendations do
    [
      %{
        category: :monitoring,
        recommendations: [
          "Set up alerts for ETS memory > 1GB",
          "Monitor WebSocket connection count and memory",
          "Track subscription index growth rate",
          "Enable memory monitoring dashboard"
        ]
      },
      %{
        category: :thresholds,
        recommendations: [
          "MEMORY_THRESHOLD_MB=1000 (current default)",
          "EMERGENCY_MEMORY_THRESHOLD_MB=1500 (current default)",
          "KILLMAIL_RETENTION_DAYS=2 (configurable)",
          "WebSocket max connections: 10,000"
        ]
      },
      %{
        category: :optimization,
        recommendations: [
          "Consider partitioning killmails table by time",
          "Implement subscription index compression",
          "Add memory-based circuit breakers",
          "Use binary references for duplicate data"
        ]
      }
    ]
  end

  # ============================================================================
  # Formatting Helpers
  # ============================================================================

  defp print_system_summary(system) do
    IO.puts("\n## System Memory Summary")
    IO.puts("Total: #{system.total_mb} MB")
    IO.puts("Processes: #{system.processes_mb} MB")
    IO.puts("ETS: #{system.ets_mb} MB")
    IO.puts("Binary: #{system.binary_mb} MB")
    IO.puts("Code: #{system.code_mb} MB")
    IO.puts("Atoms: #{system.atom_mb} MB")
  end

  defp print_ets_summary(tables) do
    IO.puts("\n## ETS Table Memory Usage")

    existing_tables = Enum.filter(tables, & &1[:exists])

    total_ets_mb =
      Enum.reduce(existing_tables, 0, fn t, acc ->
        acc + (t[:memory_mb] || 0)
      end)

    IO.puts("Total ETS Memory: #{Float.round(total_ets_mb * 1.0, 2)} MB")
    IO.puts("\nTable Details:")

    Enum.each(existing_tables, fn table ->
      IO.puts("  #{table.name}: #{table[:memory_mb] || 0} MB (#{table[:size] || 0} entries)")
    end)
  end

  defp print_process_summary(processes) do
    IO.puts("\n## Process Memory Summary")
    IO.puts("Total Processes: #{processes.total_count}")
    IO.puts("GenServers: #{processes.genserver_memory_mb} MB")
    IO.puts("WebSockets: #{processes.websocket_memory_mb} MB")
    IO.puts("Supervisors: #{processes.supervisor_memory_mb} MB")

    IO.puts("\nTop 5 Memory Consumers:")

    processes.top_consumers
    |> Enum.take(5)
    |> Enum.each(fn proc ->
      IO.puts("  #{inspect(proc.name)}: #{proc.memory_mb} MB")
    end)
  end

  defp print_cache_summary(caches) do
    IO.puts("\n## Cache Memory Usage")
    IO.puts("Estimated Cache Memory: #{caches.estimated_memory_mb} MB")
    IO.puts("Cache Tables: #{caches.table_count}")

    case caches.stats do
      %{error: _} ->
        IO.puts("Cache stats not available")

      stats ->
        IO.puts("Cache Hits: #{stats.hits}")
        IO.puts("Cache Misses: #{stats.misses}")
    end
  end

  defp print_subscription_summary(subs) do
    IO.puts("\n## Subscription Index Memory")
    IO.puts("Total Subscription Memory: #{Float.round(subs.total_memory_mb * 1.0, 2)} MB")

    if subs.character_index[:exists] do
      IO.puts(
        "Character Index: #{subs.character_index[:memory_mb]} MB (#{subs.character_index[:entries]} entries)"
      )
    end

    if subs.system_index[:exists] do
      IO.puts(
        "System Index: #{subs.system_index[:memory_mb]} MB (#{subs.system_index[:entries]} entries)"
      )
    end
  end

  defp print_recommendations(recommendations) do
    IO.puts("\n## Recommendations")

    Enum.each(recommendations, fn rec ->
      IO.puts("\n### #{String.capitalize(Atom.to_string(rec.category))}")

      Enum.each(rec.recommendations, fn r ->
        IO.puts("  - #{r}")
      end)
    end)
  end

  defp bytes_to_mb(bytes) when is_number(bytes) do
    mb = bytes / 1024 / 1024
    Float.round(mb * 1.0, 2)
  end

  defp bytes_to_mb(_), do: 0.0
end
