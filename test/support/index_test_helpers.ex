defmodule IndexTestHelpers do
  @moduledoc """
  Shared test helpers for subscription index testing.

  Provides parameterized test patterns that work across different entity types
  (character, system) to eliminate test code duplication while maintaining
  comprehensive coverage of index functionality.

  ## Usage

      defmodule MyEntityIndexTest do
        use ExUnit.Case, async: false
        import IndexTestHelpers
        
        setup do
          setup_index(MyEntityIndex, [123, 456, 789])
        end
        
        test "basic operations", %{index_module: index_module, test_entities: entities} do
          test_basic_operations(index_module, entities)
        end
      end

  ## Test Patterns

  - **Basic Operations**: Add/remove subscriptions, entity lookups
  - **Edge Cases**: Empty results, invalid IDs, duplicate handling
  - **Performance**: Timing tests, memory usage, large dataset handling
  - **Concurrency**: Multiple GenServer interactions, race conditions
  - **Stats**: Metrics accuracy, memory tracking, deduplication efficiency
  """

  import ExUnit.Assertions
  import ExUnit.Callbacks

  @doc """
  Sets up an index module for testing with optional test entities.

  Returns a test context map with:
  - `index_module`: The module being tested
  - `test_entities`: List of entity IDs for testing
  - `pid`: GenServer PID for cleanup
  """
  def setup_index(index_module, test_entities \\ [123, 456, 789]) do
    # Handle case where GenServer is already started (named servers)
    pid =
      case index_module.start_link([]) do
        {:ok, pid} ->
          on_exit(fn ->
            GenServer.stop(pid, :normal, 1000)
          end)

          pid

        {:error, {:already_started, pid}} ->
          # Already started, just use existing process
          pid

        {:error, reason} ->
          raise "Failed to start index #{inspect(index_module)}: #{inspect(reason)}"
      end

    # Clear any existing data
    index_module.clear()

    %{
      index_module: index_module,
      test_entities: test_entities,
      pid: pid
    }
  end

  @doc """
  Tests basic subscription operations: add, find, remove, clear.
  """
  def test_basic_operations(index_module, test_entities) do
    [entity1, entity2, entity3] = Enum.take(test_entities, 3)

    # Initially empty
    assert index_module.find_subscriptions_for_entity(entity1) == []
    assert index_module.find_subscriptions_for_entities([entity1, entity2]) == []

    # Add subscription
    :ok = index_module.add_subscription("sub_1", [entity1, entity2])

    # Find subscriptions
    assert index_module.find_subscriptions_for_entity(entity1) == ["sub_1"]
    assert index_module.find_subscriptions_for_entity(entity2) == ["sub_1"]
    assert index_module.find_subscriptions_for_entity(entity3) == []

    # Find for multiple entities
    subscriptions = index_module.find_subscriptions_for_entities([entity1, entity2, entity3])
    assert subscriptions == ["sub_1"]

    # Add another subscription
    :ok = index_module.add_subscription("sub_2", [entity2, entity3])

    # Verify deduplication
    subscriptions = index_module.find_subscriptions_for_entities([entity1, entity2, entity3])
    assert MapSet.new(subscriptions) == MapSet.new(["sub_1", "sub_2"])

    # Remove subscription
    :ok = index_module.remove_subscription("sub_1")

    assert index_module.find_subscriptions_for_entity(entity1) == []
    assert index_module.find_subscriptions_for_entity(entity2) == ["sub_2"]
    assert index_module.find_subscriptions_for_entity(entity3) == ["sub_2"]

    # Clear all
    :ok = index_module.clear()

    assert index_module.find_subscriptions_for_entity(entity2) == []
    assert index_module.find_subscriptions_for_entity(entity3) == []
  end

  @doc """
  Tests edge cases: empty lists, non-existent IDs, duplicate subscriptions.
  """
  def test_edge_cases(index_module, test_entities) do
    [entity1, entity2 | _] = test_entities

    # Empty entity list
    :ok = index_module.add_subscription("sub_empty", [])
    assert index_module.find_subscriptions_for_entity(entity1) == []

    # Non-existent entity
    assert index_module.find_subscriptions_for_entity(999_999) == []
    assert index_module.find_subscriptions_for_entities([999_999, 888_888]) == []

    # Duplicate subscription (should replace by removing then adding)
    :ok = index_module.add_subscription("sub_1", [entity1])
    :ok = index_module.remove_subscription("sub_1")
    :ok = index_module.add_subscription("sub_1", [entity2])

    assert index_module.find_subscriptions_for_entity(entity1) == []
    assert index_module.find_subscriptions_for_entity(entity2) == ["sub_1"]

    # Remove non-existent subscription (should not crash)
    :ok = index_module.remove_subscription("non_existent")

    # Empty find_subscriptions_for_entities
    assert index_module.find_subscriptions_for_entities([]) == []
  end

  @doc """
  Tests statistics accuracy and memory tracking.
  """
  def test_statistics(index_module, test_entities) do
    [entity1, entity2, entity3] = Enum.take(test_entities, 3)

    # Initial stats
    stats = index_module.get_stats()
    assert stats.total_subscriptions == 0
    assert is_number(stats.memory_usage_bytes)

    # Add subscriptions and verify stats
    :ok = index_module.add_subscription("sub_1", [entity1, entity2])
    :ok = index_module.add_subscription("sub_2", [entity2, entity3])
    :ok = index_module.add_subscription("sub_3", [entity1, entity3])

    stats = index_module.get_stats()
    assert stats.total_subscriptions == 3

    # Verify entity-specific counts (depends on entity type)
    total_entity_subscriptions = get_entity_subscriptions_count(stats, index_module)
    # 2 + 2 + 2
    assert total_entity_subscriptions == 6

    total_entity_entries = get_entity_entries_count(stats, index_module)
    # Unique entities: entity1, entity2, entity3
    assert total_entity_entries == 3

    # Memory should be positive
    assert stats.memory_usage_bytes > 0
  end

  @doc """
  Tests performance characteristics under load.
  """
  def test_performance(index_module, test_entities) do
    # Add substantial data
    for i <- 1..100 do
      entities = Enum.take_random(test_entities, 2)
      index_module.add_subscription("sub_#{i}", entities)
    end

    # Test lookup performance
    [test_entity | _] = test_entities

    {time, _result} =
      :timer.tc(fn ->
        index_module.find_subscriptions_for_entity(test_entity)
      end)

    # Performance assertions only run when PERF_TEST env var is set
    if System.get_env("PERF_TEST") do
      # Should complete in under 50ms even with 100 subscriptions (relaxed from 10ms)
      assert time < 50_000
    end

    # Test batch lookup performance
    {time, _result} =
      :timer.tc(fn ->
        index_module.find_subscriptions_for_entities(test_entities)
      end)

    if System.get_env("PERF_TEST") do
      # Batch lookup should also be fast - relaxed from 50ms to 100ms
      assert time < 100_000
    end

    # Memory assertions only run when PERF_TEST env var is set
    if System.get_env("PERF_TEST") do
      stats = index_module.get_stats()
      memory_mb = stats.memory_usage_bytes / (1024 * 1024)
      # Under 10MB for 100 subscriptions
      assert memory_mb < 10
    end
  end

  @doc """
  Tests concurrent access patterns and GenServer reliability.
  """
  def test_concurrency(index_module, test_entities) do
    [entity1, entity2 | _] = test_entities

    # Spawn multiple processes doing concurrent operations
    tasks =
      for i <- 1..10 do
        entities = if rem(i, 2) == 0, do: [entity1], else: [entity2]

        Task.async(fn ->
          # Each task adds/removes its own subscription
          subscription_id = "concurrent_sub_#{i}"

          :ok = index_module.add_subscription(subscription_id, entities)

          # Quick lookup
          _result = index_module.find_subscriptions_for_entity(entity1)

          # Remove subscription
          :ok = index_module.remove_subscription(subscription_id)

          :ok
        end)
      end

    # Wait for all tasks to complete
    results = Task.await_many(tasks, 5000)

    # All should succeed
    assert Enum.all?(results, &(&1 == :ok))

    # Index should be in consistent state
    stats = index_module.get_stats()
    assert stats.total_subscriptions == 0
  end

  @doc """
  Tests health check integration and metrics accuracy.
  """
  def test_health_integration(health_module, index_module, test_entities) do
    [entity1, entity2 | _] = test_entities

    # Initial health check
    health = health_module.check_health()
    assert health.healthy == true
    assert health.status == "healthy"

    # Add some data
    :ok = index_module.add_subscription("health_sub_1", [entity1, entity2])
    :ok = index_module.add_subscription("health_sub_2", [entity2])

    # Health check with data
    health = health_module.check_health()
    assert health.healthy == true

    # Verify health check components
    checks = health.details.checks
    assert Map.has_key?(checks, :index_availability)
    assert Map.has_key?(checks, :index_performance)
    assert Map.has_key?(checks, :memory_usage)
    assert Map.has_key?(checks, :subscription_counts)

    # All should be healthy for small dataset
    assert checks.index_availability.status == :healthy
    assert checks.index_performance.status == :healthy
    assert checks.memory_usage.status == :healthy
    assert checks.subscription_counts.status == :healthy

    # Test metrics collection
    metrics = health_module.get_metrics()
    assert metrics.component =~ "_subscriptions"

    m = metrics.metrics
    assert m.total_subscriptions == 2
    assert m.total_entity_entries == 2
    assert is_number(m.memory_usage_bytes)
    assert is_number(m.avg_entities_per_subscription)
  end

  @doc """
  Comprehensive test suite runner that executes all test patterns.
  """
  def run_comprehensive_tests(index_module, health_module, test_entities) do
    index_module.clear()
    test_basic_operations(index_module, test_entities)

    index_module.clear()
    test_edge_cases(index_module, test_entities)

    index_module.clear()
    test_statistics(index_module, test_entities)

    index_module.clear()
    test_performance(index_module, test_entities)

    index_module.clear()
    test_concurrency(index_module, test_entities)

    index_module.clear()
    test_health_integration(health_module, index_module, test_entities)
  end

  # ============================================================================
  # Private Helper Functions
  # ============================================================================

  # Extracts entity-specific subscription count from stats
  defp get_entity_subscriptions_count(stats, index_module) do
    entity_type = get_entity_type(index_module)

    key =
      case entity_type do
        "character" -> :total_character_subscriptions
        "system" -> :total_system_subscriptions
        _ -> :total_entity_subscriptions
      end

    Map.get(stats, key, 0)
  end

  # Extracts entity-specific entry count from stats
  defp get_entity_entries_count(stats, index_module) do
    entity_type = get_entity_type(index_module)

    key =
      case entity_type do
        "character" -> :total_character_entries
        "system" -> :total_system_entries
        _ -> :total_entity_entries
      end

    Map.get(stats, key, 0)
  end

  # Determines entity type from module name
  defp get_entity_type(index_module) do
    module_name = index_module |> Module.split() |> List.last()

    cond do
      String.contains?(module_name, "Character") -> "character"
      String.contains?(module_name, "System") -> "system"
      # Generic fallback
      true -> "entity"
    end
  end
end
