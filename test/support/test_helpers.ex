defmodule WandererKills.TestHelpers do
  @moduledoc """
  Consolidated test helper module providing common utilities for tests.

  This module merges functionality from:
  - General helpers
  - Cache helpers
  - ETS helpers
  - Data helpers (basic utilities)
  """

  import ExUnit.Assertions, only: [assert: 2]

  alias WandererKills.Http.ClientMock
  alias WandererKills.TestFactory

  # ============================================================================
  # Cache Helpers
  # ============================================================================

  @doc """
  Clears all caches used in the application.
  """
  def clear_all_caches do
    # Clear Cachex cache
    case Process.whereis(:wanderer_cache) do
      nil ->
        :ok

      _pid ->
        Cachex.clear(:wanderer_cache)
        :ok
    end

    # Clear any ETS tables if they exist
    clear_ets_tables()
  end

  @doc """
  Clears a specific cache namespace.
  """
  def clear_cache_namespace(namespace) do
    case Process.whereis(:wanderer_cache) do
      nil ->
        :ok

      _pid ->
        # Get all keys for the namespace and delete them
        stream = Cachex.stream(:wanderer_cache, [])

        stream
        |> Stream.filter(fn {key, _} -> String.starts_with?(key, "#{namespace}:") end)
        |> Stream.each(fn {key, _} -> Cachex.del(:wanderer_cache, key) end)
        |> Stream.run()

        :ok
    end
  end

  # ============================================================================
  # ETS Helpers
  # ============================================================================

  @doc """
  Clears all ETS tables used by the application.
  """
  def clear_ets_tables do
    tables = [
      :killmails,
      :system_killmails,
      :system_kill_counts,
      :system_fetch_timestamps,
      :killmail_events,
      :client_offsets,
      :counters,
      :character_subscriptions,
      :system_subscriptions,
      :ship_types
    ]

    Enum.each(tables, &clear_ets_table/1)

    # Also clear subscription-specific tables
    clear_subscription_ets_tables()
  end

  defp clear_ets_table(table_name) do
    case :ets.whereis(table_name) do
      :undefined ->
        :ok

      _tid ->
        :ets.delete_all_objects(table_name)
        :ok
    end
  rescue
    _ -> :ok
  end

  @doc """
  Waits for an ETS table to exist, useful in async tests.
  """
  def wait_for_ets_table(table_name, timeout \\ 1000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      case :ets.whereis(table_name) do
        :undefined ->
          Process.sleep(10)
          :retry

        tid ->
          {:ok, tid}
      end
    end)
    |> Stream.take_while(fn
      :retry -> System.monotonic_time(:millisecond) < deadline
      _ -> false
    end)
    |> Enum.reduce(:retry, fn
      {:ok, tid}, _ -> {:ok, tid}
      :retry, _ -> :retry
    end)
    |> case do
      {:ok, tid} -> {:ok, tid}
      :retry -> {:error, :timeout}
    end
  end

  # ============================================================================
  # Mock Setup
  # ============================================================================

  @doc """
  Sets up common mocks for testing.
  """
  def setup_mocks do
    # Try to set global mode, catch any errors if already set
    try do
      Mox.set_mox_global()
    catch
      :error, _ ->
        # Already in global mode or other error, that's fine
        :ok
    end

    setup_http_client_mocks()
    setup_esi_client_mocks()
    setup_zkb_client_mocks()

    :ok
  end

  defp setup_http_client_mocks do
    # HTTP client mocks - Core.Http.Behaviour implementations
    Mox.stub(ClientMock, :get, &mock_http_get/3)
    Mox.stub(ClientMock, :get_with_rate_limit, &mock_http_get_with_rate_limit/3)

    Mox.stub(ClientMock, :post, fn _url, _body, _headers, _opts ->
      {:ok, %{status: 200, body: Jason.encode!(%{"data" => "mock_response"})}}
    end)

    Mox.stub(ClientMock, :get_esi, &mock_http_get_esi/3)
    Mox.stub(ClientMock, :get_zkb, &mock_http_get_zkb/3)
  end

  defp mock_http_get(url, _headers, _opts) do
    cond do
      String.contains?(url, "/universe/types/") ->
        mock_universe_types_response(url)

      String.contains?(url, "/universe/characters/") ->
        mock_universe_characters_response(url)

      String.contains?(url, "/universe/corporations/") ->
        mock_universe_corporations_response(url)

      true ->
        {:ok, %{status: 200, body: Jason.encode!(%{"data" => "mock_response"})}}
    end
  end

  defp mock_universe_types_response(url) do
    type_id = url |> String.split("/") |> List.last() |> String.replace("/", "")

    ship_data = %{
      "type_id" => String.to_integer(type_id),
      "name" => "Test Ship Type #{type_id}",
      "group_id" => 25,
      "category_id" => 6
    }

    {:ok, %{status: 200, body: Jason.encode!(ship_data)}}
  end

  defp mock_universe_characters_response(url) do
    char_id = url |> String.split("/") |> Enum.at(-2)

    char_data = %{
      "character_id" => String.to_integer(char_id),
      "name" => "Test Character #{char_id}",
      "corporation_id" => 98_765
    }

    {:ok, %{status: 200, body: Jason.encode!(char_data)}}
  end

  defp mock_universe_corporations_response(url) do
    corp_id = url |> String.split("/") |> Enum.at(-2)

    corp_data = %{
      "corporation_id" => String.to_integer(corp_id),
      "name" => "Test Corporation #{corp_id}",
      "alliance_id" => 54_321
    }

    {:ok, %{status: 200, body: Jason.encode!(corp_data)}}
  end

  defp mock_http_get_with_rate_limit(url, _headers, _opts) do
    cond do
      String.contains?(url, "characterID") ->
        {:ok, %{status: 200, body: Jason.encode!([])}}

      String.contains?(url, "systemID") ->
        {:ok, %{status: 200, body: Jason.encode!([])}}

      String.contains?(url, "zkb.test.local") ->
        {:ok, %{status: 200, body: Jason.encode!([])}}

      true ->
        {:ok, %{status: 200, body: Jason.encode!(%{"data" => "mock_response"})}}
    end
  end

  defp mock_http_get_esi(url, _headers, _opts) do
    # ESI mock logic - using if instead of cond with single condition
    if String.contains?(url, "esi.test.local") do
      {:ok, %{status: 200, body: Jason.encode!(%{"name" => "Test Entity"})}}
    else
      {:ok, %{status: 200, body: Jason.encode!(%{"data" => "mock_esi_response"})}}
    end
  end

  defp mock_http_get_zkb(url, _headers, _opts) do
    cond do
      String.contains?(url, "zkillboard.com/api/killID") ->
        mock_zkb_killmail_response(url)

      String.contains?(url, "redisq.zkillboard.com") ->
        {:ok, %{status: 200, body: Jason.encode!(%{package: nil})}}

      true ->
        {:ok, %{status: 200, body: Jason.encode!([])}}
    end
  end

  defp mock_zkb_killmail_response(url) do
    killmail_id = extract_killmail_id_from_url(url)

    if killmail_id do
      {:ok, %{status: 200, body: Jason.encode!([build_test_killmail(killmail_id)])}}
    else
      {:ok, %{status: 200, body: Jason.encode!([])}}
    end
  end

  defp setup_esi_client_mocks do
    # ESI client mocks - simple default implementations
    Mox.stub(EsiClientMock, :get_character, fn _character_id ->
      {:ok, %{"name" => "Test Character"}}
    end)

    Mox.stub(EsiClientMock, :get_type, fn _type_id ->
      {:ok, %{"name" => "Test Ship", "group_id" => 25}}
    end)

    # Batch operations
    Mox.stub(EsiClientMock, :get_character_batch, fn character_ids ->
      Enum.map(character_ids, fn id ->
        {:ok, %{"character_id" => id, "name" => "Test Character #{id}"}}
      end)
    end)

    Mox.stub(EsiClientMock, :get_corporation, fn _corp_id ->
      {:ok, %{"name" => "Test Corporation"}}
    end)

    Mox.stub(EsiClientMock, :get_corporation_batch, fn corp_ids ->
      Enum.map(corp_ids, fn id ->
        {:ok, %{"corporation_id" => id, "name" => "Test Corporation #{id}"}}
      end)
    end)

    Mox.stub(EsiClientMock, :get_alliance, fn _alliance_id ->
      {:ok, %{"name" => "Test Alliance"}}
    end)

    Mox.stub(EsiClientMock, :get_alliance_batch, fn alliance_ids ->
      Enum.map(alliance_ids, fn id ->
        {:ok, %{"alliance_id" => id, "name" => "Test Alliance #{id}"}}
      end)
    end)

    Mox.stub(EsiClientMock, :get_type_batch, fn type_ids ->
      Enum.map(type_ids, fn id ->
        {:ok, %{"type_id" => id, "name" => "Test Type #{id}", "group_id" => 25}}
      end)
    end)

    Mox.stub(EsiClientMock, :get_group, fn _group_id ->
      {:ok, %{"name" => "Test Group"}}
    end)

    Mox.stub(EsiClientMock, :get_group_batch, fn group_ids ->
      Enum.map(group_ids, fn id ->
        {:ok, %{"group_id" => id, "name" => "Test Group #{id}"}}
      end)
    end)

    Mox.stub(EsiClientMock, :get_system, fn _system_id ->
      {:ok, %{"name" => "Test System"}}
    end)

    Mox.stub(EsiClientMock, :get_system_batch, fn system_ids ->
      Enum.map(system_ids, fn id ->
        {:ok, %{"system_id" => id, "name" => "Test System #{id}"}}
      end)
    end)

    Mox.stub(EsiClientMock, :fetch, fn _args ->
      {:ok, %{"data" => "mock ESI response"}}
    end)
  end

  defp setup_zkb_client_mocks do
    # ZkbClient mocks
    Mox.stub(WandererKills.Ingest.Killmails.ZkbClient.Mock, :fetch_history, fn _date ->
      {:ok, %{"history" => []}}
    end)

    Mox.stub(
      WandererKills.Ingest.Killmails.ZkbClient.Mock,
      :fetch_system_killmails,
      fn _system_id ->
        {:ok, []}
      end
    )

    Mox.stub(
      WandererKills.Ingest.Killmails.ZkbClient.Mock,
      :fetch_system_killmails,
      fn _system_id, _opts ->
        {:ok, []}
      end
    )
  end

  @doc """
  Sets up a specific mock expectation.
  """
  def expect_mock(mock_module, function_name, return_value) do
    Mox.expect(mock_module, function_name, fn _ -> return_value end)
  end

  @doc """
  Clear ETS tables and ensure they exist for tests.
  """
  def clear_subscription_ets_tables do
    # List of subscription-specific ETS tables to clear/create
    tables = [:character_subscription_index, :system_subscription_index, :sse_connections]

    Enum.each(tables, fn table ->
      clear_or_create_table(table)
    end)

    :ok
  end

  defp clear_or_create_table(table) do
    case :ets.info(table) do
      :undefined ->
        # Create the table if it doesn't exist (for controller tests)
        if table == :sse_connections do
          try do
            :ets.new(table, [:set, :public, :named_table])
          rescue
            ArgumentError ->
              # Table might have been created by another process
              :ok
          end
        end

        :ok

      _ ->
        try do
          :ets.delete_all_objects(table)
        rescue
          ArgumentError ->
            # Table might not be accessible, ignore
            :ok
        end
    end
  end

  def expect_mock(mock_module, function_name, arity, return_value) do
    case arity do
      0 -> Mox.expect(mock_module, function_name, fn -> return_value end)
      1 -> Mox.expect(mock_module, function_name, fn _ -> return_value end)
      2 -> Mox.expect(mock_module, function_name, fn _, _ -> return_value end)
      3 -> Mox.expect(mock_module, function_name, fn _, _, _ -> return_value end)
      4 -> Mox.expect(mock_module, function_name, fn _, _, _, _ -> return_value end)
      5 -> Mox.expect(mock_module, function_name, fn _, _, _, _, _ -> return_value end)
      _ -> raise ArgumentError, "expect_mock/4 only supports arities 0-5"
    end
  end

  # ============================================================================
  # Mock Helper Functions
  # ============================================================================

  # Extracts killmail ID from a ZKB API URL.
  defp extract_killmail_id_from_url(url) do
    case Regex.run(~r/killID\/(\d+)/, url) do
      [_, id_str] -> String.to_integer(id_str)
      _ -> nil
    end
  end

  # Builds a test killmail for mocking purposes.
  defp build_test_killmail(killmail_id) do
    TestFactory.build_killmail(killmail_id)
  end

  # ============================================================================
  # Data Generation Helpers
  # ============================================================================

  @doc """
  Generates a random system ID.
  """
  def random_system_id, do: TestFactory.random_system_id()

  @doc """
  Generates a random character ID.
  """
  def random_character_id, do: TestFactory.random_character_id()

  @doc """
  Generates a random killmail ID.
  """
  def random_killmail_id, do: TestFactory.random_killmail_id()

  @doc """
  Creates a test killmail with the given ID.
  """
  def create_test_killmail(id \\ nil) do
    TestFactory.build_killmail(id || random_killmail_id())
  end

  @doc """
  Creates multiple test killmails.
  """
  def create_test_killmails(count) do
    Enum.map(1..count, fn _ -> create_test_killmail() end)
  end

  @doc """
  Generates test data of specified type.
  """
  def generate_test_data(:killmail, killmail_id) do
    TestFactory.build_killmail(killmail_id)
  end

  def generate_test_data(:system_killmail, system_id) do
    [
      TestFactory.build_killmail(random_killmail_id(), %{solar_system_id: system_id}),
      TestFactory.build_killmail(random_killmail_id(), %{solar_system_id: system_id})
    ]
  end

  # ============================================================================
  # Process Helpers
  # ============================================================================

  @doc """
  Ensures the TaskSupervisor is running, starting it if needed.
  This helper is useful in tests that require the WandererKills.TaskSupervisor.
  """
  def ensure_task_supervisor do
    case Process.whereis(WandererKills.TaskSupervisor) do
      nil ->
        ExUnit.Callbacks.start_supervised!({Task.Supervisor, name: WandererKills.TaskSupervisor})

      _pid ->
        :ok
    end
  end

  @doc """
  Waits for a GenServer to be registered with a given name.
  """
  def wait_for_process(name, timeout \\ 1000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      case Process.whereis(name) do
        nil ->
          Process.sleep(10)
          :retry

        pid ->
          {:ok, pid}
      end
    end)
    |> Stream.take_while(fn
      :retry -> System.monotonic_time(:millisecond) < deadline
      _ -> false
    end)
    |> Enum.reduce(:retry, fn
      {:ok, pid}, _ -> {:ok, pid}
      :retry, _ -> :retry
    end)
    |> case do
      {:ok, pid} -> {:ok, pid}
      :retry -> {:error, :timeout}
    end
  end

  @doc """
  Ensures a process is stopped if it exists.
  """
  def ensure_process_stopped(name) do
    case Process.whereis(name) do
      nil ->
        :ok

      pid ->
        Process.exit(pid, :kill)
        wait_for_process_death(name)
    end
  end

  defp wait_for_process_death(name, timeout \\ 100) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      case Process.whereis(name) do
        nil ->
          :ok

        _ ->
          Process.sleep(5)
          :retry
      end
    end)
    |> Stream.take_while(fn
      :retry -> System.monotonic_time(:millisecond) < deadline
      _ -> false
    end)
    |> Enum.reduce(:retry, fn
      :ok, _ -> :ok
      :retry, _ -> :retry
    end)
  end

  # ============================================================================
  # Test Environment Setup
  # ============================================================================

  @doc """
  Sets up a clean test environment with all common operations.
  """
  def setup_test_environment do
    clear_all_caches()
    clear_ets_tables()
    setup_mocks()
    :ok
  end

  @doc """
  Captures log messages during test execution.
  """
  defmacro capture_log(block) do
    quote do
      ExUnit.CaptureLog.capture_log(fn -> unquote(block) end)
    end
  end

  @doc """
  Asserts that a log message was produced.
  """
  def assert_log_contains(log_output, expected) do
    assert log_output =~ expected,
           "Expected log to contain '#{expected}', but got: #{log_output}"
  end
end
