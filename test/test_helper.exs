# Start ExUnit first
ExUnit.start()

# Ensure the application is started before any tests run
# This is critical for integration tests that rely on supervisors
{:ok, _} = Application.ensure_all_started(:wanderer_kills)

# Ensure Mox is available and define mocks conditionally
mox_available =
  case Application.ensure_all_started(:mox) do
    {:ok, _} ->
      # Define mocks
      Mox.defmock(WandererKills.Http.ClientMock,
        for: WandererKills.Http.ClientBehaviour
      )

      Mox.defmock(WandererKills.Ingest.Killmails.ZkbClient.Mock,
        for: WandererKills.Ingest.Killmails.ZkbClientBehaviour
      )

      Mox.defmock(EsiClientMock, for: WandererKills.Ingest.ESI.ClientBehaviour)
      true

    {:error, reason} ->
      IO.puts("Warning: Failed to start mox: #{inspect(reason)}")
      false
  end

# Application already started above

# Configure ExUnit for parallel execution and exclude performance tests by default
ExUnit.configure(
  parallel: true,
  max_cases: System.schedulers_online() * 2,
  exclude: [:perf]
)

# Create basic test case modules that use the now-loaded support modules
defmodule WandererKills.TestCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      # Import common test utilities
      import WandererKills.TestHelpers
      import WandererKills.TestFactory
      import Mox

      # Setup mocks by default
      setup :verify_on_exit!

      setup do
        # Set Mox to global mode for cross-process calls
        Mox.set_mox_global()

        # Set up unique test environment
        unique_id = System.unique_integer([:positive])
        Process.put(:test_unique_id, unique_id)

        # Clear any existing processes and caches
        WandererKills.TestHelpers.clear_all_caches()

        %{test_id: unique_id}
      end

      # Make common aliases available
      alias WandererKills.Core.Cache
      alias WandererKills.TestFactory
      alias WandererKills.TestHelpers
    end
  end
end

defmodule WandererKills.DataCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      # Import common test utilities
      import WandererKills.TestHelpers
      import WandererKills.TestContexts
      import WandererKills.TestFactory
      import Mox

      # Setup mocks by default
      setup :verify_on_exit!

      setup context do
        alias WandererKills.Subs.SimpleSubscriptionManager
        alias WandererKills.Subs.{CharacterIndex, SystemIndex}

        # Set up unique test environment
        unique_id = System.unique_integer([:positive])
        Process.put(:test_unique_id, unique_id)

        # Clear caches on setup
        WandererKills.TestHelpers.clear_all_caches()

        # Setup mocks if not disabled
        unless context[:no_mocks] do
          WandererKills.TestHelpers.setup_mocks()
        end

        # Clear subscription indexes if needed
        if context[:clear_indexes] do
          try do
            CharacterIndex.clear()
            SystemIndex.clear()
          rescue
            _ -> :ok
          catch
            :exit, _ -> :ok
          end
        end

        # Clear all subscriptions if needed
        if context[:clear_subscriptions] do
          try do
            SimpleSubscriptionManager.clear_all_subscriptions()
          rescue
            # Ignore if the function doesn't exist
            UndefinedFunctionError -> :ok
          end
        end

        %{test_id: unique_id}
      end

      # Make common aliases available
      alias WandererKills.Core.Cache
      alias WandererKills.TestFactory
      alias WandererKills.TestHelpers
    end
  end
end

defmodule WandererKills.IntegrationCase do
  use ExUnit.CaseTemplate

  using do
    quote do
      # Import all test utilities
      import WandererKills.TestHelpers
      import WandererKills.TestContexts
      import WandererKills.TestFactory
      import Mox

      # Setup mocks and unique environment
      setup :verify_on_exit!

      setup do
        alias WandererKills.Subs.{CharacterIndex, SystemIndex}

        # Set up unique test environment with all features
        unique_id = System.unique_integer([:positive])
        Process.put(:test_unique_id, unique_id)

        # Full cleanup and setup
        WandererKills.TestHelpers.clear_all_caches()

        # Always set up mocks for integration tests
        WandererKills.TestHelpers.setup_mocks()

        # Clear indexes and subscriptions
        try do
          CharacterIndex.clear()
          SystemIndex.clear()
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end

        # Provide common integration test context
        %{
          test_id: unique_id,
          killmail_data:
            WandererKills.TestFactory.build_killmail(
              WandererKills.TestFactory.random_killmail_id()
            ),
          system_id: WandererKills.TestFactory.random_system_id(),
          character_id: WandererKills.TestFactory.random_character_id()
        }
      end

      # Make all aliases available
      alias WandererKills.Core.Cache
      alias WandererKills.TestFactory
      alias WandererKills.TestHelpers
    end
  end
end
