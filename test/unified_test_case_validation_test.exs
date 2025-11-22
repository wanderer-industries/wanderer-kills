defmodule WandererKills.UnifiedTestCaseValidationTest do
  # Test various configurations of the unified test case

  defmodule BasicTest do
    use WandererKills.UnifiedTestCase, mocks: false

    test "basic test works", %{test_id: test_id} do
      assert is_integer(test_id)
    end
  end

  defmodule ConnTest do
    use WandererKills.UnifiedTestCase, type: :conn

    test "conn test provides connection", %{conn: conn} do
      assert %Plug.Conn{} = conn
    end
  end

  defmodule IntegrationTest do
    use WandererKills.UnifiedTestCase, type: :integration, mocks: false

    test "integration test provides extra context", context do
      assert Map.has_key?(context, :killmail_data)
      assert Map.has_key?(context, :system_id)
      assert Map.has_key?(context, :character_id)
    end
  end

  defmodule AsyncTest do
    use WandererKills.UnifiedTestCase, async: false

    test "async can be disabled" do
      assert true
    end
  end
end
