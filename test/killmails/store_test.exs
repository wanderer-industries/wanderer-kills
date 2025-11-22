defmodule WandererKills.Ingest.Killmails.StoreTest do
  use WandererKills.UnifiedTestCase, async: false, type: :integration

  alias WandererKills.Core.Storage.KillmailStore
  alias WandererKills.TestHelpers

  @moduletag area: :killmail_storage

  @system_id_1 30_000_142

  @test_killmail_1 %{
    "killmail_id" => 12_345,
    "solar_system_id" => @system_id_1,
    "victim" => %{"character_id" => 123},
    "attackers" => [],
    "zkb" => %{"totalValue" => 1000}
  }

  @test_killmail_2 %{
    "killmail_id" => 12_346,
    "solar_system_id" => @system_id_1,
    "victim" => %{"character_id" => 124},
    "attackers" => [],
    "zkb" => %{"totalValue" => 2000}
  }

  # Test setup is handled by UnifiedTestCase

  describe "killmail operations" do
    test "can store and retrieve a killmail" do
      killmail = @test_killmail_1
      :ok = KillmailStore.put(12_345, @system_id_1, killmail)
      assert {:ok, ^killmail} = KillmailStore.get(12_345)
    end

    test "returns error for non-existent killmail" do
      assert {:error, _} = KillmailStore.get(999)
    end

    test "can delete a killmail" do
      killmail = TestHelpers.create_test_killmail(123)
      :ok = KillmailStore.put(123, @system_id_1, killmail)
      :ok = KillmailStore.delete(123)
      assert {:error, _} = KillmailStore.get(123)
    end
  end

  describe "system operations" do
    test "can store and retrieve system killmails" do
      killmail1 = Map.put(@test_killmail_1, "killmail_id", 123)
      killmail2 = Map.put(@test_killmail_2, "killmail_id", 456)

      assert :ok = KillmailStore.put(123, @system_id_1, killmail1)
      assert :ok = KillmailStore.put(456, @system_id_1, killmail2)

      killmails = KillmailStore.list_by_system(@system_id_1)
      killmail_ids = Enum.map(killmails, & &1["killmail_id"])
      assert Enum.sort(killmail_ids) == [123, 456]
    end

    test "returns empty list for system with no killmails" do
      killmails = KillmailStore.list_by_system(@system_id_1)
      assert killmails == []
    end

    test "can remove killmail from system" do
      killmail = Map.put(@test_killmail_1, "killmail_id", 123)
      assert :ok = KillmailStore.put(123, @system_id_1, killmail)
      assert :ok = KillmailStore.delete(123)

      killmails = KillmailStore.list_by_system(@system_id_1)
      assert killmails == []
    end
  end

  describe "edge cases" do
    test "handles non-existent system" do
      non_existent_system = 99_999_999

      # Fetch for non-existent system should return empty list
      killmails = KillmailStore.list_by_system(non_existent_system)
      assert killmails == []
    end

    test "handles multiple systems correctly" do
      system_2 = 30_000_143

      killmail1 = Map.put(@test_killmail_1, "killmail_id", 123)
      killmail2 = Map.put(@test_killmail_2, "killmail_id", 456)

      # Store killmails in different systems
      assert :ok = KillmailStore.put(123, @system_id_1, killmail1)
      assert :ok = KillmailStore.put(456, system_2, killmail2)

      # Each system should only return its own killmails
      system_1_killmails = KillmailStore.list_by_system(@system_id_1)
      system_2_killmails = KillmailStore.list_by_system(system_2)

      assert length(system_1_killmails) == 1
      assert length(system_2_killmails) == 1
      assert hd(system_1_killmails)["killmail_id"] == 123
      assert hd(system_2_killmails)["killmail_id"] == 456
    end

    test "handles killmail updates correctly" do
      killmail = @test_killmail_1
      updated_killmail = Map.put(killmail, "updated", true)

      # Store initial killmail
      assert :ok = KillmailStore.put(12_345, @system_id_1, killmail)
      assert {:ok, ^killmail} = KillmailStore.get(12_345)

      # Update with new data
      assert :ok = KillmailStore.put(12_345, @system_id_1, updated_killmail)
      assert {:ok, ^updated_killmail} = KillmailStore.get(12_345)
    end
  end
end
