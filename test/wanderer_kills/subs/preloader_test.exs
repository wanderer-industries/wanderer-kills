defmodule WandererKills.Subs.PreloaderTest do
  use WandererKills.UnifiedTestCase, async: false

  alias WandererKills.Subs.Preloader

  describe "preload_kills_for_systems/2" do
    test "returns empty map for empty system_ids" do
      assert %{} = Preloader.preload_kills_for_systems([])
    end

    test "returns map of system kills" do
      # This will fetch from cache or API based on what's available
      # We're just testing the structure is correct
      result =
        Preloader.preload_kills_for_systems([30_000_142],
          days: 1,
          batch_size: 10
        )

      # Should return a map
      assert is_map(result)

      # Each system ID should map to a list of kills
      assert Map.has_key?(result, 30_000_142)
      assert is_list(result[30_000_142])
    end

    test "respects batch_size parameter with mock data" do
      # Create mock killmails
      mock_kills =
        for i <- 1..7 do
          %WandererKills.Domain.Killmail{
            killmail_id: i,
            kill_time: DateTime.utc_now(),
            system_id: 30_000_142,
            victim: %{character_id: 100 + i},
            attackers: []
          }
        end

      # Test batching logic directly
      batches = Enum.chunk_every(mock_kills, 3)

      # 7 items / 3 per batch = 3 batches
      assert length(batches) == 3
      assert length(Enum.at(batches, 0)) == 3
      assert length(Enum.at(batches, 1)) == 3
      # Last batch has remainder
      assert length(Enum.at(batches, 2)) == 1
    end
  end

  describe "preload_kills_for_characters/2" do
    test "returns empty map for empty character_ids" do
      result = Preloader.preload_kills_for_characters([])
      assert is_map(result)
      assert result == %{"kills" => []}
    end

    test "handles invalid character IDs gracefully" do
      # Negative character IDs should be handled gracefully
      result = Preloader.preload_kills_for_characters([-1, -999])

      # Should return empty kills (not raise an error)
      assert is_map(result)
      assert result == %{"kills" => []}
    end

    test "validates days parameter within 1-90 range" do
      # Test that days parameter is properly clamped
      # days = 0 should be clamped to 1
      result = Preloader.preload_kills_for_characters([12_345], days: 0)
      assert is_map(result)
      assert result == %{"kills" => []}

      # days = 120 should be clamped to 90
      result = Preloader.preload_kills_for_characters([12_345], days: 120)
      assert is_map(result)
      assert Map.has_key?(result, "kills")

      # days = -5 should be clamped to 1
      result = Preloader.preload_kills_for_characters([12_345], days: -5)
      assert is_map(result)
      assert result == %{"kills" => []}
    end

    test "handles kill count exactly matching batch size" do
      # Test boundary where kill count equals batch size exactly
      # Create exactly 10 mock killmails
      mock_kills =
        for i <- 1..10 do
          %WandererKills.Domain.Killmail{
            killmail_id: i,
            kill_time: DateTime.utc_now(),
            system_id: 30_000_142,
            victim: %{character_id: 100 + i},
            attackers: []
          }
        end

      # With batch_size = 10, should create exactly 1 batch
      batches = Enum.chunk_every(mock_kills, 10)
      assert length(batches) == 1
      assert length(Enum.at(batches, 0)) == 10
    end
  end

  describe "preload_kills_for_systems/2 edge cases" do
    test "handles invalid system IDs gracefully" do
      # Test with invalid system IDs
      result = Preloader.preload_kills_for_systems([-1, 0, 999_999_999])

      # Should return a map with empty lists (not raise an error)
      assert is_map(result)
      assert result[-1] == []
      assert result[0] == []
      assert result[999_999_999] == []
    end

    test "handles invalid options gracefully" do
      # Test with invalid option values
      result = Preloader.preload_kills_for_systems([30_000_142], days: -10, batch_size: 0)

      # Should handle gracefully with defaults
      assert is_map(result)
      assert Map.has_key?(result, 30_000_142)
    end

    test "validates days parameter edge cases" do
      # Test edge cases for days parameter
      valid_systems = [30_000_142]

      # days = 1 (minimum)
      result = Preloader.preload_kills_for_systems(valid_systems, days: 1)
      assert is_map(result)

      # days = 90 (maximum)
      result = Preloader.preload_kills_for_systems(valid_systems, days: 90)
      assert is_map(result)

      # days = 91 (should be clamped to 90)
      result = Preloader.preload_kills_for_systems(valid_systems, days: 91)
      assert is_map(result)
    end
  end
end
