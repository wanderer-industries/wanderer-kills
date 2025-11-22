defmodule WandererKills.Ingest.Killmails.CharacterCacheTest do
  use WandererKills.UnifiedTestCase, async: false, type: :integration

  alias WandererKills.Ingest.Killmails.CharacterCache

  describe "extract_characters_cached/1" do
    test "extracts and caches characters from killmail" do
      killmail = %{
        "killmail_id" => 12_345,
        "victim" => %{"character_id" => 100},
        "attackers" => [
          %{"character_id" => 200},
          %{"character_id" => 300}
        ]
      }

      # First call should extract and cache
      result1 = CharacterCache.extract_characters_cached(killmail)
      assert Enum.sort(result1) == [100, 200, 300]

      # Second call should use cache
      result2 = CharacterCache.extract_characters_cached(killmail)
      assert result2 == result1

      # Verify the value is actually in cache
      stats = CharacterCache.get_cache_stats()
      assert stats.entries > 0
    end

    test "handles killmail without killmail_id" do
      killmail = %{
        "victim" => %{"character_id" => 100},
        "attackers" => []
      }

      # Should extract but not cache
      result = CharacterCache.extract_characters_cached(killmail)
      assert result == [100]
    end

    test "handles killmail with nil character data" do
      killmail = %{
        "killmail_id" => 12_346,
        "victim" => %{},
        "attackers" => []
      }

      result = CharacterCache.extract_characters_cached(killmail)
      assert result == []
    end
  end

  describe "batch_extract_cached/1" do
    test "processes multiple killmails with caching" do
      killmails = [
        %{
          "killmail_id" => 1,
          "victim" => %{"character_id" => 100},
          "attackers" => []
        },
        %{
          "killmail_id" => 2,
          "victim" => %{"character_id" => 200},
          "attackers" => [%{"character_id" => 201}]
        },
        %{
          # No killmail_id
          "victim" => %{"character_id" => 300},
          "attackers" => []
        }
      ]

      result = CharacterCache.batch_extract_cached(killmails)

      assert result[1] == [100]
      assert Enum.sort(result[2]) == [200, 201]
      # The one without killmail_id gets a generated key
      assert [300] in Map.values(result)
    end

    test "uses cache for repeated batch calls" do
      killmails = [
        %{
          "killmail_id" => 1,
          "victim" => %{"character_id" => 100},
          "attackers" => []
        }
      ]

      # First call
      result1 = CharacterCache.batch_extract_cached(killmails)

      # Second call should use cache
      result2 = CharacterCache.batch_extract_cached(killmails)

      assert result1 == result2
    end

    test "handles empty killmail list" do
      result = CharacterCache.batch_extract_cached([])
      assert result == %{}
    end
  end

  describe "warm_cache/1" do
    test "preloads cache with killmail data" do
      killmails = [
        %{
          "killmail_id" => 1001,
          "victim" => %{"character_id" => 100},
          "attackers" => []
        },
        %{
          "killmail_id" => 1002,
          "victim" => %{"character_id" => 200},
          "attackers" => []
        }
      ]

      # Warm the cache
      assert CharacterCache.warm_cache(killmails) == :ok

      # Verify data is cached by extracting without additional processing
      result1 = CharacterCache.extract_characters_cached(Enum.at(killmails, 0))
      assert result1 == [100]

      result2 = CharacterCache.extract_characters_cached(Enum.at(killmails, 1))
      assert result2 == [200]
    end

    test "skips killmails already in cache" do
      killmail = %{
        "killmail_id" => 1003,
        "victim" => %{"character_id" => 100},
        "attackers" => []
      }

      # First, manually cache the killmail
      CharacterCache.extract_characters_cached(killmail)

      # Warming should skip it
      assert CharacterCache.warm_cache([killmail]) == :ok
    end

    test "ignores killmails without killmail_id" do
      killmails = [
        %{
          "victim" => %{"character_id" => 100},
          "attackers" => []
        }
      ]

      assert CharacterCache.warm_cache(killmails) == :ok
    end
  end

  describe "get_cache_stats/0" do
    test "returns cache statistics" do
      stats = CharacterCache.get_cache_stats()

      assert is_map(stats)
      assert stats.namespace == "character_extraction"
      assert is_number(stats.hits) || stats.hits == 0
      assert is_number(stats.misses) || stats.misses == 0
      assert is_number(stats.hit_rate) || stats.hit_rate == 0.0
    end
  end

  describe "clear_cache/0" do
    test "removes all cached entries" do
      killmail = %{
        "killmail_id" => 2001,
        "victim" => %{"character_id" => 100},
        "attackers" => []
      }

      # Cache some data
      CharacterCache.extract_characters_cached(killmail)

      # Clear cache
      assert CharacterCache.clear_cache() == :ok

      # Verify cache is empty
      stats = CharacterCache.get_cache_stats()
      assert stats.entries == 0
    end
  end
end
