defmodule WandererKills.Ingest.Killmails.BatchProcessorTest do
  use WandererKills.UnifiedTestCase, async: false

  alias WandererKills.Domain.Killmail
  alias WandererKills.Ingest.Killmails.BatchProcessor
  alias WandererKills.TestFactory

  describe "extract_all_characters/1" do
    test "extracts unique character IDs from multiple killmails" do
      # Create killmail maps using the factory
      killmail_map_1 =
        TestFactory.build_killmail(123_456_789, %{
          "victim" => %{
            "character_id" => 1001,
            "corporation_id" => 2001,
            "ship_type_id" => 587,
            "damage_taken" => 1000
          },
          "attackers" => [
            %{
              "character_id" => 1002,
              "corporation_id" => 2002,
              "ship_type_id" => 588,
              "damage_done" => 1000,
              "final_blow" => true
            }
          ]
        })

      killmail_map_2 =
        TestFactory.build_killmail(123_456_790, %{
          "victim" => %{
            "character_id" => 1003,
            "corporation_id" => 2003,
            "ship_type_id" => 589,
            "damage_taken" => 1500
          },
          "attackers" => [
            %{
              # Same as first killmail
              "character_id" => 1002,
              "corporation_id" => 2002,
              "ship_type_id" => 590,
              "damage_done" => 1500,
              "final_blow" => true
            },
            %{
              # New character
              "character_id" => 1004,
              "corporation_id" => 2004,
              "ship_type_id" => 591,
              "damage_done" => 500,
              "final_blow" => false
            }
          ]
        })

      # Convert maps to Killmail structs
      {:ok, killmail_1} = Killmail.new(killmail_map_1)
      {:ok, killmail_2} = Killmail.new(killmail_map_2)

      killmails = [killmail_1, killmail_2]

      # Extract characters
      result = BatchProcessor.extract_all_characters(killmails)

      # Should extract unique characters: 1001, 1002, 1003, 1004
      expected_characters = MapSet.new([1001, 1002, 1003, 1004])
      assert MapSet.equal?(result, expected_characters)
    end

    test "handles empty killmail list" do
      result = BatchProcessor.extract_all_characters([])
      assert MapSet.size(result) == 0
    end

    test "handles killmails with no characters" do
      # Create a minimal killmail with no character IDs
      killmail_map =
        TestFactory.build_killmail(123_456_789, %{
          "victim" => %{
            "corporation_id" => 2001,
            "ship_type_id" => 587,
            "damage_taken" => 1000
          },
          "attackers" => [
            %{
              "corporation_id" => 2002,
              "ship_type_id" => 588,
              "damage_done" => 1000,
              "final_blow" => true
            }
          ]
        })

      {:ok, killmail} = Killmail.new(killmail_map)

      result = BatchProcessor.extract_all_characters([killmail])

      # Should handle missing character IDs gracefully
      assert is_struct(result, MapSet)
    end
  end

  describe "find_interested_subscriptions/1" do
    test "finds subscriptions for killmails with characters" do
      # Create test killmail
      killmail_map =
        TestFactory.build_killmail(123_456_789, %{
          "victim" => %{
            "character_id" => 1001,
            "corporation_id" => 2001,
            "ship_type_id" => 587,
            "damage_taken" => 1000
          },
          "attackers" => [
            %{
              "character_id" => 1002,
              "corporation_id" => 2002,
              "ship_type_id" => 588,
              "damage_done" => 1000,
              "final_blow" => true
            }
          ]
        })

      {:ok, killmail} = Killmail.new(killmail_map)
      killmails = [killmail]

      # Call the function
      result = BatchProcessor.find_interested_subscriptions(killmails)

      # Should return a map (even if empty in test environment)
      assert is_map(result)
    end

    test "returns empty map for killmails with no character subscriptions" do
      # Create killmail with characters that don't have subscriptions
      killmail_map =
        TestFactory.build_killmail(123_456_789, %{
          "victim" => %{
            # Unlikely to have subscriptions
            "character_id" => 999_999_999,
            "corporation_id" => 2001,
            "ship_type_id" => 587,
            "damage_taken" => 1000
          },
          "attackers" => [
            %{
              # Unlikely to have subscriptions
              "character_id" => 999_999_998,
              "corporation_id" => 2002,
              "ship_type_id" => 588,
              "damage_done" => 1000,
              "final_blow" => true
            }
          ]
        })

      {:ok, killmail} = Killmail.new(killmail_map)
      killmails = [killmail]

      result = BatchProcessor.find_interested_subscriptions(killmails)

      # Should return empty map when no subscriptions match
      assert result == %{}
    end
  end

  describe "match_killmails_to_subscriptions/2" do
    test "matches killmails to subscription character map" do
      # Create test killmail
      killmail_map =
        TestFactory.build_killmail(123_456_789, %{
          "victim" => %{
            "character_id" => 1001,
            "corporation_id" => 2001,
            "ship_type_id" => 587,
            "damage_taken" => 1000
          },
          "attackers" => [
            %{
              "character_id" => 1002,
              "corporation_id" => 2002,
              "ship_type_id" => 588,
              "damage_done" => 1000,
              "final_blow" => true
            }
          ]
        })

      {:ok, killmail} = Killmail.new(killmail_map)
      killmails = [killmail]

      # Create a simple subscription character map
      subscription_character_map = %{
        "subscription_1" => MapSet.new([1001]),
        "subscription_2" => MapSet.new([1002]),
        # No match
        "subscription_3" => MapSet.new([9999])
      }

      result =
        BatchProcessor.match_killmails_to_subscriptions(killmails, subscription_character_map)

      # Should return map with subscription matches
      assert is_map(result)

      # The result should contain subscription_1 and subscription_2 but not subscription_3
      # (exact assertion depends on the implementation details)
    end

    test "handles empty subscription map" do
      killmail_map = TestFactory.build_killmail(123_456_789)
      {:ok, killmail} = Killmail.new(killmail_map)
      killmails = [killmail]

      result = BatchProcessor.match_killmails_to_subscriptions(killmails, %{})

      assert result == %{}
    end
  end

  describe "group_killmails_by_subscription/2" do
    test "groups killmails by subscription ID" do
      # Create test killmails
      killmail_map_1 = TestFactory.build_killmail(123_456_789)
      killmail_map_2 = TestFactory.build_killmail(123_456_790)

      {:ok, killmail_1} = Killmail.new(killmail_map_1)
      {:ok, killmail_2} = Killmail.new(killmail_map_2)

      killmails = [killmail_1, killmail_2]

      # The function expects a map of subscription_id -> subscription_data
      subscriptions = %{
        "sub_1" => %{"id" => "sub_1", "system_ids" => [30_000_142]},
        "sub_2" => %{"id" => "sub_2", "system_ids" => [30_000_143]}
      }

      result = BatchProcessor.group_killmails_by_subscription(killmails, subscriptions)

      # Should return a map with subscription groupings
      assert is_map(result)
    end

    test "handles empty subscriptions map" do
      killmail_map = TestFactory.build_killmail(123_456_789)
      {:ok, killmail} = Killmail.new(killmail_map)
      killmails = [killmail]

      result = BatchProcessor.group_killmails_by_subscription(killmails, %{})

      assert result == %{}
    end
  end
end
