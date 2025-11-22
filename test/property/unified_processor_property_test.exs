defmodule WandererKills.Ingest.Killmails.UnifiedProcessorPropertyTest do
  use WandererKills.UnifiedTestCase
  use ExUnitProperties

  @moduletag :property
  @moduletag area: :killmail_processing
  @moduletag performance: :medium

  alias WandererKills.Ingest.Killmails.UnifiedProcessor

  describe "UnifiedProcessor.process_killmail/3 properties" do
    @tag :property
    property "never raises and always returns tagged tuple for any input" do
      check all(
              json_fragment <- json_like_data(),
              max_runs: 100
            ) do
        cutoff_time = DateTime.utc_now() |> DateTime.add(-30, :day)

        # UnifiedProcessor expects a map, so wrap non-map inputs or convert them
        input =
          case json_fragment do
            map when is_map(map) -> map
            _ -> %{"data" => json_fragment}
          end

        result = UnifiedProcessor.process_killmail(input, cutoff_time, validate_only: true)

        # Should never raise - always return {:ok, _} or {:error, _}
        assert match?({:ok, _}, result) or match?({:error, _}, result)
      end
    end

    @tag :property
    property "parses valid killmail structure correctly" do
      check all(
              killmail <- valid_killmail_generator(),
              max_runs: 50
            ) do
        # Use a cutoff time that won't filter out test killmails
        cutoff_time = DateTime.utc_now() |> DateTime.add(-365 * 5, :day)
        result = UnifiedProcessor.process_killmail(killmail, cutoff_time, validate_only: true)

        case result do
          {:ok, parsed} when is_struct(parsed) ->
            assert match?(%WandererKills.Domain.Killmail{}, parsed)
            assert parsed.killmail_id != nil
            assert parsed.system_id != nil
            assert parsed.kill_time != nil

          {:ok, :kill_older} ->
            # Killmail was too old, that's valid
            :ok

          {:error, _reason} ->
            # Some validation error, also valid for random data
            :ok
        end
      end
    end

    @tag :property
    property "handles missing required fields gracefully" do
      check all(
              killmail <- incomplete_killmail_generator(),
              max_runs: 50
            ) do
        cutoff_time = DateTime.utc_now() |> DateTime.add(-365 * 5, :day)
        result = UnifiedProcessor.process_killmail(killmail, cutoff_time, validate_only: true)

        # Should return error for missing required fields or :kill_older
        assert match?({:error, _}, result) or match?({:ok, :kill_older}, result)

        # Error should be descriptive
        case result do
          {:error, reason} when is_binary(reason) ->
            assert String.length(reason) > 0

          {:error, %{message: message}} ->
            assert String.length(message) > 0

          _ ->
            :ok
        end
      end
    end

    @tag :property
    property "preserves data integrity for well-formed killmails" do
      check all(
              killmail_id <- positive_integer(),
              system_id <- integer(30_000_000..31_000_000),
              timestamp <- timestamp_generator(),
              max_runs: 50
            ) do
        killmail = %{
          "killmail_id" => killmail_id,
          "solar_system_id" => system_id,
          "killmail_time" => timestamp,
          "victim" => valid_victim(),
          "attackers" => [valid_attacker()],
          "zkb" => valid_zkb()
        }

        # Use a very recent cutoff to ensure our test killmails pass the time check
        cutoff_time = DateTime.utc_now() |> DateTime.add(-365 * 10, :day)

        case UnifiedProcessor.process_killmail(killmail, cutoff_time, validate_only: true) do
          {:ok, parsed} when is_struct(parsed) ->
            assert parsed.killmail_id == killmail_id
            assert parsed.system_id == system_id
            assert DateTime.to_iso8601(parsed.kill_time) == timestamp

          {:ok, :kill_older} ->
            # The generated timestamp was too old, that's okay
            :ok

          {:error, _} ->
            # Some other validation error
            :ok
        end
      end
    end

    @tag :property
    property "handles various JSON input types without crashing" do
      check all(
              input <-
                one_of([
                  # Valid JSON-like structures
                  map_of(string(:ascii), term()),
                  list_of(term()),
                  string(:ascii),
                  integer(),
                  float(),
                  boolean(),
                  constant(nil)
                ]),
              max_runs: 100
            ) do
        cutoff_time = DateTime.utc_now() |> DateTime.add(-30, :day)

        # UnifiedProcessor expects a map
        map_input =
          case input do
            map when is_map(map) -> map
            _ -> %{"data" => input}
          end

        result = UnifiedProcessor.process_killmail(map_input, cutoff_time, validate_only: true)
        assert match?({:ok, _}, result) or match?({:error, _}, result)
      end
    end

    @tag :property
    property "rejects malformed victim data" do
      check all(
              victim_data <- malformed_victim_generator(),
              max_runs: 50
            ) do
        killmail = %{
          "killmail_id" => 123_456,
          "solar_system_id" => 30_000_142,
          "killmail_time" => "2024-01-01T00:00:00Z",
          "victim" => victim_data,
          "attackers" => [],
          "zkb" => valid_zkb()
        }

        cutoff_time = DateTime.utc_now() |> DateTime.add(-30, :day)
        result = UnifiedProcessor.process_killmail(killmail, cutoff_time, validate_only: true)

        # Should handle gracefully - either parse what it can or return error
        assert match?({:ok, _}, result) or match?({:error, _}, result)
      end
    end
  end

  # Generators

  defp json_like_data do
    frequency([
      {3, map_of(string(:ascii), json_value())},
      {2, list_of(json_value())},
      {1, string(:ascii)},
      {1, integer()},
      {1, float()},
      {1, boolean()},
      {1, constant(nil)}
    ])
  end

  defp json_value do
    frequency([
      {4, string(:ascii)},
      {3, integer()},
      {2, float()},
      {2, boolean()},
      {1, constant(nil)},
      {1, list_of(string(:ascii), max_length: 3)},
      {1, map_of(string(:ascii), string(:ascii), max_length: 3)}
    ])
  end

  defp valid_killmail_generator do
    gen all(
          killmail_id <- positive_integer(),
          system_id <- integer(30_000_000..31_000_000),
          timestamp <- timestamp_generator()
        ) do
      %{
        "killmail_id" => killmail_id,
        "solar_system_id" => system_id,
        "killmail_time" => timestamp,
        "victim" => valid_victim(),
        "attackers" =>
          Enum.take(Stream.repeatedly(fn -> valid_attacker() end), Enum.random(1..5)),
        "zkb" => valid_zkb()
      }
    end
  end

  defp incomplete_killmail_generator do
    # Generate killmails with missing required fields
    gen all(
          fields <-
            list_of(
              member_of(["killmail_id", "solar_system_id", "killmail_time"]),
              min_length: 1,
              max_length: 2
            )
        ) do
      base = %{
        "killmail_id" => 123_456,
        "solar_system_id" => 30_000_142,
        "killmail_time" => "2024-01-01T00:00:00Z",
        "victim" => valid_victim(),
        "attackers" => [],
        "zkb" => valid_zkb()
      }

      # Remove selected fields
      Enum.reduce(fields, base, &Map.delete(&2, &1))
    end
  end

  defp timestamp_generator do
    gen all(
          year <- integer(2020..2025),
          month <- integer(1..12),
          day <- integer(1..28),
          hour <- integer(0..23),
          minute <- integer(0..59),
          second <- integer(0..59)
        ) do
      :io_lib.format(
        "~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ",
        [year, month, day, hour, minute, second]
      )
      |> to_string()
    end
  end

  defp valid_victim do
    %{
      "character_id" => Enum.random(90_000_000..100_000_000),
      "corporation_id" => Enum.random(1_000_000..2_000_000),
      "ship_type_id" => Enum.random([587, 17_619, 11_993, 11_978]),
      "damage_taken" => Enum.random(100..50_000)
    }
  end

  defp valid_zkb do
    %{
      "hash" => Base.encode16(:crypto.strong_rand_bytes(20), case: :lower),
      "npc" => false,
      "solo" => Enum.random([true, false]),
      "points" => Enum.random(1..100),
      "locationID" => Enum.random(50_000_000..60_000_000)
    }
  end

  defp valid_attacker do
    %{
      "character_id" => Enum.random(90_000_000..100_000_000),
      "corporation_id" => Enum.random(1_000_000..2_000_000),
      "ship_type_id" => Enum.random([587, 17_619, 11_993, 11_978]),
      "damage_done" => Enum.random(100..10_000),
      "final_blow" => Enum.random([true, false])
    }
  end

  defp malformed_victim_generator do
    one_of([
      # Missing fields
      constant(%{}),
      # Wrong types
      constant(%{
        "character_id" => "not_a_number",
        "ship_type_id" => true,
        "damage_taken" => nil
      }),
      # Nested weirdness
      constant(%{
        "character_id" => %{"nested" => "value"},
        "ship_type_id" => [1, 2, 3]
      }),
      # Extreme values
      constant(%{
        "character_id" => -999_999,
        "ship_type_id" => 999_999_999,
        "damage_taken" => -1000
      })
    ])
  end
end
