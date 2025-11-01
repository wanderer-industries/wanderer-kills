defmodule WandererKills.Ingest.Killmails.StoreEventStreamingTest do
  use WandererKills.UnifiedTestCase, async: false

  alias WandererKills.Core.Storage.KillmailStore

  @moduletag area: :killmail_storage
  @moduletag area: :event_streaming

  @system_id_1 30_000_142
  @system_id_2 30_000_143
  @client_id "test_client_123"

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

  @test_killmail_3 %{
    "killmail_id" => 12_347,
    "solar_system_id" => @system_id_2,
    "victim" => %{"character_id" => 125},
    "attackers" => [],
    "zkb" => %{"totalValue" => 3000}
  }

  # Test setup is handled by UnifiedTestCase

  setup do
    # Ensure event streaming is enabled before table initialization
    original_config = Application.get_env(:wanderer_kills, :storage)
    Application.put_env(:wanderer_kills, :storage, enable_event_streaming: true)

    # The application should have already started and initialized tables.
    # We just need to ensure event streaming tables are available.
    # If they're not, it means the application started without event streaming enabled,
    # so we need to create them manually.
    KillmailStore.init_tables!()

    on_exit(fn ->
      if original_config do
        Application.put_env(:wanderer_kills, :storage, original_config)
      else
        Application.delete_env(:wanderer_kills, :storage)
      end
    end)

    :ok
  end

  describe "event streaming - insert_event/2" do
    test "creates event when inserting killmail" do
      # Subscribe to PubSub to verify broadcast
      Phoenix.PubSub.subscribe(WandererKills.PubSub, "system:#{@system_id_1}")

      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_1)

      # Verify PubSub broadcast
      assert_receive {:new_killmail, @system_id_1, killmail}
      assert killmail["killmail_id"] == @test_killmail_1["killmail_id"]

      # Verify killmail is stored
      assert {:ok, stored} = KillmailStore.get(@test_killmail_1["killmail_id"])
      assert stored == @test_killmail_1

      # Verify it's in system list
      system_kills = KillmailStore.list_by_system(@system_id_1)
      assert length(system_kills) == 1
      assert hd(system_kills)["killmail_id"] == @test_killmail_1["killmail_id"]
    end

    test "events have sequential IDs" do
      # Insert multiple events
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_1)
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_2)
      assert :ok = KillmailStore.insert_event(@system_id_2, @test_killmail_3)

      # Fetch events and verify sequential IDs
      {:ok, events} = KillmailStore.fetch_for_client(@client_id, [@system_id_1, @system_id_2])

      # Events should be in order with sequential IDs
      assert length(events) == 3
      event_ids = Enum.map(events, fn {id, _, _} -> id end)
      assert event_ids == Enum.sort(event_ids)

      # Verify consecutive nature
      [id1, id2, id3] = event_ids
      assert id2 == id1 + 1
      assert id3 == id2 + 1
    end
  end

  describe "event streaming - fetch_for_client/2" do
    test "fetches all new events for a client" do
      # Insert some events
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_1)
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_2)
      assert :ok = KillmailStore.insert_event(@system_id_2, @test_killmail_3)

      # Fetch events for both systems
      {:ok, events} = KillmailStore.fetch_for_client(@client_id, [@system_id_1, @system_id_2])
      assert length(events) == 3

      # Verify event structure
      Enum.each(events, fn {event_id, system_id, killmail} ->
        assert is_integer(event_id)
        assert system_id in [@system_id_1, @system_id_2]
        assert is_map(killmail)
        assert killmail["killmail_id"] in [12_345, 12_346, 12_347]
      end)
    end

    test "respects client offsets" do
      # Insert some events
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_1)
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_2)

      # Fetch first batch
      {:ok, events} = KillmailStore.fetch_for_client(@client_id, [@system_id_1])
      assert length(events) == 2

      # Get the last event ID
      {last_event_id, _, _} = List.last(events)

      # Update client offset
      offsets = %{@system_id_1 => last_event_id}
      assert :ok = KillmailStore.put_client_offsets(@client_id, offsets)

      # Fetch again - should get no events
      {:ok, new_events} = KillmailStore.fetch_for_client(@client_id, [@system_id_1])
      assert new_events == []

      # Add a new event
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_3)

      # Now should get only the new event
      {:ok, newer_events} = KillmailStore.fetch_for_client(@client_id, [@system_id_1])
      assert length(newer_events) == 1
      {_, _, killmail} = hd(newer_events)
      assert killmail["killmail_id"] == @test_killmail_3["killmail_id"]
    end

    test "handles multiple systems with different offsets" do
      # Insert events for different systems
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_1)
      assert :ok = KillmailStore.insert_event(@system_id_2, @test_killmail_3)

      # Fetch and process system 1
      {:ok, sys1_events} = KillmailStore.fetch_for_client(@client_id, [@system_id_1])
      {sys1_last_id, _, _} = hd(sys1_events)

      # Update offset only for system 1
      assert :ok = KillmailStore.put_client_offsets(@client_id, %{@system_id_1 => sys1_last_id})

      # Add more events
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_2)
      new_killmail_sys2 = Map.put(@test_killmail_2, "killmail_id", 12_348)
      assert :ok = KillmailStore.insert_event(@system_id_2, new_killmail_sys2)

      # Fetch for both systems
      {:ok, events} = KillmailStore.fetch_for_client(@client_id, [@system_id_1, @system_id_2])

      # Should get 1 new event from system 1 and 2 events from system 2
      sys1_new = Enum.filter(events, fn {_, sys_id, _} -> sys_id == @system_id_1 end)
      sys2_all = Enum.filter(events, fn {_, sys_id, _} -> sys_id == @system_id_2 end)

      assert length(sys1_new) == 1
      assert length(sys2_all) == 2
    end
  end

  describe "event streaming - fetch_one_event/2" do
    test "fetches single event at a time" do
      # Insert multiple events
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_1)
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_2)

      # Fetch one event
      case KillmailStore.fetch_one_event(@client_id, [@system_id_1]) do
        {:ok, {event_id, system_id, killmail}} ->
          assert is_integer(event_id)
          assert system_id == @system_id_1
          assert killmail["killmail_id"] == @test_killmail_1["killmail_id"]

        other ->
          flunk("Expected {:ok, event_tuple}, got: #{inspect(other)}")
      end

      # Fetch next event
      case KillmailStore.fetch_one_event(@client_id, [@system_id_1]) do
        {:ok, {_, _, killmail}} ->
          assert killmail["killmail_id"] == @test_killmail_2["killmail_id"]

        other ->
          flunk("Expected {:ok, event_tuple}, got: #{inspect(other)}")
      end

      # No more events
      assert KillmailStore.fetch_one_event(@client_id, [@system_id_1]) == :empty
    end

    test "works with single system_id" do
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_1)

      # Can pass single system_id instead of list
      case KillmailStore.fetch_one_event(@client_id, @system_id_1) do
        {:ok, {_, system_id, killmail}} ->
          assert system_id == @system_id_1
          assert killmail["killmail_id"] == @test_killmail_1["killmail_id"]

        other ->
          flunk("Expected {:ok, event_tuple}, got: #{inspect(other)}")
      end
    end
  end

  describe "event streaming - client offsets" do
    test "get_client_offsets returns empty map for new client" do
      offsets = KillmailStore.get_client_offsets("new_client")
      assert offsets == %{}
    end

    test "put and get client offsets" do
      offsets = %{@system_id_1 => 10, @system_id_2 => 20}
      assert :ok = KillmailStore.put_client_offsets(@client_id, offsets)

      retrieved = KillmailStore.get_client_offsets(@client_id)
      assert retrieved == offsets
    end

    test "updates existing offsets" do
      # Set initial offsets
      initial = %{@system_id_1 => 10}
      assert :ok = KillmailStore.put_client_offsets(@client_id, initial)

      # Update with new offsets
      updated = %{@system_id_1 => 20, @system_id_2 => 5}
      assert :ok = KillmailStore.put_client_offsets(@client_id, updated)

      retrieved = KillmailStore.get_client_offsets(@client_id)
      assert retrieved == updated
    end
  end

  describe "event streaming - clear operation" do
    test "clear removes all event data" do
      # Insert events and set offsets
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_1)
      assert :ok = KillmailStore.put_client_offsets(@client_id, %{@system_id_1 => 1})

      # Clear all data
      assert :ok = KillmailStore.clear()

      # Verify everything is cleared
      assert {:error, _} = KillmailStore.get(@test_killmail_1["killmail_id"])
      assert KillmailStore.list_by_system(@system_id_1) == []
      assert KillmailStore.get_client_offsets(@client_id) == %{}

      # New events should start from beginning
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_2)
      {:ok, events} = KillmailStore.fetch_for_client("new_client", [@system_id_1])
      assert length(events) == 1
    end
  end

  describe "event streaming disabled" do
    setup do
      # Disable event streaming for these tests
      Application.put_env(:wanderer_kills, :storage, enable_event_streaming: false)

      on_exit(fn ->
        Application.put_env(:wanderer_kills, :storage, enable_event_streaming: true)
      end)

      :ok
    end

    test "insert_event falls back to regular put when disabled" do
      assert :ok = KillmailStore.insert_event(@system_id_1, @test_killmail_1)

      # Should still store the killmail
      assert {:ok, stored} = KillmailStore.get(@test_killmail_1["killmail_id"])
      assert stored == @test_killmail_1

      # But no events should be available
      {:ok, events} = KillmailStore.fetch_for_client(@client_id, [@system_id_1])
      assert events == []
    end

    test "fetch operations return empty when disabled" do
      # Even if we somehow had events, they shouldn't be accessible
      assert {:ok, []} = KillmailStore.fetch_for_client(@client_id, [@system_id_1])
      assert :empty = KillmailStore.fetch_one_event(@client_id, [@system_id_1])
      assert %{} = KillmailStore.get_client_offsets(@client_id)
    end
  end

  describe "concurrent event operations" do
    setup do
      # Clear any existing data to ensure clean state
      KillmailStore.clear()
      :ok
    end

    test "handles concurrent inserts safely" do
      # Spawn multiple processes to insert events concurrently
      tasks =
        Enum.map(1..10, fn i ->
          Task.async(fn ->
            killmail = %{
              "killmail_id" => 20_000 + i,
              "solar_system_id" => @system_id_1,
              "victim" => %{"character_id" => 1000 + i}
            }

            KillmailStore.insert_event(@system_id_1, killmail)
          end)
        end)

      # Wait for all to complete
      results = Task.await_many(tasks)
      assert Enum.all?(results, &(&1 == :ok))

      # Verify all events were created
      {:ok, events} = KillmailStore.fetch_for_client(@client_id, [@system_id_1])
      assert length(events) == 10

      # Verify sequential event IDs
      event_ids = Enum.map(events, fn {id, _, _} -> id end)
      sorted_ids = Enum.sort(event_ids)
      assert event_ids == sorted_ids
    end

    test "handles concurrent client fetches" do
      # Insert some events
      Enum.each(1..5, fn i ->
        killmail = Map.put(@test_killmail_1, "killmail_id", 30_000 + i)
        KillmailStore.insert_event(@system_id_1, killmail)
      end)

      # Multiple clients fetching concurrently
      client_ids = Enum.map(1..5, fn i -> "client_#{i}" end)

      tasks =
        Enum.map(client_ids, fn client_id ->
          Task.async(fn ->
            KillmailStore.fetch_for_client(client_id, [@system_id_1])
          end)
        end)

      results = Task.await_many(tasks)

      # All clients should get the same events
      Enum.each(results, fn {:ok, events} ->
        assert length(events) == 5
      end)
    end
  end
end
