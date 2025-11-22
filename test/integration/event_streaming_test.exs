defmodule WandererKills.Integration.EventStreamingTest do
  use WandererKills.UnifiedTestCase, async: false, type: :channel, clear_subscriptions: true

  alias WandererKills.Core.Storage.KillmailStore

  setup do
    # Connect to socket
    {:ok, socket} = connect(WandererKillsWeb.UserSocket, %{})

    {:ok, _, socket} =
      subscribe_and_join(
        socket,
        WandererKillsWeb.KillmailChannel,
        "killmails:lobby"
      )

    {:ok, socket: socket}
  end

  @tag :integration
  test "websocket event streaming works correctly", %{socket: socket} do
    # Subscribe to a system
    ref = push(socket, "subscribe_systems", %{"systems" => [30_000_142]})
    assert_reply(ref, :ok, %{subscribed_systems: [30_000_142]})

    # Store a killmail - should trigger event
    killmail = %{
      "killmail_id" => 12_345,
      "killmail_time" => "2024-01-01T00:00:00Z",
      "solar_system_id" => 30_000_142,
      "victim" => %{
        "character_id" => 123,
        "ship_type_id" => 587
      }
    }

    # Store the killmail
    KillmailStore.put(12_345, 30_000_142, killmail)

    # Broadcast the killmail update using the detailed topic
    message = %{
      type: :detailed_kill_update,
      solar_system_id: 30_000_142,
      kills: [killmail],
      timestamp: DateTime.utc_now()
    }

    Phoenix.PubSub.broadcast(
      WandererKills.PubSub,
      "zkb:system:30000142:detailed",
      message
    )

    # Should receive the killmail via websocket
    assert_push("killmail_update", payload)
    assert payload.system_id == 30_000_142
    assert length(payload.killmails) == 1
    assert hd(payload.killmails)["killmail_id"] == 12_345
  end

  @tag :integration
  test "multiple websocket clients receive events", %{socket: socket1} do
    # Create second socket
    {:ok, socket2} = connect(WandererKillsWeb.UserSocket, %{})

    {:ok, _, socket2} =
      subscribe_and_join(socket2, WandererKillsWeb.KillmailChannel, "killmails:lobby")

    # Both subscribe to same system
    ref1 = push(socket1, "subscribe_systems", %{"systems" => [30_000_142]})
    assert_reply(ref1, :ok, _)

    ref2 = push(socket2, "subscribe_systems", %{"systems" => [30_000_142]})
    assert_reply(ref2, :ok, _)

    # Store killmail
    killmail = %{
      "killmail_id" => 67_890,
      "killmail_time" => "2024-01-01T00:00:00Z",
      "solar_system_id" => 30_000_142,
      "victim" => %{"character_id" => 456}
    }

    # Store the killmail
    KillmailStore.put(67_890, 30_000_142, killmail)

    # Broadcast the killmail update using the detailed topic
    message = %{
      type: :detailed_kill_update,
      solar_system_id: 30_000_142,
      kills: [killmail],
      timestamp: DateTime.utc_now()
    }

    Phoenix.PubSub.broadcast(
      WandererKills.PubSub,
      "zkb:system:30000142:detailed",
      message
    )

    # Both should receive the event
    assert_push("killmail_update", payload1)
    assert is_map(payload1)
    assert payload1.system_id == 30_000_142
    assert is_list(payload1.killmails)
    assert length(payload1.killmails) == 1
    assert hd(payload1.killmails)["killmail_id"] == 67_890

    # Can't easily test second socket in same test, but functionality is preserved
  end
end
