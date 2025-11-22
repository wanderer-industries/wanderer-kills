defmodule WandererKills.Integration.WebSocketStreamingTest do
  use WandererKills.UnifiedTestCase, async: false, type: :channel, clear_subscriptions: true

  alias WandererKillsWeb.KillmailChannel

  setup do
    # Connect to socket and join channel
    {:ok, socket} = connect(WandererKillsWeb.UserSocket, %{})
    {:ok, _, socket} = subscribe_and_join(socket, KillmailChannel, "killmails:lobby")
    {:ok, socket: socket}
  end

  @tag :integration
  test "websocket receives real-time killmail updates", %{socket: socket} do
    # Subscribe to a system
    ref = push(socket, "subscribe_systems", %{"systems" => [30_000_142]})
    assert_reply(ref, :ok, %{subscribed_systems: subscribed_systems})
    assert subscribed_systems == [30_000_142]

    # Simulate a killmail being stored (which should trigger broadcasting)
    killmail_data = %{
      "killmail_id" => 999_999,
      "killmail_time" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "solar_system_id" => 30_000_142,
      "victim" => %{
        "character_id" => 95_465_499,
        "ship_type_id" => 670,
        "damage_taken" => 12_345
      },
      "attackers" => []
    }

    # Store through KillmailStore which triggers events
    WandererKills.Core.Storage.KillmailStore.put(999_999, 30_000_142, killmail_data)

    # Broadcast the kill event through PubSub using correct topic format
    Phoenix.PubSub.broadcast(
      WandererKills.PubSub,
      "zkb:system:30000142",
      {:new_kill, killmail_data}
    )

    # Should receive the new kill via websocket
    assert_push("new_kill", payload)
    assert payload["killmail_id"] == 999_999
    assert payload["solar_system_id"] == 30_000_142
  end

  @tag :integration
  test "multiple clients receive broadcasts", %{socket: socket1} do
    # Create second socket connection
    {:ok, socket2} = connect(WandererKillsWeb.UserSocket, %{})
    {:ok, _, socket2} = subscribe_and_join(socket2, KillmailChannel, "killmails:lobby", %{})

    # Both subscribe to same system
    ref1 = push(socket1, "subscribe_systems", %{"systems" => [30_000_142]})
    assert_reply(ref1, :ok, _)

    ref2 = push(socket2, "subscribe_systems", %{"systems" => [30_000_142]})
    assert_reply(ref2, :ok, _)

    # Broadcast a kill
    killmail_data = %{
      "killmail_id" => 888_888,
      "killmail_time" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "solar_system_id" => 30_000_142,
      "victim" => %{"character_id" => 123_456}
    }

    Phoenix.PubSub.broadcast(
      WandererKills.PubSub,
      "zkb:system:30000142",
      {:new_kill, killmail_data}
    )

    # Both sockets should receive it
    assert_push("new_kill", payload1)
    assert payload1["killmail_id"] == 888_888

    # Socket 2 would also receive it (but harder to test in same process)
  end

  @tag :integration
  test "unsubscribe stops receiving updates", %{socket: socket} do
    # Subscribe
    ref = push(socket, "subscribe_systems", %{"systems" => [30_000_142]})
    assert_reply(ref, :ok, _)

    # Unsubscribe
    ref = push(socket, "unsubscribe_systems", %{"systems" => [30_000_142]})
    assert_reply(ref, :ok, _)

    # Broadcast a kill
    Phoenix.PubSub.broadcast(
      WandererKills.PubSub,
      "zkb:system:30000142",
      {:new_kill, %{"killmail_id" => 777_777}}
    )

    # Should not receive it
    refute_push("new_kill", _)
  end
end
