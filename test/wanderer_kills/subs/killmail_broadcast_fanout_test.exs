defmodule WandererKills.Subs.KillmailBroadcastFanoutTest do
  @moduledoc """
  Regression test for the duplicate killmail fan-out fixed in #13.

  `R2Z2.broadcast_killmail_update_enriched/1` makes two calls for every
  killmail it ingests: an async cast into `SimpleSubscriptionManager` for
  per-subscription delivery, and a direct `Broadcaster.broadcast_killmail_update/2`
  for the global PubSub topics. The manager used to *also* call the broadcaster,
  so every SSE and WebSocket subscriber received each killmail twice.

  This test reproduces that exact pair of calls and asserts that each global
  topic sees the killmail once, while the matching subscription still gets its
  own delivery.
  """

  use ExUnit.Case, async: false

  alias WandererKills.Core.Support.Utils
  alias WandererKills.Domain.Killmail
  alias WandererKills.Subs.SimpleSubscriptionManager, as: SubscriptionManager
  alias WandererKills.Subs.{Broadcaster, CharacterIndex, SystemIndex}

  @system_id 30_000_142
  @victim_character_id 95_465_499
  @killmail_id 123_456_789

  setup do
    # The manager is an application-level singleton that owns the index ETS
    # tables; start nothing here, just wait for the application supervisor.
    WandererKills.TestHelpers.ensure_task_supervisor()
    {:ok, _pid} = WandererKills.TestHelpers.wait_for_process(SubscriptionManager)

    SubscriptionManager.clear_all_subscriptions()
    CharacterIndex.clear()
    SystemIndex.clear()

    :ok
  end

  test "a killmail reaches every global topic exactly once, and its subscription once" do
    {:ok, subscription_id} =
      SubscriptionManager.add_subscription(
        %{
          "subscriber_id" => "fanout_test_#{System.unique_integer([:positive])}",
          "system_ids" => [@system_id],
          "character_ids" => []
        },
        :websocket
      )

    collectors = %{
      system: start_collector(Utils.system_topic(@system_id)),
      detailed: start_collector(Utils.system_detailed_topic(@system_id)),
      all_systems: start_collector(Utils.all_systems_topic()),
      character: start_collector("zkb:character:#{@victim_character_id}"),
      subscription: start_collector("subscription:#{subscription_id}")
    }

    killmail_map = Killmail.to_map(build_killmail())

    # Exactly what R2Z2.broadcast_killmail_update_enriched/1 does, in order.
    SubscriptionManager.broadcast_killmail_update_async(@system_id, [killmail_map])
    Broadcaster.broadcast_killmail_update(@system_id, [killmail_map])

    # A call flushes the mailbox, so the cast above has been handled (and any
    # broadcast it makes has been sent) by the time this returns.
    SubscriptionManager.get_stats()

    # Ordering between the manager's sends and the drain request below is not
    # guaranteed across three processes, so give the broadcasts a moment to
    # land. Without this the test could pass by missing the duplicate rather
    # than by there not being one.
    Process.sleep(50)

    messages = Map.new(collectors, fn {name, pid} -> {name, collect(pid)} end)

    # zkb:system:<id> carries both a WebSocket update and an SSE event.
    assert count_websocket_updates(messages.system) == 1
    assert count_sse_events(messages.system) == 1

    # The detailed topic is WebSocket-only.
    assert count_websocket_updates(messages.detailed) == 1
    assert count_sse_events(messages.detailed) == 0

    assert count_websocket_updates(messages.all_systems) == 1
    assert count_sse_events(messages.all_systems) == 1

    assert count_character_kills(messages.character) == 1

    # The whole point of the cast: the matching subscription still gets its
    # own delivery, and only one.
    assert count_websocket_updates(messages.subscription) == 1
  end

  defp build_killmail do
    {:ok, killmail} =
      Killmail.new(%{
        "killmail_id" => @killmail_id,
        "kill_time" => "2024-01-01T12:00:00Z",
        "system_id" => @system_id,
        "victim" => %{
          "character_id" => @victim_character_id,
          "ship_type_id" => 587,
          "damage_taken" => 100
        },
        "attackers" => []
      })

    killmail
  end

  # A WebSocket-shaped update: Broadcaster.broadcast_killmail_update/2 sends
  # these to the system, detailed and all-systems topics, and the manager sends
  # the same shape to the per-subscription topic.
  defp count_websocket_updates(messages) do
    Enum.count(messages, fn
      %{type: :killmail_update, system_id: @system_id, kills: kills} ->
        Enum.any?(kills, &(&1["killmail_id"] == @killmail_id))

      _ ->
        false
    end)
  end

  # SSE events are {WandererKills.PubSub, json} tuples, one per killmail.
  defp count_sse_events(messages) do
    pubsub = Broadcaster.pubsub_name()

    Enum.count(messages, fn
      {^pubsub, json} when is_binary(json) -> String.contains?(json, "#{@killmail_id}")
      _ -> false
    end)
  end

  defp count_character_kills(messages) do
    Enum.count(messages, fn
      {:killmail, kill} -> kill["killmail_id"] == @killmail_id
      _ -> false
    end)
  end

  # Each topic gets its own process so the counts cannot be confused: the same
  # WebSocket message is published to several topics, and a single subscriber
  # process could not tell which topic delivered it.
  defp start_collector(topic) do
    parent = self()

    pid =
      spawn_link(fn ->
        Phoenix.PubSub.subscribe(Broadcaster.pubsub_name(), topic)
        send(parent, {:collector_ready, self()})
        collect_loop([])
      end)

    receive do
      {:collector_ready, ^pid} -> pid
    after
      1_000 -> flunk("collector for #{topic} never subscribed")
    end
  end

  defp collect_loop(acc) do
    receive do
      {:collector_drain, from, ref} -> send(from, {ref, Enum.reverse(acc)})
      message -> collect_loop([message | acc])
    end
  end

  defp collect(pid) do
    ref = make_ref()
    send(pid, {:collector_drain, self(), ref})

    receive do
      {^ref, messages} -> messages
    after
      1_000 -> flunk("collector never answered")
    end
  end
end
