defmodule WandererKillsWeb.KillmailChannel do
  @moduledoc """
  Phoenix Channel for real-time killmail subscriptions.

  Allows WebSocket clients to:
  - Subscribe to specific EVE Online systems
  - Subscribe to specific character IDs (as victim or attacker)
  - Receive real-time killmail updates
  - Manage subscriptions dynamically

  ## Installation

  ```bash
  npm install phoenix-websocket
  # or
  yarn add phoenix-websocket
  ```

  ## Usage

  Connect to the WebSocket and join the channel:
  ```javascript
  import {Socket} from "phoenix-websocket"

  const socket = new Socket("ws://localhost:4000/socket", {
    params: {client_identifier: "my-app"}
  })
  socket.connect()

  const channel = socket.channel("killmails:lobby", {
    systems: [30000142, 30002187],
    character_ids: [95465499, 90379338],  // Optional character IDs
    preload: {
      enabled: true,
      since_hours: 24,
      limit_per_system: 100
    }
  })

  channel.join()
    .receive("ok", resp => console.log("Joined successfully", resp))
    .receive("error", resp => console.log("Unable to join", resp))

  // Listen for killmail updates
  channel.on("new_kill", payload => {
    console.log("New killmail:", payload)
  })

  // Add/remove system subscriptions
  channel.push("subscribe_systems", {systems: [30000144]})
  channel.push("unsubscribe_systems", {systems: [30000142]})

  // Add/remove character subscriptions
  channel.push("subscribe_characters", {character_ids: [12345678]})
  channel.push("unsubscribe_characters", {character_ids: [95465499]})

  // Get current subscription status
  channel.push("get_status", {})
    .receive("ok", resp => console.log("Current status:", resp))
  ```

  ## Heartbeat and Connection Management

  Phoenix channels include a built-in heartbeat mechanism that automatically:
  - Sends heartbeat messages every 30 seconds (configurable)
  - Detects disconnected clients and cleans up resources
  - Attempts to reconnect automatically on connection loss

  To configure the heartbeat interval and timeout in your client:
  ```javascript
  const socket = new Socket("ws://localhost:4000/socket", {
    params: {client_identifier: "my-app"},
    heartbeatIntervalMs: 30000,  // Send heartbeat every 30 seconds
    reconnectAfterMs: (tries) => {
      // Exponential backoff: [1s, 2s, 5s, 10s]
      return [1000, 2000, 5000, 10000][tries - 1] || 10000
    }
  })
  ```

  The server will automatically disconnect clients that fail to respond to heartbeats,
  ensuring resources are cleaned up properly.
  """

  use WandererKillsWeb, :channel

  require Logger

  alias WandererKills.Core.Observability.Metrics
  alias WandererKills.Core.Support.Error
  alias WandererKills.Core.Support.SupervisedTask
  alias WandererKills.Core.Support.Utils
  alias WandererKills.Ingest.RequestCoalescer
  alias WandererKills.Subs.Filter
  alias WandererKills.Subs.Preloader
  alias WandererKills.Subs.SimpleSubscriptionManager
  alias WandererKillsWeb.Channels.HeartbeatMonitor

  @impl true
  def join("killmails:lobby", %{"systems" => systems} = params, socket) when is_list(systems) do
    character_ids = Map.get(params, "character_ids", [])
    preload_config = Map.get(params, "preload", %{})
    join_with_filters(socket, systems, character_ids, preload_config)
  end

  def join("killmails:lobby", %{"character_ids" => character_ids} = params, socket)
      when is_list(character_ids) do
    systems = Map.get(params, "systems", [])
    preload_config = Map.get(params, "preload", %{})
    join_with_filters(socket, systems, character_ids, preload_config)
  end

  def join("killmails:lobby", params, socket) do
    # Join without initial systems or characters - they can subscribe later
    preload_config = Map.get(params, "preload", %{})
    subscription_id = create_subscription(socket, [], [], preload_config)

    socket =
      socket
      |> assign(:subscription_id, subscription_id)
      |> assign(:subscribed_systems, MapSet.new())
      |> assign(:subscribed_characters, MapSet.new())

    # Track connection
    Logger.info(
      "[WebSocket] Tracking connection - calling Metrics.track_websocket_connection(:connected)"
    )

    Metrics.track_websocket_connection(:connected, %{
      user_id: socket.assigns.user_id,
      subscription_id: subscription_id,
      initial_systems_count: 0
    })

    Logger.debug("[DEBUG] Client connected and joined killmail channel",
      user_id: socket.assigns.user_id,
      subscription_id: subscription_id,
      initial_systems_count: 0,
      initial_characters_count: 0
    )

    response = %{
      subscription_id: subscription_id,
      subscribed_systems: [],
      subscribed_characters: [],
      status: "connected"
    }

    {:ok, response, socket}
  end

  # Handle subscribing to additional systems
  @impl true
  def handle_in("subscribe_systems", %{"systems" => systems}, socket) when is_list(systems) do
    case validate_systems(systems) do
      {:ok, valid_systems} ->
        current_systems = socket.assigns.subscribed_systems
        new_systems = MapSet.difference(MapSet.new(valid_systems), current_systems)

        if MapSet.size(new_systems) > 0 do
          # Subscribe to new PubSub topics
          subscribe_to_systems(MapSet.to_list(new_systems))

          # Update subscription
          all_systems = MapSet.union(current_systems, new_systems)

          updates = %{
            systems: MapSet.to_list(all_systems),
            character_ids: MapSet.to_list(socket.assigns[:subscribed_characters] || MapSet.new())
          }

          update_subscription(socket.assigns.subscription_id, updates)

          socket = assign(socket, :subscribed_systems, all_systems)

          # Check if we need to unsubscribe from all_systems topic
          maybe_unsubscribe_from_all_systems(
            socket,
            MapSet.size(current_systems),
            MapSet.size(new_systems)
          )

          # Track the subscription update
          Metrics.track_websocket_subscription(:updated, MapSet.size(new_systems), %{
            user_id: socket.assigns.user_id,
            subscription_id: socket.assigns.subscription_id,
            operation: :add_systems,
            character_count: 0
          })

          Logger.debug("[DEBUG] Client subscribed to systems",
            user_id: socket.assigns.user_id,
            subscription_id: socket.assigns.subscription_id,
            new_systems_count: MapSet.size(new_systems),
            total_systems_count: MapSet.size(all_systems)
          )

          # Preload recent kills for new systems
          preload_kills_for_systems(socket, MapSet.to_list(new_systems), "subscription")

          {:reply, {:ok, %{subscribed_systems: MapSet.to_list(all_systems)}}, socket}
        else
          {:reply, {:ok, %{message: "Already subscribed to all requested systems"}}, socket}
        end

      {:error, reason} ->
        {:reply, {:error, %{reason: reason}}, socket}
    end
  end

  # Handle unsubscribing from systems
  def handle_in("unsubscribe_systems", %{"systems" => systems}, socket) when is_list(systems) do
    case validate_systems(systems) do
      {:ok, valid_systems} ->
        current_systems = socket.assigns.subscribed_systems
        systems_to_remove = MapSet.intersection(current_systems, MapSet.new(valid_systems))

        if MapSet.size(systems_to_remove) > 0 do
          # Unsubscribe from PubSub topics
          unsubscribe_from_systems(MapSet.to_list(systems_to_remove))

          # Update subscription
          remaining_systems = MapSet.difference(current_systems, systems_to_remove)

          updates = %{
            systems: MapSet.to_list(remaining_systems),
            character_ids: MapSet.to_list(socket.assigns[:subscribed_characters] || MapSet.new())
          }

          update_subscription(socket.assigns.subscription_id, updates)

          socket = assign(socket, :subscribed_systems, remaining_systems)

          # Track the subscription update (negative count for removals)
          Metrics.track_websocket_subscription(:updated, -MapSet.size(systems_to_remove), %{
            user_id: socket.assigns.user_id,
            subscription_id: socket.assigns.subscription_id,
            operation: :remove_systems,
            character_count: 0
          })

          Logger.debug("[DEBUG] Client unsubscribed from systems",
            user_id: socket.assigns.user_id,
            subscription_id: socket.assigns.subscription_id,
            removed_systems_count: MapSet.size(systems_to_remove),
            remaining_systems_count: MapSet.size(remaining_systems)
          )

          {:reply, {:ok, %{subscribed_systems: MapSet.to_list(remaining_systems)}}, socket}
        else
          {:reply, {:ok, %{message: "Not subscribed to any of the requested systems"}}, socket}
        end

      {:error, reason} ->
        {:reply, {:error, %{reason: reason}}, socket}
    end
  end

  # Handle subscribing to characters
  def handle_in("subscribe_characters", %{"character_ids" => character_ids}, socket)
      when is_list(character_ids) do
    case validate_characters(character_ids) do
      {:ok, valid_characters} ->
        process_character_subscription(socket, valid_characters)

      {:error, reason} ->
        {:reply, {:error, %{reason: reason}}, socket}
    end
  end

  # Handle unsubscribing from characters
  def handle_in("unsubscribe_characters", %{"character_ids" => character_ids}, socket)
      when is_list(character_ids) do
    with {:ok, valid_characters} <- validate_characters(character_ids),
         {:ok, socket} <- process_character_unsubscription(socket, valid_characters) do
      remaining_characters = socket.assigns.subscribed_characters
      {:reply, {:ok, %{subscribed_characters: MapSet.to_list(remaining_characters)}}, socket}
    else
      {:error, reason} ->
        {:reply, {:error, %{reason: reason}}, socket}

      {:noop, _} ->
        {:reply, {:ok, %{message: "Not subscribed to any of the requested characters"}}, socket}
    end
  end

  # Handle getting current subscription status
  def handle_in("get_status", _params, socket) do
    response = %{
      subscription_id: socket.assigns.subscription_id,
      subscribed_systems: MapSet.to_list(socket.assigns.subscribed_systems),
      subscribed_characters:
        MapSet.to_list(socket.assigns[:subscribed_characters] || MapSet.new()),
      connected_at: socket.assigns.connected_at,
      user_id: socket.assigns.user_id
    }

    {:reply, {:ok, response}, socket}
  end

  # Handle preload after join completes
  @impl true
  def handle_info({:after_join, systems, _preload_config}, socket) do
    Logger.debug("[DEBUG] Starting preload after join completed",
      user_id: socket.assigns.user_id,
      subscription_id: socket.assigns.subscription_id,
      systems_count: length(systems)
    )

    # For WebSocket connections, always use standard preload
    preload_kills_for_systems(socket, systems, "initial join")

    {:noreply, socket}
  end

  # Handle legacy after_join without preload config
  def handle_info({:after_join, systems}, socket) do
    Logger.debug("[DEBUG] Starting preload after join completed",
      user_id: socket.assigns.user_id,
      subscription_id: socket.assigns.subscription_id,
      systems_count: length(systems)
    )

    preload_kills_for_systems(socket, systems, "initial join")
    {:noreply, socket}
  end

  # Handle Phoenix PubSub messages for killmail updates (from SubscriptionManager)
  def handle_info(
        %{
          type: :detailed_kill_update,
          solar_system_id: system_id,
          kills: killmails,
          timestamp: timestamp
        },
        socket
      ) do
    Logger.debug("[WebSocket] Received killmail update",
      user_id: socket.assigns.user_id,
      subscription_id: socket.assigns.subscription_id,
      system_id: system_id,
      killmail_count: length(killmails),
      subscribed_systems: MapSet.to_list(socket.assigns.subscribed_systems),
      subscribed_characters:
        MapSet.to_list(socket.assigns[:subscribed_characters] || MapSet.new())
    )

    # Build a subscription-like structure for filtering
    subscription = %{
      "system_ids" => MapSet.to_list(socket.assigns.subscribed_systems),
      "character_ids" => MapSet.to_list(socket.assigns[:subscribed_characters] || MapSet.new())
    }

    # Filter killmails based on both system and character subscriptions
    filtered_killmails = Filter.filter_killmails(killmails, subscription)

    if filtered_killmails != [] do
      Logger.debug("Forwarding real-time kills to WebSocket client",
        user_id: socket.assigns.user_id,
        system_id: system_id,
        original_count: length(killmails),
        filtered_count: length(filtered_killmails),
        timestamp: timestamp
      )

      # Use structs directly for lazy JSON encoding (Jason.Encoder is implemented)
      push(socket, "killmail_update", %{
        system_id: system_id,
        killmails: filtered_killmails,
        timestamp: DateTime.to_iso8601(timestamp),
        preload: false
      })

      # Track kills sent to websocket
      Metrics.increment_kills_sent(:realtime, length(filtered_killmails))
    end

    {:noreply, socket}
  end

  def handle_info(
        %{
          type: :kill_count_update,
          solar_system_id: system_id,
          kills: count,
          timestamp: timestamp
        },
        socket
      ) do
    # Only send if we're subscribed to this system
    if MapSet.member?(socket.assigns.subscribed_systems, system_id) do
      Logger.debug("[DEBUG] Forwarding kill count update to WebSocket client",
        user_id: socket.assigns.user_id,
        system_id: system_id,
        count: count,
        timestamp: timestamp
      )

      push(socket, "kill_count_update", %{
        system_id: system_id,
        count: count,
        timestamp: DateTime.to_iso8601(timestamp)
      })
    end

    {:noreply, socket}
  end

  # Handle preload status updates from HistoricalFetcher
  def handle_info(
        %Phoenix.Socket.Broadcast{
          topic: topic,
          event: "preload_status",
          payload: payload
        },
        socket
      ) do
    if String.ends_with?(topic, socket.assigns.subscription_id) do
      push(socket, "preload_status", payload)
    end

    {:noreply, socket}
  end

  def handle_info(
        %Phoenix.Socket.Broadcast{
          topic: topic,
          event: "preload_batch",
          payload: payload
        },
        socket
      ) do
    if String.ends_with?(topic, socket.assigns.subscription_id) do
      push(socket, "preload_batch", payload)

      # Track kills sent
      if payload[:kills] do
        Metrics.increment_kills_sent(:preload, length(payload[:kills]))
      end
    end

    {:noreply, socket}
  end

  def handle_info(
        %Phoenix.Socket.Broadcast{
          topic: topic,
          event: "preload_complete",
          payload: payload
        },
        socket
      ) do
    if String.ends_with?(topic, socket.assigns.subscription_id) do
      push(socket, "preload_complete", payload)
    end

    {:noreply, socket}
  end

  # Handle Phoenix heartbeat messages
  def handle_info(:heartbeat, socket) do
    # Phoenix handles heartbeats internally, just acknowledge
    {:noreply, socket}
  end

  # Handle simple new_kill messages from PubSub
  def handle_info({:new_kill, killmail}, socket) do
    push(socket, "new_kill", killmail)
    {:noreply, socket}
  end

  # Handle killmail_update messages from Broadcaster
  def handle_info(
        %{
          type: :killmail_update,
          system_id: system_id,
          kills: killmails,
          timestamp: timestamp
        },
        socket
      ) do
    Logger.debug("[WebSocket] Received killmail_update from Broadcaster",
      user_id: socket.assigns.user_id,
      subscription_id: socket.assigns.subscription_id,
      system_id: system_id,
      killmail_count: length(killmails)
    )

    # Build a subscription-like structure for filtering
    subscription = %{
      "system_ids" => MapSet.to_list(socket.assigns.subscribed_systems),
      "character_ids" => MapSet.to_list(socket.assigns[:subscribed_characters] || MapSet.new())
    }

    # Filter killmails based on both system and character subscriptions
    filtered_killmails = Filter.filter_killmails(killmails, subscription)

    if filtered_killmails != [] do
      # Use structs directly for lazy JSON encoding (Jason.Encoder is implemented)
      push(socket, "killmail_update", %{
        system_id: system_id,
        killmails: filtered_killmails,
        timestamp: DateTime.to_iso8601(timestamp),
        preload: false
      })

      # Track kills sent to websocket
      Metrics.increment_kills_sent(:realtime, length(filtered_killmails))
    end

    {:noreply, socket}
  end

  # Handle any unmatched PubSub messages
  def handle_info(message, socket) do
    Logger.debug("[DEBUG] Unhandled PubSub message",
      user_id: socket.assigns.user_id,
      message: inspect(message) |> String.slice(0, 200)
    )

    {:noreply, socket}
  end

  # Clean up when client disconnects
  @impl true
  def terminate(reason, socket) do
    # Unregister from heartbeat monitor
    HeartbeatMonitor.unregister_connection(self())

    if subscription_id = socket.assigns[:subscription_id] do
      # Track disconnection
      Metrics.track_websocket_connection(:disconnected, %{
        user_id: socket.assigns.user_id,
        subscription_id: subscription_id,
        reason: reason
      })

      # Track subscription removal
      subscribed_systems_count = MapSet.size(socket.assigns.subscribed_systems || MapSet.new())

      subscribed_characters_count =
        MapSet.size(socket.assigns[:subscribed_characters] || MapSet.new())

      Metrics.track_websocket_subscription(:removed, subscribed_systems_count, %{
        user_id: socket.assigns.user_id,
        subscription_id: subscription_id,
        character_count: subscribed_characters_count
      })

      # Clean up subscription
      remove_subscription(subscription_id)

      duration =
        case socket.assigns[:connected_at] do
          nil ->
            "unknown"

          connected_at ->
            DateTime.diff(DateTime.utc_now(), connected_at, :second)
        end

      Logger.debug("[DEBUG] Client disconnected from killmail channel",
        user_id: socket.assigns.user_id,
        subscription_id: subscription_id,
        subscribed_systems_count: MapSet.size(socket.assigns.subscribed_systems || MapSet.new()),
        disconnect_reason: reason,
        connection_duration_seconds: duration,
        socket_transport: socket.transport
      )
    else
      Logger.debug("[DEBUG] Client disconnected (no active subscription)",
        user_id: socket.assigns[:user_id] || "unknown",
        disconnect_reason: reason,
        socket_transport: socket.transport
      )
    end

    :ok
  end

  # Private helper functions

  # Helper function to handle join with filters
  defp join_with_filters(socket, systems, character_ids, preload_config) do
    with {:ok, valid_systems} <- validate_systems(systems),
         {:ok, valid_characters} <- validate_characters(character_ids) do
      # Register this WebSocket connection as a subscriber
      subscription_id =
        create_subscription(socket, valid_systems, valid_characters, preload_config)

      # Track subscription creation
      Metrics.track_websocket_subscription(:added, length(valid_systems), %{
        user_id: socket.assigns.user_id,
        subscription_id: subscription_id,
        character_count: length(valid_characters)
      })

      # Track connection with initial systems
      Logger.info(
        "[WebSocket] Tracking connection with systems - calling Metrics.track_websocket_connection(:connected)"
      )

      Metrics.track_websocket_connection(:connected, %{
        user_id: socket.assigns.user_id,
        subscription_id: subscription_id,
        initial_systems_count: length(valid_systems),
        initial_characters_count: length(valid_characters)
      })

      socket =
        socket
        |> assign(:subscription_id, subscription_id)
        |> assign(:subscribed_systems, MapSet.new(valid_systems))
        |> assign(:subscribed_characters, MapSet.new(valid_characters))
        |> assign(:preload_config, preload_config)

      # Subscribe to the subscription's own topic for preload updates
      Phoenix.PubSub.subscribe(
        WandererKills.PubSub,
        "killmails:#{subscription_id}"
      )

      Logger.info("[WebSocket] Client connected",
        user_id: socket.assigns.user_id,
        client_identifier: socket.assigns[:client_identifier],
        subscription_id: subscription_id,
        initial_systems_count: length(valid_systems),
        initial_characters_count: length(valid_characters)
      )

      # Register with heartbeat monitor
      HeartbeatMonitor.register_connection(
        self(),
        socket.assigns.user_id,
        subscription_id
      )

      # Subscribe to Phoenix PubSub topics
      cond do
        valid_systems != [] ->
          subscribe_to_systems(valid_systems)

        # If only character subscriptions, subscribe to all_systems topic
        valid_characters != [] ->
          Phoenix.PubSub.subscribe(
            WandererKills.PubSub,
            Utils.all_systems_topic()
          )

        true ->
          :ok
      end

      # Schedule preload after join completes (can't push during join)
      if valid_systems != [] do
        send(self(), {:after_join, valid_systems, preload_config})
      end

      response = %{
        subscription_id: subscription_id,
        subscribed_systems: valid_systems,
        subscribed_characters: valid_characters,
        status: "connected"
      }

      {:ok, response, socket}
    else
      {:error, reason} ->
        Logger.warning("[WARNING] Failed to join killmail channel",
          user_id: socket.assigns.user_id,
          reason: reason,
          peer_data: socket.assigns.peer_data,
          systems: systems,
          character_ids: character_ids
        )

        {:error, %{reason: Error.to_string(reason)}}
    end
  end

  defp validate_systems(systems) do
    max_systems =
      Application.get_env(:wanderer_kills, :validation, [])
      |> Keyword.get(:max_subscribed_systems, 10_000)

    max_system_id =
      Application.get_env(:wanderer_kills, :validation, [])
      |> Keyword.get(:max_system_id, 50_000_000)

    cond do
      # Empty list is valid - allows clients to join without initial system subscriptions
      systems == [] ->
        {:ok, []}

      length(systems) > max_systems ->
        {:error,
         Error.validation_error(:too_many_systems, "Too many systems (max: #{max_systems})", %{
           max: max_systems,
           provided: length(systems)
         })}

      Enum.all?(systems, &is_integer/1) ->
        valid_systems =
          Enum.filter(systems, &(&1 > 0 and &1 <= max_system_id))

        if length(valid_systems) == length(systems) do
          {:ok, Enum.uniq(valid_systems)}
        else
          {:error,
           Error.validation_error(:invalid_system_ids, "Invalid system IDs", %{systems: systems})}
        end

      true ->
        {:error,
         Error.validation_error(:non_integer_system_ids, "System IDs must be integers", %{
           systems: systems
         })}
    end
  end

  defp validate_characters(characters) do
    max_characters =
      Application.get_env(:wanderer_kills, :validation, [])
      |> Keyword.get(:max_subscribed_characters, 1000)

    max_character_id =
      Application.get_env(:wanderer_kills, :validation, [])
      |> Keyword.get(:max_character_id, 3_000_000_000)

    cond do
      # Empty list is valid - allows clients to join without initial character subscriptions
      characters == [] ->
        {:ok, []}

      length(characters) > max_characters ->
        {:error,
         Error.validation_error(
           :too_many_characters,
           "Too many characters (max: #{max_characters})",
           %{
             max: max_characters,
             provided: length(characters)
           }
         )}

      Enum.all?(characters, &is_integer/1) ->
        # Character IDs should be positive integers within valid range
        # EVE Online character IDs typically start from 90000000 for players
        min_character_id = 90_000_000

        valid_characters =
          Enum.filter(characters, &(&1 >= min_character_id and &1 <= max_character_id))

        if length(valid_characters) == length(characters) do
          {:ok, Enum.uniq(valid_characters)}
        else
          {:error,
           Error.validation_error(:invalid_character_ids, "Invalid character IDs", %{
             characters: characters
           })}
        end

      true ->
        {:error,
         Error.validation_error(:non_integer_character_ids, "Character IDs must be integers", %{
           characters: characters
         })}
    end
  end

  defp create_subscription(socket, systems, characters, preload_config) do
    subscription_id = generate_random_id()

    # Register with SimpleSubscriptionManager with character support
    SimpleSubscriptionManager.add_websocket_subscription(%{
      "id" => subscription_id,
      # Required field
      "subscriber_id" => socket.assigns.user_id,
      "user_id" => socket.assigns.user_id,
      "system_ids" => systems,
      "character_ids" => characters,
      "socket_pid" => self(),
      "connected_at" => DateTime.utc_now(),
      "preload_config" => preload_config
    })

    subscription_id
  end

  defp update_subscription(subscription_id, updates) do
    # Convert atom keys to string keys for consistency
    string_updates =
      updates
      |> Enum.map(fn
        {:systems, value} -> {"system_ids", value}
        {:character_ids, value} -> {"character_ids", value}
        {key, value} -> {to_string(key), value}
      end)
      |> Enum.into(%{})

    SimpleSubscriptionManager.update_websocket_subscription(subscription_id, string_updates)
  end

  defp remove_subscription(subscription_id) do
    SimpleSubscriptionManager.remove_websocket_subscription(subscription_id)
  end

  defp subscribe_to_systems(systems) do
    Enum.each(systems, fn system_id ->
      Phoenix.PubSub.subscribe(
        WandererKills.PubSub,
        Utils.system_topic(system_id)
      )

      Phoenix.PubSub.subscribe(
        WandererKills.PubSub,
        Utils.system_detailed_topic(system_id)
      )
    end)
  end

  defp unsubscribe_from_systems(systems) do
    Enum.each(systems, fn system_id ->
      Phoenix.PubSub.unsubscribe(
        WandererKills.PubSub,
        Utils.system_topic(system_id)
      )

      Phoenix.PubSub.unsubscribe(
        WandererKills.PubSub,
        Utils.system_detailed_topic(system_id)
      )
    end)
  end

  defp process_character_unsubscription(socket, valid_characters) do
    current_characters = socket.assigns[:subscribed_characters] || MapSet.new()
    characters_to_remove = MapSet.intersection(current_characters, MapSet.new(valid_characters))

    if MapSet.size(characters_to_remove) == 0 do
      {:noop, socket}
    else
      # Update subscription
      remaining_characters = MapSet.difference(current_characters, characters_to_remove)

      updates = %{
        systems: MapSet.to_list(socket.assigns.subscribed_systems),
        character_ids: MapSet.to_list(remaining_characters)
      }

      update_subscription(socket.assigns.subscription_id, updates)
      socket = assign(socket, :subscribed_characters, remaining_characters)

      Logger.debug("[DEBUG] Client unsubscribed from characters",
        user_id: socket.assigns.user_id,
        subscription_id: socket.assigns.subscription_id,
        removed_characters_count: MapSet.size(characters_to_remove),
        remaining_characters_count: MapSet.size(remaining_characters)
      )

      # Check if we need to unsubscribe from all_systems topic
      maybe_unsubscribe_from_all_systems(
        socket,
        0,
        MapSet.size(socket.assigns.subscribed_systems)
      )

      {:ok, socket}
    end
  end

  defp preload_kills_for_systems(socket, systems, reason) do
    user_id = socket.assigns.user_id
    subscription_id = socket.assigns.subscription_id
    limit_per_system = 5

    # Count current subscriptions for info logging
    current_systems = MapSet.size(socket.assigns.subscribed_systems)
    current_characters = MapSet.size(socket.assigns[:subscribed_characters] || MapSet.new())

    Logger.debug("[WebSocket] Starting preload: #{length(systems)} systems",
      user_id: user_id,
      subscription_id: subscription_id,
      systems_to_preload: length(systems),
      total_subscribed_systems: current_systems,
      total_subscribed_characters: current_characters,
      reason: reason
    )

    # Use SupervisedTask to track WebSocket preload operations
    SupervisedTask.start_child(
      fn ->
        total_kills_sent =
          systems
          |> Enum.map(fn system_id ->
            preload_system_kills_for_websocket(socket, system_id, limit_per_system)
          end)
          |> Enum.sum()

        Logger.debug(
          "[DEBUG] Preload completed: #{total_kills_sent} kills sent across #{length(systems)} systems",
          user_id: user_id,
          subscription_id: subscription_id,
          total_systems: length(systems),
          total_kills_sent: total_kills_sent,
          reason: reason
        )

        # Emit telemetry for kills delivered
        :telemetry.execute(
          [:wanderer_kills, :preload, :kills_delivered],
          %{count: total_kills_sent},
          %{user_id: user_id, subscription_id: subscription_id}
        )
      end,
      task_name: "websocket_preload",
      metadata: %{
        user_id: user_id,
        subscription_id: subscription_id,
        systems_count: length(systems),
        reason: reason
      }
    )
  end

  defp preload_system_kills_for_websocket(socket, system_id, limit) do
    kills = fetch_system_kills_with_coalescing(system_id, limit)
    send_preload_kills_to_websocket(socket, system_id, kills)
  end

  defp fetch_system_kills_with_coalescing(system_id, limit) do
    features = Application.get_env(:wanderer_kills, :features, [])

    if features[:request_coalescing] do
      coalesce_system_kills_request(system_id, limit)
    else
      # Use the shared preloader directly
      Preloader.preload_kills_for_system(system_id, limit, 24)
    end
  end

  defp coalesce_system_kills_request(system_id, limit) do
    request_key = {:websocket_preload, system_id, limit, 24}

    case RequestCoalescer.request(request_key, fn ->
           Preloader.preload_kills_for_system(system_id, limit, 24)
         end) do
      {:ok, kills} ->
        kills

      {:error, reason} ->
        Logger.debug(
          "[KillmailChannel] Request coalescing failed, using direct preload",
          system_id: system_id,
          error: reason
        )

        Preloader.preload_kills_for_system(system_id, limit, 24)

      kills when is_list(kills) ->
        # Handle direct list response (when executor function returns a list)
        kills
    end
  end

  # Removed - now using shared Preloader module

  # Helper function to send preload kills to WebSocket client
  defp send_preload_kills_to_websocket(socket, system_id, kills) when is_list(kills) do
    if kills != [] do
      # Use structs directly for lazy JSON encoding (Jason.Encoder is implemented)
      push(socket, "killmail_update", %{
        system_id: system_id,
        killmails: kills,
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
        preload: true
      })

      # Track kills sent to websocket
      Metrics.increment_kills_sent(:preload, length(kills))

      length(kills)
    else
      0
    end
  end

  # Removed - now using shared Preloader module for these helper functions

  @doc """
  Get websocket statistics - delegated to WebSocketStats GenServer
  """
  @spec get_stats() :: {:ok, map()} | {:error, term()}
  def get_stats do
    Metrics.get_websocket_stats()
  end

  @doc """
  Reset websocket statistics - delegated to WebSocketStats GenServer
  """
  @spec reset_stats() :: :ok
  def reset_stats do
    Metrics.reset_websocket_stats()
  end

  # Generate a unique random ID for subscriptions
  # Uses random bytes encoded in URL-safe Base64
  defp generate_random_id do
    :crypto.strong_rand_bytes(16)
    |> Base.url_encode64(padding: false)
  end

  defp process_character_subscription(socket, valid_characters) do
    current_characters = socket.assigns[:subscribed_characters] || MapSet.new()
    new_characters = MapSet.difference(MapSet.new(valid_characters), current_characters)
    all_characters = MapSet.union(current_characters, MapSet.new(valid_characters))

    {updated_socket, message} =
      if MapSet.size(new_characters) > 0 do
        # Update subscription
        updates = %{
          systems: MapSet.to_list(socket.assigns.subscribed_systems),
          character_ids: MapSet.to_list(all_characters)
        }

        update_subscription(socket.assigns.subscription_id, updates)

        socket = assign(socket, :subscribed_characters, all_characters)

        Logger.debug("[DEBUG] Client subscribed to characters",
          user_id: socket.assigns.user_id,
          subscription_id: socket.assigns.subscription_id,
          new_characters_count: MapSet.size(new_characters),
          total_characters_count: MapSet.size(all_characters)
        )

        {socket, %{subscribed_characters: MapSet.to_list(all_characters)}}
      else
        {socket, %{message: "Already subscribed to all requested characters"}}
      end

    # Check if we need to subscribe to all_systems topic (only if we added new characters)
    if MapSet.size(new_characters) > 0 do
      maybe_subscribe_to_all_systems(updated_socket, all_characters)
    end

    {:reply, {:ok, message}, updated_socket}
  end

  defp maybe_unsubscribe_from_all_systems(socket, current_system_count, new_system_count) do
    current_characters = socket.assigns[:subscribed_characters] || MapSet.new()

    cond do
      # Case 1: Going from 0 system subscriptions to >0 and we have character subscriptions
      current_system_count == 0 and new_system_count > 0 and
          MapSet.size(current_characters) > 0 ->
        Phoenix.PubSub.unsubscribe(
          WandererKills.PubSub,
          Utils.all_systems_topic()
        )

        Logger.debug(
          "[DEBUG] Unsubscribed from all_systems topic due to specific system subscription",
          user_id: socket.assigns.user_id,
          subscription_id: socket.assigns.subscription_id
        )

      # Case 2: No subscriptions remaining at all
      new_system_count == 0 and MapSet.size(current_characters) == 0 ->
        Phoenix.PubSub.unsubscribe(
          WandererKills.PubSub,
          Utils.all_systems_topic()
        )

        Logger.debug(
          "[DEBUG] Unsubscribed from all_systems topic (no subscriptions remaining)",
          user_id: socket.assigns.user_id,
          subscription_id: socket.assigns.subscription_id
        )

      true ->
        :ok
    end
  end

  defp maybe_subscribe_to_all_systems(socket, all_characters) do
    if MapSet.size(socket.assigns.subscribed_systems) == 0 and
         MapSet.size(all_characters) > 0 do
      Phoenix.PubSub.subscribe(
        WandererKills.PubSub,
        Utils.all_systems_topic()
      )
    end
  end
end
