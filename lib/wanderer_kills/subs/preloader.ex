defmodule WandererKills.Subs.Preloader do
  @moduledoc """
  Handles preloading of killmail data for new subscriptions.

  This module manages the asynchronous preloading of recent killmails
  when a new subscription is created, ensuring subscribers receive
  historical data.
  """

  require Logger

  alias WandererKills.Core.Support.SupervisedTask
  alias WandererKills.Subs.{Broadcaster, WebhookNotifier}

  # Preload configuration
  @default_limit_per_system 5
  @default_since_hours 24

  @doc """
  Preloads recent kills for a new subscriber's systems.

  This function spawns an asynchronous task to:
  1. Fetch recent kills for each subscribed system
  2. Broadcast updates via PubSub
  3. Send webhook notifications if configured

  ## Parameters
  - `subscription` - The subscription map containing system_ids and callback_url
  - `opts` - Options for preloading:
    - `:limit_per_system` - Max kills per system (default: 5)
    - `:since_hours` - Hours to look back (default: 24)

  ## Returns
  - `:ok` immediately (processing happens asynchronously)
  """
  @spec preload_for_subscription(map(), keyword()) :: :ok
  def preload_for_subscription(subscription, opts \\ []) do
    limit_per_system = Keyword.get(opts, :limit_per_system, @default_limit_per_system)
    since_hours = Keyword.get(opts, :since_hours, @default_since_hours)

    # Start supervised async task
    SupervisedTask.start_child(
      fn -> do_preload(subscription, limit_per_system, since_hours) end,
      task_name: "subscription_preload",
      metadata: %{
        subscription_id: subscription["id"],
        system_count: length(subscription["system_ids"])
      }
    )

    :ok
  end

  # Private Functions

  defp do_preload(subscription, limit_per_system, since_hours) do
    %{
      "id" => subscription_id,
      "subscriber_id" => subscriber_id,
      "system_ids" => system_ids,
      "callback_url" => callback_url
    } = subscription

    Logger.info("[INFO] Starting kill preload for new subscription",
      subscription_id: subscription_id,
      subscriber_id: subscriber_id,
      system_count: length(system_ids),
      limit_per_system: limit_per_system,
      since_hours: since_hours
    )

    # Process each system
    results =
      system_ids
      |> Enum.map(&preload_system(&1, limit_per_system, since_hours))
      |> Enum.filter(fn {_system_id, kills} -> kills != [] end)

    # Broadcast and notify for each system with kills
    Enum.each(results, fn {system_id, kills} ->
      # Always broadcast to PubSub
      Broadcaster.broadcast_killmail_update(system_id, kills)

      # Send webhook if configured
      if callback_url do
        WebhookNotifier.notify_webhook(callback_url, system_id, kills, subscription_id)
      end
    end)

    total_kills = results |> Enum.map(fn {_, kills} -> length(kills) end) |> Enum.sum()

    Logger.info("[INFO] Completed kill preload",
      subscription_id: subscription_id,
      systems_with_kills: length(results),
      total_kills: total_kills
    )
  rescue
    error in [ArgumentError, KeyError] ->
      Logger.error("[ERROR] Failed to preload kills for subscription - invalid data",
        subscription_id: subscription["id"],
        error_type: error.__struct__,
        error: Exception.message(error)
      )

    error ->
      stacktrace = __STACKTRACE__

      Logger.error("[ERROR] Failed to preload kills for subscription - unexpected error",
        subscription_id: subscription["id"],
        error_type: error.__struct__,
        error: Exception.format(:error, error, stacktrace)
      )
  end

  defp preload_system(system_id, limit, since_hours) do
    kills = __MODULE__.preload_kills_for_system(system_id, limit, since_hours)

    if kills != [] do
      Logger.debug("[DEBUG] Preloaded kills for system",
        system_id: system_id,
        kill_count: length(kills)
      )
    end

    {system_id, kills}
  end

  @doc """
  Preloads recent kills for a specific system.
  """
  @spec preload_kills_for_system(integer(), integer(), integer()) :: list()
  def preload_kills_for_system(system_id, limit \\ 5, since_hours \\ 24) do
    alias WandererKills.Core.Storage.KillmailStore

    # Calculate the cutoff time
    cutoff_time = DateTime.utc_now() |> DateTime.add(-since_hours * 3600, :second)

    # Get killmail IDs for the system
    {:ok, killmail_ids} = KillmailStore.get_killmails_for_system(system_id)

    # Get the actual killmail data for each ID
    killmail_ids
    |> Enum.map(&get_killmail_safe/1)
    |> Enum.filter(&(&1 != nil))
    |> Enum.filter(&killmail_recent?(&1, cutoff_time))
    |> Enum.sort_by(& &1["kill_time"], :desc)
    |> Enum.take(limit)
  end

  @doc """
  Preloads recent kills for multiple systems.
  """
  @spec preload_kills_for_systems(list(), keyword()) :: map()
  def preload_kills_for_systems(system_ids, opts \\ []) do
    limit = Keyword.get(opts, :limit, 5)
    since_hours = Keyword.get(opts, :hours, 24)

    system_ids
    |> Enum.map(fn system_id ->
      kills = preload_kills_for_system(system_id, limit, since_hours)
      {system_id, kills}
    end)
    |> Enum.into(%{})
  end

  @doc """
  Preloads recent kills for multiple characters.
  """
  @spec preload_kills_for_characters(list(), keyword()) :: map()
  def preload_kills_for_characters(character_ids, opts \\ []) do
    alias WandererKills.Core.Storage.KillmailStore

    limit = Keyword.get(opts, :limit, 10)
    since_hours = Keyword.get(opts, :hours, 24)
    cutoff_time = DateTime.utc_now() |> DateTime.add(-since_hours * 3600, :second)

    # Get recent killmails from storage by querying recent events
    # Since we don't have list_all_killmails, we'll return empty for now
    # TODO: Implement proper character-based killmail lookup
    all_killmails = []

    character_kills =
      all_killmails
      |> Enum.filter(fn killmail ->
        # Check if any of the character IDs are involved
        victim_id = get_in(killmail, ["victim", "character_id"])

        attacker_ids =
          (killmail["attackers"] || [])
          |> Enum.map(& &1["character_id"])
          |> Enum.filter(&(&1 != nil))

        all_char_ids = [victim_id | attacker_ids] |> Enum.filter(&(&1 != nil))

        Enum.any?(character_ids, &(&1 in all_char_ids))
      end)
      |> Enum.filter(&killmail_recent?(&1, cutoff_time))
      |> Enum.sort_by(& &1["kill_time"], :desc)
      |> Enum.take(limit)

    %{"kills" => character_kills}
  end

  defp get_killmail_safe(id) do
    alias WandererKills.Core.Storage.KillmailStore

    case KillmailStore.get(id) do
      {:ok, killmail} -> killmail
      _ -> nil
    end
  end

  defp killmail_recent?(killmail, cutoff_time) do
    kill_time_str = killmail["kill_time"]

    if kill_time_str do
      case DateTime.from_iso8601(kill_time_str) do
        {:ok, kill_time, _} -> DateTime.compare(kill_time, cutoff_time) == :gt
        _ -> false
      end
    else
      false
    end
  end
end
