defmodule WandererKills.Ingest.ESI.Client do
  @moduledoc """
  ESI (EVE Swagger Interface) API client.

  This module provides data fetching capabilities for EVE Online's ESI API.
  It handles caching, concurrent requests, error handling, and rate limiting
  for all ESI operations including characters, corporations, alliances,
  ship types, systems, and killmails.

  ## Features

  - Type-specific methods for each ESI entity type
  - Automatic caching with configurable TTLs
  - Concurrent batch operations
  - Rate limiting and error handling
  - Generic fetch interface for flexibility
  """

  @behaviour WandererKills.Ingest.ESI.ClientBehaviour

  require Logger

  alias WandererKills.Core.Cache
  alias WandererKills.Core.Support.Error
  alias WandererKills.Core.Support.Utils
  alias WandererKills.Http.Client, as: HttpClient

  # Default ship group IDs that contain ship types
  @ship_group_ids [6, 7, 9, 11, 16, 17, 23]

  # Compile-time configuration
  @esi_concurrency Application.compile_env(:wanderer_kills, [:esi, :batch_concurrency], 10)
  @esi_base_url Application.compile_env(
                  :wanderer_kills,
                  [:esi, :base_url],
                  "https://esi.evetech.net/latest"
                )

  # ============================================================================
  # ESI.ClientBehaviour Implementation
  # ============================================================================

  @impl true
  def get_character(character_id) when is_integer(character_id) do
    case Cache.get_character(character_id) do
      {:ok, character_data} ->
        {:ok, character_data}

      {:error, %Error{type: :not_found}} ->
        case fetch_from_api(:character, character_id) do
          %{} = data ->
            Cache.put_character(character_id, data)
            {:ok, data}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def get_character_batch(character_ids) when is_list(character_ids) do
    fetch_batch(:character, character_ids)
  end

  @impl true
  def get_corporation(corporation_id) when is_integer(corporation_id) do
    case Cache.get_corporation(corporation_id) do
      {:ok, corp_data} ->
        {:ok, corp_data}

      {:error, %Error{type: :not_found}} ->
        case fetch_from_api(:corporation, corporation_id) do
          %{} = data ->
            Cache.put_corporation(corporation_id, data)
            {:ok, data}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def get_corporation_batch(corporation_ids) when is_list(corporation_ids) do
    fetch_batch(:corporation, corporation_ids)
  end

  @impl true
  def get_alliance(alliance_id) when is_integer(alliance_id) do
    case Cache.get_alliance(alliance_id) do
      {:ok, alliance_data} ->
        {:ok, alliance_data}

      {:error, %Error{type: :not_found}} ->
        case fetch_from_api(:alliance, alliance_id) do
          %{} = data ->
            Cache.put_alliance(alliance_id, data)
            {:ok, data}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def get_alliance_batch(alliance_ids) when is_list(alliance_ids) do
    fetch_batch(:alliance, alliance_ids)
  end

  @impl true
  def get_type(type_id) when is_integer(type_id) do
    case Cache.get_ship_type(type_id) do
      {:ok, type_data} ->
        {:ok, type_data}

      {:error, %Error{type: :not_found}} ->
        case fetch_from_api(:type, type_id) do
          %{} = data ->
            Cache.put_ship_type(type_id, data)
            {:ok, data}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def get_type_batch(type_ids) when is_list(type_ids) do
    fetch_batch(:type, type_ids)
  end

  @impl true
  def get_group(group_id) when is_integer(group_id) do
    case Cache.get(:esi_data, "group:#{group_id}") do
      {:ok, group_data} ->
        {:ok, group_data}

      {:error, %Error{type: :not_found}} ->
        case fetch_from_api(:group, group_id) do
          %{} = data ->
            Cache.put(:esi_data, "group:#{group_id}", data)
            {:ok, data}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def get_group_batch(group_ids) when is_list(group_ids) do
    fetch_batch(:group, group_ids)
  end

  @impl true
  def get_system(system_id) when is_integer(system_id) do
    Cache.get_or_set(:systems, system_id, fn ->
      fetch_from_api(:system, system_id)
    end)
  end

  @impl true
  def get_system_batch(system_ids) when is_list(system_ids) do
    fetch_batch(:system, system_ids)
  end

  # ============================================================================
  # Generic Fetch Implementation
  # ============================================================================

  @impl true
  def fetch({:character, character_id}), do: get_character(character_id)
  def fetch({:corporation, corporation_id}), do: get_corporation(corporation_id)
  def fetch({:alliance, alliance_id}), do: get_alliance(alliance_id)
  def fetch({:type, type_id}), do: get_type(type_id)
  def fetch({:group, group_id}), do: get_group(group_id)
  def fetch({:system, system_id}), do: get_system(system_id)
  def fetch({:killmail, killmail_id, killmail_hash}), do: get_killmail(killmail_id, killmail_hash)
  def fetch(_), do: {:error, Error.esi_error(:unsupported, "Unsupported fetch operation")}

  @doc """
  Checks if the given fetch operation is supported.
  """
  def supports?({:character, _}), do: true
  def supports?({:corporation, _}), do: true
  def supports?({:alliance, _}), do: true
  def supports?({:type, _}), do: true
  def supports?({:group, _}), do: true
  def supports?({:system, _}), do: true
  def supports?({:killmail, _, _}), do: true
  def supports?(_), do: false

  # ============================================================================
  # Killmail-specific functions
  # ============================================================================

  @doc """
  Fetches a killmail from ESI using killmail ID and hash.
  """
  def get_killmail(killmail_id, killmail_hash)
      when is_integer(killmail_id) and is_binary(killmail_hash) do
    Cache.get_or_set(:killmails, killmail_id, fn ->
      fetch_killmail_from_api(killmail_id, killmail_hash)
    end)
  end

  @doc """
  Fetches multiple killmails concurrently.
  """
  def get_killmails_batch(killmail_specs) when is_list(killmail_specs) do
    killmail_specs
    |> Flow.from_enumerable(max_demand: @esi_concurrency)
    |> Flow.map(fn {killmail_id, killmail_hash} ->
      get_killmail(killmail_id, killmail_hash)
    end)
    |> Flow.partition()
    |> Enum.to_list()
  end

  # ============================================================================
  # Ship type utilities
  # ============================================================================

  @doc """
  Returns the default ship group IDs.
  """
  def ship_group_ids, do: @ship_group_ids

  @doc """
  Updates ship groups by fetching fresh data from ESI.
  """
  def update_ship_groups(group_ids \\ @ship_group_ids) when is_list(group_ids) do
    Logger.info("Updating ship groups from ESI", group_ids: group_ids)

    results =
      group_ids
      |> Flow.from_enumerable(max_demand: @esi_concurrency)
      |> Flow.map(&get_group/1)
      |> Flow.partition()
      |> Enum.to_list()

    errors = Enum.filter(results, &match?({:error, _}, &1))

    if errors != [] do
      Logger.error("Failed to update some ship groups",
        error_count: length(errors),
        total_groups: length(group_ids)
      )

      {:error, {:partial_failure, errors}}
    else
      Logger.info("Successfully updated all ship groups")
      :ok
    end
  end

  @doc """
  Fetches types for specific groups and returns parsed ship data.
  """
  def fetch_ship_types_for_groups(group_ids \\ @ship_group_ids) when is_list(group_ids) do
    Logger.info("Fetching ship types for groups", group_ids: group_ids)

    with {:ok, groups} <- fetch_groups(group_ids),
         {:ok, ship_types} <- extract_and_fetch_types(groups) do
      {:ok, ship_types}
    else
      {:error, reason} ->
        Logger.error("Failed to fetch ship types", error: reason)
        {:error, reason}
    end
  end

  @doc """
  Fetches a killmail directly from ESI API (raw implementation).

  This provides direct access to the ESI API for killmail fetching,
  bypassing the cache layer. Used by the parser when fresh killmail data is needed.
  """
  @spec get_killmail_raw(integer(), String.t()) :: {:ok, map()} | {:error, term()}
  def get_killmail_raw(killmail_id, killmail_hash) do
    url = "#{esi_base_url()}/killmails/#{killmail_id}/#{killmail_hash}/"

    Utils.retry_http_operation(
      fn ->
        case HttpClient.get_esi(url, [], []) do
          {:ok, %{body: body}} -> {:ok, body}
          {:error, reason} -> {:error, reason}
        end
      end,
      operation_name: "ESI get raw killmail #{killmail_id}"
    )
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp fetch_groups(group_ids) do
    Logger.debug("Fetching groups from ESI", group_ids: group_ids)

    results =
      group_ids
      |> Flow.from_enumerable(max_demand: @esi_concurrency)
      |> Flow.map(&get_group/1)
      |> Flow.partition()
      |> Enum.to_list()

    errors = Enum.filter(results, &match?({:error, _}, &1))
    successes = Enum.filter(results, &match?({:ok, _}, &1))

    if errors != [] do
      Logger.error("Failed to fetch some groups",
        error_count: length(errors),
        success_count: length(successes)
      )

      {:error, {:partial_failure, errors}}
    else
      groups = Enum.map(successes, fn {:ok, group} -> group end)
      {:ok, groups}
    end
  end

  defp extract_and_fetch_types(groups) do
    Logger.debug("Extracting type IDs from groups")

    type_ids =
      groups
      |> Enum.flat_map(fn group -> Map.get(group, "types", []) end)
      |> Enum.uniq()

    Logger.debug("Fetching types", type_count: length(type_ids))

    results =
      type_ids
      |> Flow.from_enumerable(max_demand: @esi_concurrency)
      |> Flow.map(&get_type/1)
      |> Flow.partition()
      |> Enum.to_list()

    errors = Enum.filter(results, &match?({:error, _}, &1))
    successes = Enum.filter(results, &match?({:ok, _}, &1))

    if errors != [] do
      Logger.error("Failed to fetch some types",
        error_count: length(errors),
        success_count: length(successes)
      )

      {:error, {:partial_failure, errors}}
    else
      types = Enum.map(successes, fn {:ok, type} -> type end)
      {:ok, types}
    end
  end

  defp fetch_batch(entity_type, ids) when is_list(ids) do
    ids
    |> Flow.from_enumerable(max_demand: @esi_concurrency)
    |> Flow.map(fn id -> fetch_from_api(entity_type, id) end)
    |> Flow.partition()
    |> Enum.to_list()
  end

  # Common HTTP response handler that reduces code duplication
  defp handle_http_response({:ok, response}, {entity_type, entity_id}, success_handler) do
    # Validate response body is a map before parsing
    case response do
      %{body: body} when is_map(body) ->
        success_handler.(entity_type, entity_id, response)

      %{body: ""} ->
        Logger.error("Received empty response body from ESI",
          entity_type: entity_type,
          entity_id: entity_id
        )

        {:error,
         Error.esi_error(:empty_response, "Empty response body from ESI", true, %{
           entity_type: entity_type,
           entity_id: entity_id
         })}

      %{body: body} ->
        Logger.error("Received invalid response body type from ESI",
          entity_type: entity_type,
          entity_id: entity_id,
          body_type: inspect(body)
        )

        {:error,
         Error.esi_error(:invalid_response, "Invalid response body type from ESI", false, %{
           entity_type: entity_type,
           entity_id: entity_id,
           body_type: inspect(body)
         })}
    end
  end

  defp handle_http_response({:error, reason}, {entity_type, entity_id}, _success_handler) do
    Logger.error("Failed to fetch entity from ESI",
      entity_type: entity_type,
      entity_id: entity_id,
      error: reason
    )

    {:error,
     Error.esi_error(:api_error, "Failed to fetch #{entity_type} from ESI", false, %{
       entity_type: entity_type,
       entity_id: entity_id,
       reason: reason
     })}
  end

  # Specific handler for killmail responses with detailed error handling
  defp handle_killmail_response({:ok, response}, killmail_id, killmail_hash) do
    # Validate response body is a map before parsing
    case response do
      %{body: body} when is_map(body) ->
        parse_killmail_response(killmail_id, killmail_hash, response)

      %{body: ""} ->
        Logger.error("Received empty response body from ESI for killmail",
          killmail_id: killmail_id,
          killmail_hash: String.slice(killmail_hash, 0, 8) <> "..."
        )

        {:error,
         Error.esi_error(:empty_response, "Empty response body from ESI", true, %{
           killmail_id: killmail_id,
           killmail_hash: killmail_hash
         })}

      %{body: body} ->
        Logger.error("Received invalid response body type from ESI for killmail",
          killmail_id: killmail_id,
          killmail_hash: String.slice(killmail_hash, 0, 8) <> "...",
          body_type: inspect(body)
        )

        {:error,
         Error.esi_error(:invalid_response, "Invalid response body type from ESI", false, %{
           killmail_id: killmail_id,
           killmail_hash: killmail_hash,
           body_type: inspect(body)
         })}
    end
  end

  defp handle_killmail_response(
         {:error, %Error{type: :not_found} = error},
         killmail_id,
         killmail_hash
       ) do
    {:error,
     Error.esi_error(:not_found, "Killmail not found", false, %{
       killmail_id: killmail_id,
       killmail_hash: killmail_hash,
       original_error: error
     })}
  end

  defp handle_killmail_response(
         {:error, %Error{type: :rate_limited} = error},
         killmail_id,
         killmail_hash
       ) do
    {:error,
     Error.esi_error(:forbidden, "Killmail access forbidden", false, %{
       killmail_id: killmail_id,
       killmail_hash: killmail_hash,
       original_error: error
     })}
  end

  defp handle_killmail_response(
         {:error, %Error{type: :server_error} = error},
         killmail_id,
         killmail_hash
       ) do
    {:error,
     Error.esi_error(:server_error, "ESI server error", true, %{
       killmail_id: killmail_id,
       killmail_hash: killmail_hash,
       original_error: error
     })}
  end

  defp handle_killmail_response({:error, reason}, killmail_id, killmail_hash) do
    {:error,
     Error.esi_error(:api_error, "Failed to fetch killmail from ESI", false, %{
       killmail_id: killmail_id,
       killmail_hash: killmail_hash,
       reason: reason
     })}
  end

  defp fetch_from_api(entity_type, entity_id) do
    url = build_url(entity_type, entity_id)

    result =
      Utils.retry_http_operation(
        fn ->
          HttpClient.get_esi(url, default_headers(), request_options())
        end,
        operation_name: "ESI fetch #{entity_type} #{entity_id}"
      )

    result
    |> handle_http_response({entity_type, entity_id}, &parse_response/3)
  end

  defp fetch_killmail_from_api(killmail_id, killmail_hash) do
    url = "#{esi_base_url()}/killmails/#{killmail_id}/#{killmail_hash}/"

    Logger.debug("Fetching killmail from ESI",
      killmail_id: killmail_id,
      killmail_hash: String.slice(killmail_hash, 0, 8) <> "..."
    )

    result =
      Utils.retry_http_operation(
        fn ->
          HttpClient.get_esi(url, default_headers(), request_options())
        end,
        operation_name: "ESI fetch killmail #{killmail_id}"
      )

    result
    |> handle_killmail_response(killmail_id, killmail_hash)
  end

  defp build_url(:character, id), do: "#{esi_base_url()}/characters/#{id}/"
  defp build_url(:corporation, id), do: "#{esi_base_url()}/corporations/#{id}/"
  defp build_url(:alliance, id), do: "#{esi_base_url()}/alliances/#{id}/"
  defp build_url(:type, id), do: "#{esi_base_url()}/universe/types/#{id}/"
  defp build_url(:group, id), do: "#{esi_base_url()}/universe/groups/#{id}/"
  defp build_url(:system, id), do: "#{esi_base_url()}/universe/systems/#{id}/"

  defp parse_response(:character, id, %{body: body}) do
    %{
      "character_id" => id,
      "name" => Map.get(body, "name"),
      "corporation_id" => Map.get(body, "corporation_id"),
      "alliance_id" => Map.get(body, "alliance_id"),
      "birthday" => Map.get(body, "birthday"),
      "gender" => Map.get(body, "gender"),
      "race_id" => Map.get(body, "race_id"),
      "bloodline_id" => Map.get(body, "bloodline_id"),
      "ancestry_id" => Map.get(body, "ancestry_id"),
      "security_status" => Map.get(body, "security_status")
    }
  end

  defp parse_response(:corporation, id, %{body: body}) do
    %{
      "corporation_id" => id,
      "name" => Map.get(body, "name"),
      "ticker" => Map.get(body, "ticker"),
      "alliance_id" => Map.get(body, "alliance_id"),
      "ceo_id" => Map.get(body, "ceo_id"),
      "creator_id" => Map.get(body, "creator_id"),
      "date_founded" => Map.get(body, "date_founded"),
      "description" => Map.get(body, "description"),
      "faction_id" => Map.get(body, "faction_id"),
      "home_station_id" => Map.get(body, "home_station_id"),
      "member_count" => Map.get(body, "member_count"),
      "shares" => Map.get(body, "shares"),
      "tax_rate" => Map.get(body, "tax_rate"),
      "url" => Map.get(body, "url"),
      "war_eligible" => Map.get(body, "war_eligible")
    }
  end

  defp parse_response(:alliance, id, %{body: body}) do
    %{
      "alliance_id" => id,
      "name" => Map.get(body, "name"),
      "ticker" => Map.get(body, "ticker"),
      "creator_corporation_id" => Map.get(body, "creator_corporation_id"),
      "creator_id" => Map.get(body, "creator_id"),
      "date_founded" => Map.get(body, "date_founded"),
      "executor_corporation_id" => Map.get(body, "executor_corporation_id"),
      "faction_id" => Map.get(body, "faction_id")
    }
  end

  defp parse_response(:type, id, %{body: body}) do
    %{
      "type_id" => id,
      "name" => Map.get(body, "name"),
      "description" => Map.get(body, "description"),
      "group_id" => Map.get(body, "group_id"),
      "category_id" => Map.get(body, "category_id"),
      "published" => Map.get(body, "published"),
      "mass" => Map.get(body, "mass"),
      "volume" => Map.get(body, "volume"),
      "capacity" => Map.get(body, "capacity"),
      "portion_size" => Map.get(body, "portion_size"),
      "radius" => Map.get(body, "radius"),
      "graphic_id" => Map.get(body, "graphic_id"),
      "icon_id" => Map.get(body, "icon_id"),
      "market_group_id" => Map.get(body, "market_group_id"),
      "packaged_volume" => Map.get(body, "packaged_volume")
    }
  end

  defp parse_response(:group, id, %{body: body}) do
    %{
      "group_id" => id,
      "name" => Map.get(body, "name"),
      "category_id" => Map.get(body, "category_id"),
      "published" => Map.get(body, "published"),
      "types" => Map.get(body, "types", [])
    }
  end

  defp parse_response(:system, id, %{body: body}) do
    %{
      "system_id" => id,
      "name" => Map.get(body, "name"),
      "constellation_id" => Map.get(body, "constellation_id"),
      "security_class" => Map.get(body, "security_class"),
      "security_status" => Map.get(body, "security_status"),
      "star_id" => Map.get(body, "star_id"),
      "stargates" => Map.get(body, "stargates", []),
      "stations" => Map.get(body, "stations", []),
      "planets" => Map.get(body, "planets", [])
    }
  end

  defp parse_killmail_response(killmail_id, killmail_hash, %{body: body}) do
    body
    |> Map.put("killmail_id", killmail_id)
    |> Map.put("killmail_hash", killmail_hash)
  end

  defp esi_base_url, do: @esi_base_url

  defp default_headers do
    # EVE API doesn't require specific headers
    []
  end

  defp request_options do
    []
  end

  @doc """
  Gets ESI base URL from configuration.
  """
  def base_url, do: @esi_base_url

  @doc """
  Returns the source name for this ESI client.
  """
  def source_name, do: "ESI"

  @doc """
  General update function that delegates to update_ship_groups.

  This provides compatibility for modules that expect a general update function.

  ## Options
  - `opts` - Keyword list of options
    - `group_ids` - List of group IDs to fetch (optional)

  ## Examples
      iex> WandererKills.Ingest.ESI.Client.update()
      :ok

      iex> WandererKills.Ingest.ESI.Client.update(group_ids: [23, 16])
      :ok
  """
  def update(opts \\ []) do
    group_ids = Keyword.get(opts, :group_ids)
    update_ship_groups(group_ids)
  end
end
