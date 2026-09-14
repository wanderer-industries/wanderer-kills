defmodule WandererKills.Ingest.Killmails.ZkbClientBehaviour do
  @moduledoc """
  Behaviour for ZKB (zKillboard) client implementations.
  """

  @doc """
  Fetches a killmail from zKillboard.
  """
  @callback fetch_killmail(integer()) :: {:ok, map()} | {:error, term()}

  @doc """
  Fetches killmails for a system from zKillboard.
  """
  @callback fetch_system_killmails(integer()) :: {:ok, [map()]} | {:error, term()}

  @doc """
  Fetches killmails for a system from zKillboard with options.
  """
  @callback fetch_system_killmails(integer(), keyword()) :: {:ok, [map()]} | {:error, term()}

  @doc """
  Gets the killmail count for a system.
  """
  @callback get_system_killmail_count(integer()) :: {:ok, integer()} | {:error, term()}

  @doc """
  Fetches all pages of killmails for a system, yielding each page to the given function.
  """
  @callback fetch_system_killmails_paginated(integer(), keyword(), (list() -> any())) ::
              {:ok, non_neg_integer()} | {:error, term()}

  @doc """
  Fetches historical killmail IDs for a specific date from zKillboard.
  Date should be in YYYYMMDD format.
  """
  @callback fetch_history(String.t()) :: {:ok, map()} | {:error, term()}

  @doc """
  Gets killmails for a character from zKillboard.
  """
  @callback get_character_killmails(integer()) :: {:ok, [map()]} | {:error, term()}
end

defmodule WandererKills.Ingest.Killmails.ZkbClient do
  @moduledoc """
  Unified ZKB API client for zKillboard with telemetry and processing.

  This module consolidates ZKB API interactions with telemetry, logging,
  and processing functionality. It replaces the previous split architecture
  with a single unified approach.
  """

  @behaviour WandererKills.Ingest.Killmails.ZkbClientBehaviour

  require Logger

  alias WandererKills.Core.Cache
  alias WandererKills.Core.Observability.Telemetry
  alias WandererKills.Core.Support.Error
  alias WandererKills.Http.Client

  # Compile-time configuration
  @zkb_timeout_ms Application.compile_env(:wanderer_kills, [:zkb, :request_timeout_ms], 15_000)
  @zkb_base_url Application.compile_env(
                  :wanderer_kills,
                  [:zkb, :base_url],
                  "https://zkillboard.com/api"
                )

  @type killmail_id :: pos_integer()
  @type system_id :: pos_integer()
  @type killmail :: map()

  @doc """
  Fetches a killmail from zKillboard with telemetry.
  Returns {:ok, killmail} or {:error, reason}.
  """
  @spec fetch_killmail(killmail_id()) :: {:ok, killmail()} | {:error, term()}
  def fetch_killmail(killmail_id) when is_integer(killmail_id) and killmail_id > 0 do
    Logger.debug("Fetching killmail from ZKB",
      killmail_id: killmail_id,
      operation: :fetch_killmail,
      step: :start
    )

    Telemetry.fetch_system_start(killmail_id, 1, :zkb)

    url = "#{base_url()}/killID/#{killmail_id}/"

    request_opts =
      [params: [no_items: true]]

    request_opts = Keyword.put(request_opts, :operation, :fetch_killmail)

    # Make request using injected HTTP client
    case http_client().get_zkb(url, [], request_opts) do
      {:ok, response} ->
        parsed = response.body
        handle_killmail_response(parsed, killmail_id)

      {:error, %Error{} = error} ->
        handle_killmail_error(error, killmail_id)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def fetch_killmail(invalid_id) do
    {:error,
     Error.validation_error(:invalid_format, "Invalid killmail ID format: #{inspect(invalid_id)}")}
  end

  defp handle_killmail_response([killmail], killmail_id) do
    Telemetry.fetch_system_complete(killmail_id, :success)
    {:ok, killmail}
  end

  defp handle_killmail_response([], killmail_id) do
    error = Error.zkb_error(:not_found, "Killmail not found in zKillboard", false)
    Telemetry.fetch_system_error(killmail_id, :not_found, :zkb)
    {:error, error}
  end

  defp handle_killmail_response(killmails, killmail_id) when is_list(killmails) do
    Telemetry.fetch_system_complete(killmail_id, :success)
    {:ok, List.first(killmails)}
  end

  defp handle_killmail_error(%Error{type: :rate_limited} = error, killmail_id) do
    Telemetry.fetch_system_error(killmail_id, error, :zkb)

    Logger.warning("Rate limit exceeded for zkillboard",
      killmail_id: killmail_id,
      operation: :fetch_killmail,
      error: error
    )

    {:error, error}
  end

  defp handle_killmail_error(reason, killmail_id) do
    Telemetry.fetch_system_error(killmail_id, reason, :zkb)
    {:error, reason}
  end

  @doc """
  Fetches killmails for a system from zKillboard with telemetry.
  Returns {:ok, [killmail]} or {:error, reason}.
  """
  @spec fetch_system_killmails(system_id()) :: {:ok, [killmail()]} | {:error, term()}
  def fetch_system_killmails(system_id) when is_integer(system_id) and system_id > 0 do
    fetch_system_killmails(system_id, [])
  end

  def fetch_system_killmails(invalid_id) do
    {:error,
     Error.validation_error(:invalid_format, "Invalid system ID format: #{inspect(invalid_id)}")}
  end

  @doc """
  Fetches killmails for a system from zKillboard with telemetry and options.

  ## Options
    * `:page` - Page number for pagination (default: 1)
    * `:limit` - Number of results per page, max 200 (default: nil, returns zkb default)
    * `:start_time` - ISO8601 timestamp for start of time range
    * `:end_time` - ISO8601 timestamp for end of time range
    * `:past_seconds` - Number of seconds to look back

  Returns {:ok, [killmail]} or {:error, reason}.
  """
  @spec fetch_system_killmails(system_id(), keyword()) :: {:ok, [killmail()]} | {:error, term()}
  def fetch_system_killmails(system_id, opts)
      when is_integer(system_id) and system_id > 0 and is_list(opts) do
    Logger.debug("Fetching system killmails from ZKB",
      system_id: system_id,
      operation: :fetch_system_killmails,
      options: opts,
      step: :start
    )

    Telemetry.fetch_system_start(system_id, 0, :zkb)

    url = "#{base_url()}/systemID/#{system_id}/"

    Logger.debug("[ZKB] Fetching system killmails",
      system_id: system_id,
      data_source: "zkillboard.com/api",
      request_type: "historical_data",
      options: opts
    )

    # Build query parameters from options using consolidated helper
    # Process and validate parameters
    params_with_defaults = opts ++ [no_items: true]

    query_params =
      params_with_defaults
      |> Enum.filter(fn {key, value} -> zkb_param_validator(key, value) end)
      |> Enum.map(fn {key, value} ->
        # Transform snake_case to camelCase for zkillboard API
        camel_key =
          key
          |> Atom.to_string()
          |> String.split("_")
          |> Enum.with_index()
          |> Enum.map(fn
            {part, 0} -> part
            {part, _} -> String.capitalize(part)
          end)
          |> Enum.join()

        {camel_key, value}
      end)

    request_opts =
      [params: query_params, timeout: 60_000]
      |> Keyword.put(:operation, :fetch_system_killmails)

    # Make request using injected HTTP client
    case http_client().get_zkb(url, [], request_opts) do
      {:ok, response} ->
        parsed = response.body
        handle_system_killmails_response(parsed, system_id)

      {:error, %Error{} = error} ->
        handle_system_killmails_error(error, system_id)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def fetch_system_killmails(invalid_id, _opts) do
    {:error,
     Error.validation_error(:invalid_format, "Invalid system ID format: #{inspect(invalid_id)}")}
  end

  defp handle_system_killmails_response(killmails, system_id) when is_list(killmails) do
    Telemetry.fetch_system_success(system_id, length(killmails), :zkb)

    Logger.debug("Successfully fetched system killmails from ZKB",
      system_id: system_id,
      killmail_count: length(killmails),
      operation: :fetch_system_killmails,
      step: :success
    )

    # Validate and log the format of received killmails
    validate_zkb_format(killmails, system_id)

    {:ok, killmails}
  end

  defp handle_system_killmails_response(other, system_id) do
    error = Error.zkb_error(:unexpected_response, "Unexpected response format from ZKB", false)
    Telemetry.fetch_system_error(system_id, error, :zkb)

    Logger.error("Failed to fetch system killmails from ZKB",
      system_id: system_id,
      operation: :fetch_system_killmails,
      error: error,
      unexpected_response: other,
      step: :error
    )

    {:error, error}
  end

  defp handle_system_killmails_error(%Error{type: :rate_limited} = error, system_id) do
    Telemetry.fetch_system_error(system_id, error, :zkb)

    Logger.warning("Rate limit exceeded for zkillboard",
      system_id: system_id,
      operation: :fetch_system_killmails,
      error: error
    )

    {:error, error}
  end

  defp handle_system_killmails_error(reason, system_id) do
    Telemetry.fetch_system_error(system_id, reason, :zkb)

    Logger.error("Failed to fetch system killmails from ZKB",
      system_id: system_id,
      operation: :fetch_system_killmails,
      error: reason,
      step: :error
    )

    {:error, reason}
  end

  @doc """
  Gets killmails for a corporation from zKillboard.
  """
  def get_corporation_killmails(corporation_id) do
    fetch_entity_killmails("corporationID", corporation_id)
  end

  @doc """
  Gets killmails for an alliance from zKillboard.
  """
  def get_alliance_killmails(alliance_id) do
    fetch_entity_killmails("allianceID", alliance_id)
  end

  @doc """
  Gets killmails for a character from zKillboard.
  """
  def get_character_killmails(character_id) do
    fetch_entity_killmails("characterID", character_id)
  end

  # Shared function for fetching killmails by entity type
  defp fetch_entity_killmails(entity_type, entity_id) do
    url = "#{base_url()}/#{entity_type}/#{entity_id}/"
    operation_atom = get_operation_atom(entity_type)
    request_opts = build_entity_request_opts(operation_atom)

    http_client().get_zkb(url, [], request_opts)
    |> handle_entity_response(entity_type, entity_id, operation_atom)
  end

  # Extract operation atom mapping to reduce complexity
  defp get_operation_atom(entity_type) do
    case entity_type do
      "systemID" -> :fetch_system_killmails
      "characterID" -> :fetch_character_killmails
      "corporationID" -> :fetch_corporation_killmails
      "allianceID" -> :fetch_alliance_killmails
      _ -> :fetch_unknown_killmails
    end
  end

  # Build request options for entity requests
  defp build_entity_request_opts(operation_atom) do
    [params: [no_items: true], operation: operation_atom]
  end

  # Handle response from entity killmail requests
  defp handle_entity_response(response, entity_type, entity_id, operation_atom) do
    case response do
      {:ok, response} ->
        {:ok, response.body}

      {:error, %Error{type: :rate_limited} = error} ->
        log_rate_limit_error(entity_type, entity_id, operation_atom, error)
        {:error, error}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Extract rate limit logging to reduce complexity
  defp log_rate_limit_error(entity_type, entity_id, operation_atom, error) do
    Logger.warning("Rate limit exceeded for zkillboard",
      entity_type: entity_type,
      entity_id: entity_id,
      operation: operation_atom,
      error: error
    )
  end

  @doc """
  Fetches killmails for a system from ESI.
  Returns {:ok, [killmail]} or {:error, reason}.
  """
  def fetch_system_killmails_esi(system_id) do
    url = "#{base_url()}/systemID/#{system_id}/"

    request_opts =
      []

    request_opts = Keyword.put(request_opts, :operation, :fetch_system_killmails_esi)

    case http_client().get_zkb(url, [], request_opts) do
      {:ok, response} -> {:ok, response.body}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Enriches a killmail with additional information.
  Returns {:ok, enriched_killmail} or {:error, reason}.
  """
  def enrich_killmail(killmail) do
    with {:ok, victim} <- get_victim_info(killmail),
         {:ok, attackers} <- get_attackers_info(killmail),
         {:ok, items} <- get_items_info(killmail) do
      enriched =
        Map.merge(killmail, %{
          "victim" => victim,
          "attackers" => attackers,
          "items" => items
        })

      {:ok, enriched}
    end
  end

  @doc """
  Gets the killmail count for a system from zKillboard with telemetry.
  Returns {:ok, count} or {:error, reason}.
  """
  @spec get_system_killmail_count(system_id()) :: {:ok, integer()} | {:error, term()}
  def get_system_killmail_count(system_id) when is_integer(system_id) and system_id > 0 do
    Logger.debug("Fetching system killmail count from ZKB",
      system_id: system_id,
      operation: :get_system_killmail_count,
      step: :start
    )

    url = "#{base_url()}/systemID/#{system_id}/"

    request_opts =
      []

    request_opts = Keyword.put(request_opts, :operation, :get_system_killmail_count)

    case http_client().get_zkb(url, [], request_opts) do
      {:ok, response} ->
        case response.body do
          data when is_list(data) ->
            count = length(data)

            Logger.debug("Successfully fetched system killmail count from ZKB",
              system_id: system_id,
              killmail_count: count,
              operation: :get_system_killmail_count,
              step: :success
            )

            {:ok, count}

          _ ->
            error_reason =
              Error.zkb_error(
                :unexpected_response,
                "Expected list data for killmail count but got different format",
                false
              )

            Logger.error("Failed to fetch system killmail count from ZKB",
              system_id: system_id,
              operation: :get_system_killmail_count,
              error: error_reason,
              step: :error
            )

            {:error, error_reason}
        end

      {:error, %Error{} = error} ->
        Logger.error("Failed to fetch system killmail count from ZKB",
          system_id: system_id,
          operation: :get_system_killmail_count,
          error: error,
          step: :error
        )

        {:error, error}

      {:error, reason} ->
        Logger.error("Failed to fetch system killmail count from ZKB",
          system_id: system_id,
          operation: :get_system_killmail_count,
          error: reason,
          step: :error
        )

        {:error, reason}
    end
  end

  def get_system_killmail_count(invalid_id) do
    {:error,
     Error.validation_error(:invalid_format, "Invalid system ID format: #{inspect(invalid_id)}")}
  end

  @doc """
  Fetches active systems from zKillboard with caching.
  """
  @spec fetch_active_systems(keyword()) :: {:ok, [system_id()]} | {:error, term()}
  def fetch_active_systems(opts \\ []) do
    force = Keyword.get(opts, :force, false)

    if force do
      do_fetch_active_systems()
    else
      case fetch_from_cache() do
        {:ok, systems} -> {:ok, systems}
      end
    end
  end

  defp fetch_from_cache do
    case Cache.get_active_systems() do
      {:ok, systems} when is_list(systems) ->
        {:ok, systems}

      _ ->
        {:error, Error.cache_error(:not_found, "Active systems not found in cache")}
    end
  end

  defp do_fetch_active_systems do
    url = "#{base_url()}/systems/"

    request_opts =
      []

    request_opts = Keyword.put(request_opts, :operation, :fetch_active_systems)

    case http_client().get_zkb(url, [], request_opts) do
      {:ok, response} ->
        case response.body do
          systems when is_list(systems) ->
            {:ok, systems}

          _ ->
            {:error,
             Error.zkb_error(
               :unexpected_response,
               "Expected list of systems but got different format",
               false
             )}
        end

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fetches all pages of killmails for a system, yielding each page to the given function.

  ## Options
    * `:start_time` - ISO8601 timestamp for start of time range
    * `:end_time` - ISO8601 timestamp for end of time range
    * `:past_seconds` - Number of seconds to look back
    * `:max_pages` - Maximum number of pages to fetch (default: nil, fetches all)

  Returns {:ok, total_count} or {:error, reason}.
  """
  @spec fetch_system_killmails_paginated(system_id(), keyword(), (list() -> any())) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def fetch_system_killmails_paginated(system_id, opts \\ [], yield_fn)
      when is_integer(system_id) and system_id > 0 and is_function(yield_fn, 1) do
    max_pages = Keyword.get(opts, :max_pages)
    fetch_opts = Keyword.drop(opts, [:max_pages])

    do_fetch_paginated_iterative(system_id, fetch_opts, yield_fn, max_pages)
  end

  defp do_fetch_paginated_iterative(system_id, opts, yield_fn, max_pages) do
    # {current_page, total_count}
    initial_state = {1, 0}

    result =
      Enum.reduce_while(Stream.iterate(1, &(&1 + 1)), initial_state, fn page,
                                                                        {_current_page,
                                                                         total_count} ->
        process_page_iteration(system_id, opts, yield_fn, max_pages, page, total_count)
      end)

    case result do
      {:error, reason} -> {:error, reason}
      {_final_page, total_count} -> {:ok, total_count}
    end
  end

  defp process_page_iteration(system_id, opts, yield_fn, max_pages, page, total_count) do
    # Check if we've hit the max pages limit
    if max_pages && page > max_pages do
      {:halt, {page, total_count}}
    else
      fetch_opts = Keyword.put(opts, :page, page)

      case fetch_system_killmails(system_id, fetch_opts) do
        {:ok, []} ->
          # No more results, stop iteration
          {:halt, {page, total_count}}

        {:ok, killmails} ->
          # Yield this page to the callback
          yield_fn.(killmails)

          # Continue with updated total
          new_total = total_count + length(killmails)
          {:cont, {page, new_total}}

        {:error, reason} ->
          # Propagate error by halting with error
          {:halt, {:error, reason}}
      end
    end
  end

  # ZKB-specific parameter validation for use with consolidated helper
  defp zkb_param_validator(:page, page) when is_integer(page) and page > 0, do: true

  defp zkb_param_validator(:limit, limit) when is_integer(limit) and limit > 0 and limit <= 200,
    do: true

  defp zkb_param_validator(:start_time, start_time) when is_binary(start_time), do: true
  defp zkb_param_validator(:end_time, end_time) when is_binary(end_time), do: true

  defp zkb_param_validator(:past_seconds, seconds) when is_integer(seconds) and seconds > 0,
    do: true

  defp zkb_param_validator(:no_items, no_items) when is_boolean(no_items), do: true
  defp zkb_param_validator(_, _), do: false

  # Helper functions for enriching killmails
  defp get_victim_info(%{victim: victim}) when not is_nil(victim) do
    {:ok, victim}
  end

  defp get_victim_info(killmail) when is_map(killmail) do
    victim = Map.get(killmail, "victim", %{})
    {:ok, victim}
  end

  defp get_attackers_info(%{attackers: attackers}) when not is_nil(attackers) do
    {:ok, attackers}
  end

  defp get_attackers_info(killmail) when is_map(killmail) do
    attackers = Map.get(killmail, "attackers", [])
    {:ok, attackers}
  end

  defp get_items_info(%{items: items}) when not is_nil(items) do
    {:ok, items}
  end

  defp get_items_info(killmail) when is_map(killmail) do
    items = Map.get(killmail, "items", [])
    {:ok, items}
  end

  @doc """
  Gets the base URL for zKillboard API calls.
  """
  def base_url do
    @zkb_base_url
  end

  # Note: Response parsing now handled by WandererKills.Http.Client

  @doc """
  Validates and logs the format of killmails received from zKillboard API.
  """
  def validate_zkb_format(killmails, system_id) when is_list(killmails) do
    Logger.debug("[ZKB] Received killmails",
      system_id: system_id,
      killmail_count: length(killmails),
      data_source: "zkillboard.com/api"
    )

    # Track format for telemetry if we have killmails
    if killmails != [] do
      sample = List.first(killmails)

      format_type =
        cond do
          Map.has_key?(sample, "victim") && Map.has_key?(sample, "attackers") ->
            :full_esi_format

          Map.has_key?(sample, "killmail_id") && Map.has_key?(sample, "zkb") ->
            :zkb_reference_format

          true ->
            :unknown_format
        end

      # Emit telemetry event
      :ok =
        Telemetry.zkb_format(format_type, %{
          source: :zkb_api,
          system_id: system_id,
          count: length(killmails)
        })
    end
  end

  @doc """
  Fetches historical killmail IDs for a specific date from zKillboard.
  Date should be in YYYYMMDD format (e.g., "20240101").
  Returns {:ok, map} where map contains killmail_id => hash pairs.
  """
  @spec fetch_history(String.t()) :: {:ok, map()} | {:error, Error.t()}
  def fetch_history(date) when is_binary(date) do
    # Validate date format (YYYYMMDD)
    if Regex.match?(~r/^\d{8}$/, date) do
      Logger.debug("Fetching historical kills from ZKB",
        date: date,
        operation: :fetch_history,
        step: :start
      )

      fetch_history_with_rate_limit(date)
    else
      {:error, Error.validation_error(:invalid_format, "Invalid date format: #{inspect(date)}")}
    end
  end

  def fetch_history(invalid_date) do
    {:error,
     Error.validation_error(:invalid_format, "Invalid date format: #{inspect(invalid_date)}")}
  end

  defp fetch_history_with_rate_limit(date) do
    url = "#{@zkb_base_url}/history/#{date}.json"

    Logger.debug("Making HTTP request to zkillboard history API",
      url: url,
      date: date
    )

    case http_client().get_zkb(url, [], timeout: @zkb_timeout_ms) do
      {:ok, %{body: body}} ->
        handle_history_response(body, date)

      {:error, %Error{} = error} ->
        handle_history_error(error, date)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_history_response(response, date) when is_map(response) do
    Logger.debug("Successfully fetched historical kills",
      date: date,
      count: map_size(response)
    )

    {:ok, response}
  end

  # Dialyzer can't determine when this clause would be reached, but it's a defensive measure
  # for cases where the API returns non-JSON content (e.g., error pages)
  @dialyzer {:nowarn_function, handle_history_response: 2}
  defp handle_history_response(response, date) do
    Logger.error("Unexpected response format from zkillboard history API",
      date: date,
      response: response,
      response_type: typeof(response)
    )

    {:error, Error.http_error(:invalid_response, "Expected map, got #{typeof(response)}")}
  end

  # Helper function to get type information for logging
  @dialyzer {:nowarn_function, typeof: 1}
  defp typeof(term) when is_map(term), do: "map"
  defp typeof(term) when is_atom(term), do: "atom"
  defp typeof(term) when is_binary(term), do: "string"
  defp typeof(term) when is_list(term), do: "list"
  defp typeof(term) when is_number(term), do: "number"
  defp typeof(_term), do: "unknown"

  defp handle_history_error(%Error{type: :rate_limited} = error, date) do
    Logger.warning("Rate limit exceeded for zkillboard history",
      date: date,
      operation: :fetch_history,
      error: error
    )

    {:error, error}
  end

  defp handle_history_error(reason, date) do
    Logger.error("Failed to fetch historical kills",
      date: date,
      error: reason
    )

    {:error, reason}
  end

  # Get the configured HTTP client implementation
  defp http_client do
    Application.get_env(:wanderer_kills, :http, [])[:client] || Client
  end
end
