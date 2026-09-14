defmodule WandererKillsWeb.KillsController do
  @moduledoc """
  Controller for kill-related API endpoints.

  This controller provides endpoints for fetching killmails, cached data,
  and kill counts as specified in the WandererKills API interface.
  """

  use WandererKillsWeb, :controller
  use OpenApiSpex.ControllerSpecs

  import WandererKillsWeb.Api.Validators
  require Logger
  alias WandererKills.Core.Storage.KillmailStore
  alias WandererKills.Core.Support.Error
  alias WandererKills.Core.Support.Utils
  alias WandererKills.Ingest.Killmails.ZkbClient

  operation(:list,
    summary: "List kills for a system",
    description: "Fetches killmail data for a specific system with optional time filtering",
    parameters: [
      system_id: [
        in: :path,
        description: "EVE Online system ID",
        type: :integer,
        required: true,
        example: 30_000_142
      ],
      since_hours: [
        in: :query,
        description: "Fetch kills from the last N hours (default: 24)",
        type: :integer,
        required: false,
        example: 24
      ],
      limit: [
        in: :query,
        description: "Maximum number of kills to return",
        type: :integer,
        required: false,
        example: 100
      ]
    ],
    responses: %{
      200 => {"Success", "application/json", WandererKillsWeb.Schemas.KillsResponse},
      400 => {"Invalid parameters", "application/json", WandererKillsWeb.Schemas.Error},
      500 => {"Server error", "application/json", WandererKillsWeb.Schemas.Error}
    }
  )

  @doc """
  Lists kills for a specific system with time filtering.

  GET /api/v1/kills/system/:system_id?since_hours=X&limit=Y
  """
  @spec list(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def list(conn, %{"system_id" => system_id_str} = params) do
    with {:ok, system_id} <- validate_system_id(system_id_str),
         {:ok, since_hours} <- validate_since_hours(Map.get(params, "since_hours", "24")),
         {:ok, limit} <- validate_limit(Map.get(params, "limit")) do
      Logger.info("Fetching system kills",
        system_id: system_id,
        since_hours: since_hours,
        limit: limit
      )

      case fetch_system_killmails_with_retry(system_id, since_hours) do
        {:ok, killmails} ->
          # Apply limit and time filtering
          filtered_killmails =
            killmails
            |> filter_killmails_by_time(since_hours)
            |> Enum.take(limit)

          response = build_cached_response(filtered_killmails, false)
          render_success(conn, response)

        {:error, reason} ->
          Logger.error("Failed to fetch system kills",
            system_id: system_id,
            error: reason
          )

          render_error(conn, 500, "Failed to fetch system kills", "FETCH_ERROR", %{
            reason: inspect(reason)
          })
      end
    else
      {:error, %Error{}} ->
        render_error(conn, 400, "Invalid system ID format", "INVALID_SYSTEM_ID")
    end
  end

  operation(:bulk,
    summary: "Fetch kills for multiple systems",
    description: "Fetches killmail data for multiple systems with optional time filtering",
    request_body:
      {"Request body", "application/json",
       %OpenApiSpex.Schema{
         type: :object,
         properties: %{
           system_ids: %OpenApiSpex.Schema{
             type: :array,
             items: %OpenApiSpex.Schema{type: :integer},
             description: "List of EVE Online system IDs",
             example: [30_000_142, 30_000_144]
           },
           since_hours: %OpenApiSpex.Schema{
             type: :integer,
             description: "Fetch kills from the last N hours",
             example: 24
           },
           limit: %OpenApiSpex.Schema{
             type: :integer,
             description: "Maximum number of kills to return per system",
             example: 100
           }
         },
         required: [:system_ids]
       }},
    responses: %{
      200 => {"Success", "application/json", WandererKillsWeb.Schemas.BulkKillsResponse},
      400 => {"Invalid parameters", "application/json", WandererKillsWeb.Schemas.Error},
      500 => {"Server error", "application/json", WandererKillsWeb.Schemas.Error}
    }
  )

  @doc """
  Fetches kills for multiple systems.

  POST /api/v1/kills/systems
  Body: {"system_ids": [int], "since_hours": int, "limit": int}
  """
  @spec bulk(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def bulk(conn, params) do
    with {:ok, system_ids} <- validate_system_ids(Map.get(params, "system_ids")),
         {:ok, since_hours} <- validate_since_hours(Map.get(params, "since_hours", 24)),
         {:ok, limit} <- validate_limit(Map.get(params, "limit")) do
      Logger.info("Fetching kills for multiple systems",
        system_count: length(system_ids),
        since_hours: since_hours,
        limit: limit
      )

      systems_killmails = fetch_systems_killmails(system_ids, since_hours, limit)

      response = %{
        systems_kills: systems_killmails,
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
      }

      render_success(conn, response)
    else
      {:error, %Error{type: :invalid_system_ids}} ->
        render_error(conn, 400, "Invalid system IDs", "INVALID_SYSTEM_IDS")

      {:error, %Error{}} ->
        render_error(conn, 400, "Invalid parameters", "INVALID_PARAMETERS")
    end
  end

  operation(:cached,
    summary: "Get cached kills for a system",
    description: "Returns cached killmail data for a specific system",
    parameters: [
      system_id: [
        in: :path,
        description: "EVE Online system ID",
        type: :integer,
        required: true,
        example: 30_000_142
      ]
    ],
    responses: %{
      200 => {"Success", "application/json", WandererKillsWeb.Schemas.KillsResponse},
      400 => {"Invalid parameters", "application/json", WandererKillsWeb.Schemas.Error}
    }
  )

  @doc """
  Returns cached kills for a system.

  GET /api/v1/kills/cached/:system_id
  """
  @spec cached(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def cached(conn, %{"system_id" => system_id_str}) do
    case validate_system_id(system_id_str) do
      {:ok, system_id} ->
        Logger.debug("Fetching cached kills", system_id: system_id)

        killmails = KillmailStore.get_system_killmails(system_id)
        response = build_cached_response(killmails, true)
        render_success(conn, response)

      {:error, %Error{}} ->
        render_error(conn, 400, "Invalid system ID format", "INVALID_SYSTEM_ID")
    end
  end

  operation(:show,
    summary: "Get a specific killmail",
    description: "Returns detailed information about a specific killmail",
    parameters: [
      killmail_id: [
        in: :path,
        description: "Killmail ID",
        type: :integer,
        required: true,
        example: 123_456_789
      ]
    ],
    responses: %{
      200 => {"Success", "application/json", WandererKillsWeb.Schemas.KillmailResponse},
      400 => {"Invalid parameters", "application/json", WandererKillsWeb.Schemas.Error},
      404 => {"Killmail not found", "application/json", WandererKillsWeb.Schemas.Error}
    }
  )

  @doc """
  Shows a specific killmail by ID.

  GET /api/v1/killmail/:killmail_id
  """
  @spec show(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def show(conn, %{"killmail_id" => killmail_id_str}) do
    case validate_killmail_id(killmail_id_str) do
      {:ok, killmail_id} ->
        fetch_killmail_with_fallback(conn, killmail_id)

      {:error, %Error{}} ->
        render_error(conn, 400, "Invalid killmail ID format", "INVALID_KILLMAIL_ID")
    end
  end

  defp fetch_killmail_with_fallback(conn, killmail_id) do
    Logger.debug("Fetching specific killmail", killmail_id: killmail_id)

    case KillmailStore.get(killmail_id) do
      {:ok, killmail} ->
        render_success(conn, killmail)

      {:error, _} ->
        fetch_killmail_from_zkb(conn, killmail_id)
    end
  end

  defp fetch_killmail_from_zkb(conn, killmail_id) do
    case Utils.retry_http_operation(
           fn -> ZkbClient.fetch_killmail(killmail_id) end,
           operation_name: "ZKB fetch individual killmail #{killmail_id}"
         ) do
      {:ok, raw_killmail} ->
        # Process through enrichment pipeline for full API response
        case enrich_killmail_for_api(raw_killmail) do
          {:ok, enriched_killmail} ->
            render_success(conn, enriched_killmail)

          {:error, _} ->
            # Fallback to raw data if enrichment fails
            render_success(conn, raw_killmail)
        end

      {:error, _} ->
        render_error(conn, 404, "Killmail not found", "NOT_FOUND")
    end
  end

  defp enrich_killmail_for_api(raw_killmail) do
    alias WandererKills.Ingest.Killmails.UnifiedProcessor
    alias WandererKills.Domain.Killmail

    # Process through enrichment pipeline without storing
    case UnifiedProcessor.process_killmail(
           raw_killmail,
           DateTime.add(DateTime.utc_now(), -7, :day),
           store: false,
           enrich: true
         ) do
      {:ok, %Killmail{} = enriched_killmail} ->
        # Convert to map for JSON response
        {:ok, Killmail.to_map(enriched_killmail)}

      {:ok, :kill_older} ->
        # Older killmail, return enriched version anyway
        case UnifiedProcessor.process_killmail(
               raw_killmail,
               DateTime.add(DateTime.utc_now(), -365, :day),
               store: false,
               enrich: true
             ) do
          {:ok, %Killmail{} = enriched_killmail} ->
            {:ok, Killmail.to_map(enriched_killmail)}

          error ->
            error
        end

      error ->
        error
    end
  end

  operation(:count,
    summary: "Get kill count for a system",
    description: "Returns the total number of kills for a specific system",
    parameters: [
      system_id: [
        in: :path,
        description: "EVE Online system ID",
        type: :integer,
        required: true,
        example: 30_000_142
      ]
    ],
    responses: %{
      200 => {"Success", "application/json", WandererKillsWeb.Schemas.KillCountResponse},
      400 => {"Invalid parameters", "application/json", WandererKillsWeb.Schemas.Error}
    }
  )

  @doc """
  Returns kill count for a system.

  GET /api/v1/kills/count/:system_id
  """
  @spec count(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def count(conn, %{"system_id" => system_id_str}) do
    case validate_system_id(system_id_str) do
      {:ok, system_id} ->
        Logger.debug("Fetching system kill count", system_id: system_id)

        {:ok, count} = KillmailStore.get_system_killmail_count(system_id)

        response = %{
          system_id: system_id,
          count: count,
          timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
        }

        render_success(conn, response)

      {:error, %Error{}} ->
        render_error(conn, 400, "Invalid system ID format", "INVALID_SYSTEM_ID")
    end
  end

  operation(:not_found,
    summary: "Handle undefined API routes",
    description: "Returns a 404 error for undefined API routes",
    responses: %{
      404 => {"Not Found", "application/json", WandererKillsWeb.Schemas.Error}
    }
  )

  @doc """
  Handles undefined API routes.
  """
  @spec not_found(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def not_found(conn, _params) do
    render_error(conn, 404, "Not Found", "NOT_FOUND")
  end

  # Private helper functions

  defp filter_killmails_by_time(killmails, since_hours) do
    cutoff_time = DateTime.utc_now() |> DateTime.add(-since_hours * 3600, :second)

    Enum.filter(killmails, fn killmail ->
      killmail
      |> get_killmail_time()
      |> should_include_killmail?(cutoff_time)
    end)
  end

  defp get_killmail_time(killmail) do
    get_in(killmail, ["kill_time"]) || get_in(killmail, ["killmail_time"])
  end

  defp should_include_killmail?(nil, _cutoff_time), do: true

  defp should_include_killmail?(time_str, cutoff_time) do
    case DateTime.from_iso8601(time_str) do
      {:ok, kill_time, _} -> DateTime.compare(kill_time, cutoff_time) != :lt
      _ -> true
    end
  end

  defp fetch_system_killmails_with_retry(system_id, since_hours) do
    opts = [past_seconds: since_hours * 3600]

    Utils.retry_http_operation(
      fn -> ZkbClient.fetch_system_killmails(system_id, opts) end,
      operation_name: "ZKB fetch system killmails #{system_id}"
    )
  end

  defp fetch_systems_killmails(system_ids, since_hours, limit) do
    tasks =
      Enum.map(system_ids, fn system_id ->
        Task.async(fn -> fetch_system_killmails_task(system_id, since_hours, limit) end)
      end)

    tasks
    |> Enum.map(&Task.await(&1, 30_000))
    |> Enum.into(%{})
  end

  defp fetch_system_killmails_task(system_id, since_hours, limit) do
    opts = [past_seconds: since_hours * 3600]

    case ZkbClient.fetch_system_killmails(system_id, opts) do
      {:ok, killmails} ->
        filtered =
          killmails
          |> filter_killmails_by_time(since_hours)
          |> Enum.take(limit)

        {system_id, filtered}

      {:error, _} ->
        {system_id, []}
    end
  end

  @spec build_cached_response(list(), boolean(), term()) :: map()
  defp build_cached_response(killmails, cached, error \\ nil) do
    base_response = %{
      kills: killmails,
      timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
      cached: cached
    }

    if error do
      Map.put(base_response, :error, error)
    else
      base_response
    end
  end
end
