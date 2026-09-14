defmodule WandererKills.Ingest.Killmails.Pipeline.Validation do
  @moduledoc """
  Unified validation and parsing stage for killmail processing.

  This module consolidates the functionality of the old Parser and Validator
  modules into a single, more efficient stage that:

  - Validates killmail structure and required fields
  - Normalizes field names and data formats
  - Validates timestamps and applies cutoff filtering
  - Merges ESI and zKB data when needed
  - Provides clear error messages and logging

  ## New 3-Stage Pipeline

  1. **Validation** (this module) - Parse, validate, normalize
  2. **Enrichment** - Enrich with ESI data and ship info
  3. **Storage** - Store and broadcast
  """

  require Logger

  alias WandererKills.Core.Support.Error
  alias WandererKills.Ingest.Killmails.Pipeline.ESIFetcher
  alias WandererKills.Ingest.Killmails.{TimeFilters, Transformations}

  @type killmail :: map()
  @type validation_result :: {:ok, killmail()} | {:ok, :kill_older} | {:error, Error.t()}

  # Required fields for a valid killmail
  @required_fields ["killmail_id", "system_id", "victim", "attackers"]

  # ============================================================================
  # Public API - Main Processing Functions
  # ============================================================================

  @doc """
  Validates and processes a full killmail with all required data.

  ## Parameters
  - `killmail` - Complete killmail data (ESI + zKB)
  - `cutoff_time` - DateTime cutoff for filtering old killmails

  ## Returns
  - `{:ok, validated_killmail}` - On successful validation
  - `{:ok, :kill_older}` - When killmail is older than cutoff
  - `{:error, reason}` - On validation failure
  """
  @spec validate_full_killmail(killmail(), DateTime.t()) :: validation_result()
  def validate_full_killmail(killmail, cutoff_time) when is_map(killmail) do
    killmail_id = Transformations.get_killmail_id(killmail)

    Logger.debug("Validating full killmail",
      killmail_id: killmail_id,
      has_victim: Map.has_key?(killmail, "victim"),
      has_attackers: Map.has_key?(killmail, "attackers")
    )

    killmail
    |> normalize_killmail_data()
    |> validate_structure()
    |> validate_time_and_cutoff(cutoff_time)
    |> build_final_structure()
    |> handle_validation_result()
  end

  @doc """
  Validates and processes a partial killmail, fetching full data from ESI.

  ## Parameters
  - `partial` - Partial killmail with killmail_id and zkb data
  - `cutoff_time` - DateTime cutoff for filtering old killmails

  ## Returns
  - `{:ok, validated_killmail}` - On successful validation
  - `{:ok, :kill_older}` - When killmail is older than cutoff
  - `{:error, reason}` - On validation failure
  """
  @spec validate_partial_killmail(killmail(), DateTime.t()) :: validation_result()
  def validate_partial_killmail(partial, cutoff_time) do
    case extract_partial_info(partial) do
      {:ok, {killmail_id, zkb_data}} ->
        Logger.debug("Validating partial killmail", killmail_id: killmail_id)

        with {:ok, esi_data} <- fetch_full_data(killmail_id, zkb_data),
             {:ok, merged} <- merge_killmail_data(esi_data, partial) do
          validate_full_killmail(merged, cutoff_time)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Merges ESI killmail data with zKB metadata.

  ## Parameters
  - `esi_data` - Full killmail data from ESI
  - `zkb_data` - zKB metadata (can be partial killmail or just zkb section)

  ## Returns
  - `{:ok, merged_killmail}` - On successful merge
  - `{:error, reason}` - On merge failure
  """
  @spec merge_killmail_data(map(), map()) :: {:ok, map()} | {:error, Error.t()}
  def merge_killmail_data(esi_data, zkb_data) when is_map(esi_data) and is_map(zkb_data) do
    zkb_section = extract_zkb_section(zkb_data)

    case validate_merge_data(esi_data, zkb_section) do
      :ok ->
        merged_data =
          esi_data
          |> Transformations.normalize_field_names()
          |> Map.put("zkb", zkb_section)

        case ensure_kill_time(merged_data) do
          {:ok, merged} ->
            {:ok, merged}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ============================================================================
  # Private Helper Functions - Validation Pipeline
  # ============================================================================

  # Step 1: Normalize field names and basic structure
  defp normalize_killmail_data(killmail) do
    normalized = Transformations.normalize_field_names(killmail)
    {:continue, normalized}
  end

  # Step 2: Validate required structure
  defp validate_structure({:continue, killmail}) do
    case check_required_fields(killmail) do
      :ok ->
        case validate_field_types(killmail) do
          :ok -> {:continue, killmail}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Step 3: Validate time and apply cutoff
  defp validate_time_and_cutoff({:continue, killmail}, cutoff_time) do
    case TimeFilters.validate_cutoff_time(killmail, cutoff_time) do
      :ok ->
        case parse_and_validate_time(killmail) do
          {:ok, time_validated} -> {:continue, time_validated}
          {:error, reason} -> {:error, reason}
        end

      {:error, %Error{type: :kill_too_old}} ->
        {:kill_older, killmail}

      {:error, _} = error ->
        error
    end
  end

  defp validate_time_and_cutoff({:error, _} = error, _), do: error

  # Step 4: Build final validated structure
  defp build_final_structure({:continue, killmail}) do
    structured = %{
      "killmail_id" => killmail["killmail_id"],
      "kill_time" => killmail["kill_time"],
      "system_id" => killmail["system_id"],
      "victim" => Transformations.normalize_victim(killmail["victim"]),
      "attackers" => Transformations.normalize_attackers(killmail["attackers"]),
      "zkb" => killmail["zkb"] || %{},
      # Pre-compute commonly used values
      "total_value" => get_in(killmail, ["zkb", "totalValue"]) || 0,
      "npc" => get_in(killmail, ["zkb", "npc"]) || false
    }

    {:ok, structured}
  rescue
    error ->
      Logger.error("Failed to build killmail structure",
        error: inspect(error),
        killmail_id: killmail["killmail_id"]
      )

      {:error, Error.killmail_error(:build_failed, "Failed to build killmail structure")}
  end

  defp build_final_structure({:kill_older, _}), do: {:kill_older}
  defp build_final_structure({:error, _} = error), do: error

  # Step 5: Handle final result
  defp handle_validation_result({:ok, killmail}), do: {:ok, killmail}
  defp handle_validation_result({:kill_older}), do: {:ok, :kill_older}
  defp handle_validation_result({:error, reason}), do: {:error, reason}

  # ============================================================================
  # Private Helper Functions - Field Validation
  # ============================================================================

  defp check_required_fields(killmail) do
    missing_fields = Enum.reject(@required_fields, &Map.has_key?(killmail, &1))

    case missing_fields do
      [] ->
        :ok

      fields ->
        {:error,
         Error.validation_error(
           :missing_fields,
           "Missing required fields: #{Enum.join(fields, ", ")}"
         )}
    end
  end

  defp validate_field_types(killmail) do
    validations = [
      validate_killmail_id(killmail["killmail_id"]),
      validate_system_id(killmail["system_id"]),
      validate_victim(killmail["victim"]),
      validate_attackers(killmail["attackers"])
    ]

    case Enum.find(validations, fn
           {:error, _} -> true
           :ok -> false
           _ -> false
         end) do
      nil -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_killmail_id(id) when is_integer(id) and id > 0, do: :ok

  defp validate_killmail_id(_),
    do: {:error, Error.validation_error(:invalid_killmail_id, "Invalid killmail ID")}

  defp validate_system_id(id) when is_integer(id) and id > 0, do: :ok

  defp validate_system_id(_),
    do: {:error, Error.validation_error(:invalid_system_id, "Invalid system ID")}

  defp validate_victim(victim) when is_map(victim) do
    if Map.has_key?(victim, "character_id") or Map.has_key?(victim, "ship_type_id") do
      :ok
    else
      {:error,
       Error.validation_error(:invalid_victim, "Victim must have character_id or ship_type_id")}
    end
  end

  defp validate_victim(_),
    do: {:error, Error.validation_error(:invalid_victim, "Victim must be a map")}

  defp validate_attackers(attackers) do
    if attackers != [] and proper_list?(attackers) do
      :ok
    else
      {:error, Error.validation_error(:invalid_attackers, "Attackers must be a non-empty list")}
    end
  end

  # is_list/1 also accepts improper lists, which cannot be normalized downstream.
  defp proper_list?([]), do: true
  defp proper_list?([_ | tail]), do: proper_list?(tail)
  defp proper_list?(_), do: false

  # ============================================================================
  # Private Helper Functions - Time Handling
  # ============================================================================

  defp parse_and_validate_time(killmail) do
    killmail_time = killmail["kill_time"] || killmail["killmail_time"]

    case killmail_time do
      time when is_binary(time) ->
        case DateTime.from_iso8601(time) do
          {:ok, _parsed_time, _} ->
            {:ok, Map.put(killmail, "killmail_time", time)}

          {:error, _} ->
            {:error, Error.validation_error(:invalid_time_format, "Invalid time format")}
        end

      nil ->
        {:error, Error.validation_error(:missing_time, "Killmail time is required")}

      _ ->
        {:error, Error.validation_error(:invalid_time_type, "Killmail time must be a string")}
    end
  end

  # ============================================================================
  # Private Helper Functions - Partial Killmail Processing
  # ============================================================================

  defp extract_partial_info(%{"killmail_id" => id, "zkb" => zkb})
       when is_integer(id) and is_map(zkb) do
    {:ok, {id, zkb}}
  end

  defp extract_partial_info(_) do
    {:error,
     Error.killmail_error(
       :invalid_partial_format,
       "Partial killmail must have killmail_id and zkb fields"
     )}
  end

  defp fetch_full_data(killmail_id, zkb_data) do
    case Map.get(zkb_data, "hash") do
      hash when is_binary(hash) ->
        ESIFetcher.fetch_full_killmail(killmail_id, zkb_data)

      _ ->
        {:error, Error.killmail_error(:missing_hash, "zKB data missing hash for ESI fetch")}
    end
  end

  defp extract_zkb_section(%{"zkb" => zkb}), do: zkb
  defp extract_zkb_section(zkb) when is_map(zkb), do: zkb

  defp validate_merge_data(%{"killmail_id" => id}, zkb) when is_integer(id) and is_map(zkb) do
    :ok
  end

  defp validate_merge_data(_, _) do
    {:error, Error.killmail_error(:invalid_merge_data, "Invalid data for merge operation")}
  end

  defp ensure_kill_time(killmail) do
    case killmail["kill_time"] || killmail["killmail_time"] do
      nil ->
        {:error, Error.killmail_error(:missing_kill_time, "Kill time not found after merge")}

      time ->
        updated_killmail =
          killmail
          |> Map.put("killmail_time", time)
          # Remove old kill_time key
          |> Map.delete("kill_time")

        {:ok, updated_killmail}
    end
  end
end
