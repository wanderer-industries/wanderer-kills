defmodule WandererKills.Ingest.Killmails.UnifiedProcessor do
  @moduledoc """
  Simplified unified killmail processor using the new 3-stage pipeline.

  This module provides a clean, simplified interface for processing killmails
  using the new streamlined pipeline:

  1. **Validation** - Parse, validate, and normalize killmail data
  2. **Enrichment** - Enrich with ESI data and ship/system information  
  3. **Storage** - Store and broadcast killmail updates

  ## Simplifications from Previous Version

  - Removed Coordinator pattern complexity
  - Consolidated 6 pipeline stages into 3
  - Simplified error handling with better fallbacks
  - Reduced intermediate transformations
  - More efficient batch processing
  """

  require Logger

  alias WandererKills.Core.Observability.Monitoring
  alias WandererKills.Core.Storage.KillmailStore
  alias WandererKills.Domain.Killmail
  alias WandererKills.Ingest.Killmails.Pipeline.{Enrichment, Validation}
  alias WandererKills.Ingest.Killmails.Transformations

  @type process_options :: [
          store: boolean(),
          enrich: boolean(),
          validate_only: boolean()
        ]
  @type process_result :: {:ok, Killmail.t()} | {:ok, :kill_older} | {:error, term()}

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Processes any killmail using the simplified 3-stage pipeline.

  ## New Pipeline Flow

  1. **Auto-detection**: Determines if killmail is full or partial
  2. **Validation**: Validates structure, normalizes data, applies cutoff
  3. **Enrichment**: Enriches with ESI data (optional)
  4. **Storage**: Stores and broadcasts (optional)

  ## Parameters
  - `killmail` - The killmail data (full or partial)
  - `cutoff_time` - DateTime cutoff for filtering old killmails
  - `opts` - Processing options:
    - `:store` - Whether to store the killmail (default: true)
    - `:enrich` - Whether to enrich with ESI data (default: true)
    - `:validate_only` - Only validate, don't process (default: false)

  ## Returns
  - `{:ok, processed_killmail}` - Killmail struct on successful processing
  - `{:ok, :kill_older}` - When killmail is older than cutoff
  - `{:error, reason}` - On failure
  """
  @spec process_killmail(map(), DateTime.t(), process_options()) :: process_result()
  def process_killmail(killmail, cutoff_time, opts \\ []) when is_map(killmail) do
    start_time = System.monotonic_time(:millisecond)
    killmail_id = Transformations.get_killmail_id(killmail)

    Logger.debug("Processing killmail with simplified pipeline",
      killmail_id: killmail_id,
      is_partial: partial_killmail?(killmail),
      opts: opts
    )

    result =
      killmail
      |> run_pipeline(cutoff_time, opts)
      |> monitor_processing_result()

    duration = System.monotonic_time(:millisecond) - start_time
    log_processing_result(result, killmail_id, duration)

    result
  end

  @doc """
  Processes a batch of killmails efficiently using the simplified pipeline.

  ## Batch Processing Optimizations

  - Validates all killmails first (fail fast)
  - Batch enrichment to reduce API calls
  - Parallel storage operations
  - Shared error handling and monitoring

  ## Parameters
  - `killmails` - List of killmail data
  - `cutoff_time` - DateTime cutoff for filtering
  - `opts` - Processing options

  ## Returns
  - `{:ok, processed_killmails}` - List of successfully processed killmails
  """
  @spec process_batch([map()], DateTime.t(), process_options()) :: {:ok, [Killmail.t()]}
  def process_batch(killmails, cutoff_time, opts \\ []) when is_list(killmails) do
    start_time = System.monotonic_time(:millisecond)
    batch_size = length(killmails)

    Logger.info("Processing killmail batch",
      batch_size: batch_size,
      opts: opts
    )

    # Stage 1: Validate all killmails (fail fast)
    validated_results =
      killmails
      |> Task.async_stream(
        &validate_killmail(&1, cutoff_time),
        max_concurrency: 10,
        timeout: 10_000
      )
      |> collect_successful_validations()

    success_count = length(validated_results)

    Logger.debug("Batch validation completed",
      success_count: success_count,
      failure_count: batch_size - success_count
    )

    # Early exit if only validation requested
    if Keyword.get(opts, :validate_only, false) do
      structs = Enum.map(validated_results, &convert_to_struct/1)
      duration = System.monotonic_time(:millisecond) - start_time

      Logger.info("Batch validation completed",
        batch_size: batch_size,
        success_count: success_count,
        duration_ms: duration
      )

      {:ok, structs}
    else
      process_validated_batch(validated_results, opts, start_time)
    end
  end

  # ============================================================================
  # Private Pipeline Functions
  # ============================================================================

  # Main pipeline orchestration
  defp run_pipeline(killmail, cutoff_time, opts) do
    case validate_killmail(killmail, cutoff_time) do
      {:ok, :kill_older} ->
        {:ok, :kill_older}

      {:ok, validated} ->
        process_validated_killmail(validated, opts)

      {:error, _} = error ->
        error
    end
  end

  defp process_validated_killmail(validated, opts) do
    # Early exit if only validation requested
    if Keyword.get(opts, :validate_only, false) do
      {:ok, convert_to_struct(validated)}
    else
      with {:ok, enriched} <- maybe_enrich_killmail(validated, opts),
           :ok <- maybe_store_killmail(enriched, opts) do
        {:ok, convert_to_struct(enriched)}
      end
    end
  end

  # Stage 1: Validation (handles both full and partial killmails)
  defp validate_killmail(killmail, cutoff_time) do
    if partial_killmail?(killmail) do
      Validation.validate_partial_killmail(killmail, cutoff_time)
    else
      Validation.validate_full_killmail(killmail, cutoff_time)
    end
  end

  # Stage 2: Enrichment (optional)
  defp maybe_enrich_killmail(killmail, opts) do
    if Keyword.get(opts, :enrich, true) do
      # Enrichment always returns {:ok, _} now
      Enrichment.enrich_killmail(killmail)
    else
      {:ok, killmail}
    end
  end

  # Stage 3: Storage (optional)
  defp maybe_store_killmail(killmail, opts) do
    if Keyword.get(opts, :store, true) do
      store_killmail_async(killmail)
    else
      :ok
    end
  end

  # ============================================================================
  # Batch Processing Functions
  # ============================================================================

  defp collect_successful_validations(stream) do
    stream
    |> Enum.reduce([], fn
      # Skip old killmails
      {:ok, {:ok, :kill_older}}, acc -> acc
      {:ok, {:ok, validated}}, acc -> [validated | acc]
      # Skip failed validations
      {:ok, {:error, _reason}}, acc -> acc
      # Skip timed out validations
      {:exit, _reason}, acc -> acc
    end)
    |> Enum.reverse()
  end

  defp process_validated_batch(validated_killmails, opts, start_time) do
    batch_size = length(validated_killmails)

    # Stage 2: Batch enrichment
    enriched_killmails =
      if Keyword.get(opts, :enrich, true) do
        enrich_killmails_batch(validated_killmails)
      else
        validated_killmails
      end

    # Stage 3: Batch storage
    if Keyword.get(opts, :store, true) do
      store_killmails_batch(enriched_killmails)
    end

    # Convert to structs
    result_structs = Enum.map(enriched_killmails, &convert_to_struct/1)

    duration = System.monotonic_time(:millisecond) - start_time

    Logger.info("Batch processing completed",
      batch_size: batch_size,
      success_count: length(result_structs),
      duration_ms: duration
    )

    {:ok, result_structs}
  end

  defp enrich_killmails_batch(killmails) do
    killmails
    |> Task.async_stream(
      &enrich_single_killmail/1,
      # Lower concurrency for API-heavy enrichment
      max_concurrency: 5,
      timeout: 30_000
    )
    |> Enum.map(fn
      {:ok, enriched} ->
        enriched

      {:exit, _reason} ->
        Logger.warning("Killmail enrichment timed out")
        # Return empty for failed enrichment
        %{}
    end)
    # Remove empty results
    |> Enum.reject(&(map_size(&1) == 0))
  end

  defp enrich_single_killmail(killmail) do
    {:ok, enriched} = Enrichment.enrich_killmail(killmail)
    enriched
  end

  defp store_killmails_batch(killmails) do
    killmails
    |> Task.async_stream(
      &store_killmail_sync/1,
      max_concurrency: 10,
      timeout: 5_000
    )
    # Execute but don't collect results
    |> Stream.run()
  end

  # ============================================================================
  # Utility Functions
  # ============================================================================

  defp partial_killmail?(%{"zkb" => _zkb} = killmail) do
    not (Map.has_key?(killmail, "victim") and Map.has_key?(killmail, "attackers"))
  end

  defp partial_killmail?(_), do: false

  defp store_killmail_async(killmail) do
    case extract_system_id(killmail) do
      {:ok, _system_id} ->
        Task.Supervisor.start_child(WandererKills.TaskSupervisor, fn ->
          store_killmail_sync(killmail)
        end)

        :ok

      {:error, reason} ->
        Logger.error("Cannot store killmail without system_id",
          killmail_id: killmail["killmail_id"],
          error: reason
        )

        # Don't fail the pipeline for storage issues
        :ok
    end
  end

  defp store_killmail_sync(killmail) do
    case extract_system_id(killmail) do
      {:ok, system_id} ->
        Logger.debug("Storing killmail",
          killmail_id: killmail["killmail_id"],
          system_id: system_id
        )

        KillmailStore.put(killmail["killmail_id"], system_id, killmail)

      {:error, reason} ->
        Logger.error("Cannot store killmail without system_id",
          killmail_id: killmail["killmail_id"],
          error: reason
        )
    end
  end

  defp extract_system_id(%Killmail{system_id: id}) when not is_nil(id), do: {:ok, id}
  defp extract_system_id(%{"solar_system_id" => id}) when not is_nil(id), do: {:ok, id}
  defp extract_system_id(%{"system_id" => id}) when not is_nil(id), do: {:ok, id}

  defp extract_system_id(killmail) do
    killmail_id = killmail["killmail_id"] || "unknown"
    Logger.warning("Killmail missing system_id", killmail_id: killmail_id)
    {:error, :missing_system_id}
  end

  defp convert_to_struct(killmail) when is_map(killmail) do
    case Killmail.new(killmail) do
      {:ok, struct} ->
        struct

      {:error, reason} ->
        Logger.error("Failed to convert killmail to struct",
          killmail_id: killmail["killmail_id"],
          error: reason
        )

        raise "Failed to convert killmail #{killmail["killmail_id"]} to struct: #{inspect(reason)}"
    end
  end

  # ============================================================================
  # Monitoring and Logging
  # ============================================================================

  defp monitor_processing_result({:ok, :kill_older} = result) do
    Monitoring.increment_skipped()
    result
  end

  defp monitor_processing_result({:ok, _killmail} = result) do
    Monitoring.increment_stored()
    result
  end

  defp monitor_processing_result({:error, _reason} = result) do
    # Error counting is handled in log_processing_result
    result
  end

  defp log_processing_result({:ok, :kill_older}, killmail_id, duration) do
    Logger.debug("Killmail skipped (too old)",
      killmail_id: killmail_id,
      duration_ms: duration
    )
  end

  defp log_processing_result({:ok, _killmail}, killmail_id, duration) do
    Logger.debug("Killmail processed successfully",
      killmail_id: killmail_id,
      duration_ms: duration
    )
  end

  defp log_processing_result({:error, reason}, killmail_id, duration) do
    Logger.error("Killmail processing failed",
      killmail_id: killmail_id,
      duration_ms: duration,
      error: reason
    )
  end
end
