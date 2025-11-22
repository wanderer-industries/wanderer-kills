defmodule WandererKills.Http.Client do
  @moduledoc """
  Consolidated HTTP client for all WandererKills HTTP operations.

  This module provides a single, clean interface for HTTP requests with:
  - Built-in timeouts and retries
  - Telemetry integration
  - Rate limiting support via SmartRateLimiter
  - ESI and ZKB specific helpers

  All HTTP requests in the application should go through this module.
  """

  @behaviour WandererKills.Http.ClientBehaviour

  require Logger

  alias WandererKills.Core.Support.Error
  alias WandererKills.Http.ConnectionMonitor
  alias WandererKills.Ingest.SmartRateLimiter

  # Configuration
  @default_timeout_ms 30_000
  @user_agent "(wanderer-kills@proton.me; +https://github.com/wanderer-industries/wanderer-kills)"

  # ESI specific timeouts
  @esi_timeout_ms Application.compile_env(:wanderer_kills, [:esi, :request_timeout_ms], 30_000)
  @zkb_timeout_ms Application.compile_env(:wanderer_kills, [:zkb, :request_timeout_ms], 15_000)

  # RedisQ long-polling requires longer timeout
  @redisq_timeout_ms Application.compile_env(
                       :wanderer_kills,
                       [:redisq, :request_timeout_ms],
                       45_000
                     )

  @type url :: String.t()
  @type headers :: [{String.t(), String.t()}]
  @type options :: keyword()
  @type response :: {:ok, map()} | {:error, term()}

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Performs a GET request with rate limiting.
  """
  @spec get(url, headers, options) :: response
  def get(url, headers \\ [], options \\ []) do
    # Check if we should use rate limiting
    if should_rate_limit?(url) and
         Application.get_env(:wanderer_kills, :features)[:smart_rate_limiting] do
      get_with_rate_limit(url, headers, options)
    else
      do_get(url, headers, options)
    end
  end

  @doc """
  Performs a GET request with explicit rate limiting.
  """
  @spec get_with_rate_limit(url, headers, options) :: response
  def get_with_rate_limit(url, headers \\ [], options \\ []) do
    if Application.get_env(:wanderer_kills, :features)[:smart_rate_limiting] do
      # Use the reservation system for rate limiting
      case SmartRateLimiter.reserve_token(url) do
        {:ok, reservation_id} ->
          # Perform the request
          result = do_get(url, headers, options)

          # Report the result to consume appropriate tokens
          status_code = extract_status_from_result(result)
          SmartRateLimiter.report_request_result(reservation_id, status_code, url)

          result

        {:error, error} ->
          {:error, error}
      end
    else
      # Rate limiting disabled, proceed directly
      do_get(url, headers, options)
    end
  end

  @doc """
  Performs a POST request.
  """
  @spec post(url, body :: term(), headers, options) :: response
  def post(url, body, headers \\ [], options \\ []) do
    # Add content-type header if not present
    headers = ensure_content_type(headers)
    do_post(url, body, headers, options)
  end

  @doc """
  GET request specifically for ESI endpoints with ESI-specific configuration.
  """
  @spec get_esi(url, headers, options) :: response
  def get_esi(url, headers \\ [], options \\ []) do
    options = Keyword.put_new(options, :timeout, @esi_timeout_ms)
    get_with_rate_limit(url, headers, options)
  end

  @doc """
  GET request specifically for zKillboard endpoints.
  """
  @spec get_zkb(url, headers, options) :: response
  def get_zkb(url, headers \\ [], options \\ []) do
    options = Keyword.put_new(options, :timeout, @zkb_timeout_ms)
    get_with_rate_limit(url, headers, options)
  end

  @doc """
  GET request specifically for RedisQ long-polling endpoints.
  Includes longer timeout and connection handling for long-polling.
  """
  @spec get_redisq(url, headers, options) :: response
  def get_redisq(url, headers \\ [], options \\ []) do
    options = Keyword.put_new(options, :timeout, @redisq_timeout_ms)
    do_get(url, headers, options)
  end

  # ============================================================================
  # Private Implementation
  # ============================================================================

  defp do_get(url, headers, options) do
    do_get_with_redirects(url, headers, options, 0)
  end

  defp do_get_with_redirects(_url, _headers, _options, redirect_count) when redirect_count > 5 do
    {:error, Error.http_error(:too_many_redirects, "Too many redirects (>5)", false)}
  end

  defp do_get_with_redirects(url, headers, options, redirect_count) do
    Logger.debug("[HTTP] GET #{url}")

    timeout = Keyword.get(options, :timeout, @default_timeout_ms)
    headers = build_headers(headers)

    # Build Finch request
    request = Finch.build(:get, url, headers)

    # Get the Finch instance to use
    finch_name = get_finch_name()

    # Perform the request
    start_time = System.monotonic_time(:millisecond)

    result =
      request
      |> do_finch_request(finch_name, receive_timeout: timeout)
      |> handle_finch_response(url, start_time, headers, options, redirect_count)

    # Emit telemetry
    emit_telemetry(url, start_time, result)

    # Report to connection monitor
    case result do
      {:ok, _} -> ConnectionMonitor.report_success(url)
      _ -> :ok
    end

    result
  end

  defp do_post(url, body, headers, options) do
    Logger.debug("[HTTP] POST #{url}")

    timeout = Keyword.get(options, :timeout, @default_timeout_ms)
    headers = build_headers(headers)

    # Encode body as JSON if it's not already a string
    encoded_body =
      case body do
        body when is_binary(body) -> body
        _ -> Jason.encode!(body)
      end

    # Build Finch request
    request = Finch.build(:post, url, headers, encoded_body)

    # Get the Finch instance to use
    finch_name = get_finch_name()

    # Perform the request
    start_time = System.monotonic_time(:millisecond)

    result =
      case do_finch_request(request, finch_name, receive_timeout: timeout) do
        {:ok, %Finch.Response{status: status, body: resp_body, headers: resp_headers}} ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          Logger.debug("[HTTP] Response #{status} in #{elapsed}ms")

          # Parse JSON if content-type indicates it
          parsed_body = maybe_parse_json(resp_body, resp_headers)

          handle_response(status, parsed_body, resp_headers)

        {:error, reason} ->
          Logger.error("[HTTP] POST request failed: #{inspect(reason)}")

          {:error,
           Error.http_error(:request_failed, "POST request failed: #{inspect(reason)}", false)}
      end

    # Emit telemetry
    emit_telemetry(url, start_time, result)

    # Report to connection monitor
    case result do
      {:ok, _} -> ConnectionMonitor.report_success(url)
      _ -> :ok
    end

    result
  end

  # ============================================================================
  # Helper Functions
  # ============================================================================

  defp get_finch_name do
    # Always use WandererKills.Finch
    WandererKills.Finch
  end

  defp should_rate_limit?(url) do
    # Rate limit external APIs (ESI, zKillboard)
    String.contains?(url, "esi.evetech.net") or
      String.contains?(url, "zkillboard.com") or
      String.contains?(url, "zkillredisq.stream")
  end

  defp build_headers(headers) do
    # Add default headers
    default_headers = [
      {"user-agent", @user_agent},
      {"accept", "application/json"}
    ]

    # Merge with provided headers (provided headers take precedence)
    Enum.uniq_by(headers ++ default_headers, fn {key, _} -> String.downcase(key) end)
  end

  defp resolve_redirect_url(original_url, location) do
    # If the location is already an absolute URL, return it as-is
    case URI.parse(location) do
      %URI{scheme: scheme} when not is_nil(scheme) ->
        # Already absolute URL
        location

      _ ->
        # Relative URL - resolve against original URL
        original_uri = URI.parse(original_url)

        # Parse the relative location
        location_uri = URI.parse(location)

        # Merge with original URL, preserving the original host/scheme
        merged = URI.merge(original_uri, location_uri)
        URI.to_string(merged)
    end
  end

  defp handle_finch_response(
         {:ok, %Finch.Response{status: status, body: body, headers: resp_headers}},
         url,
         start_time,
         headers,
         options,
         redirect_count
       ) do
    elapsed = System.monotonic_time(:millisecond) - start_time
    Logger.debug("[HTTP] Response #{status} in #{elapsed}ms")

    # Parse JSON if content-type indicates it
    parsed_body = maybe_parse_json(body, resp_headers)

    case handle_response(status, parsed_body, resp_headers) do
      {:redirect, location} ->
        # Resolve relative URLs against the current URL
        resolved_location = resolve_redirect_url(url, location)
        do_get_with_redirects(resolved_location, headers, options, redirect_count + 1)

      other ->
        other
    end
  end

  defp handle_finch_response(
         {:error, error},
         url,
         _start_time,
         _headers,
         _options,
         _redirect_count
       ) do
    handle_request_error(error, url)
  end

  defp handle_request_error(%{reason: :timeout}, url) do
    # Report timeout to connection monitor
    ConnectionMonitor.report_timeout(url)
    {:error, Error.http_error(:timeout, "Request to #{url} timed out", true)}
  end

  defp handle_request_error(%{reason: :econnrefused}, url) do
    # Report connection failure to connection monitor
    ConnectionMonitor.report_failure(url, :connection_refused)
    {:error, Error.http_error(:connection_failed, "Connection refused for #{url}", true)}
  end

  defp handle_request_error(%Mint.TransportError{reason: :closed}, _url) do
    Logger.warning("[HTTP] Connection closed - will retry")
    {:error, Error.http_error(:connection_closed, "Connection closed", true)}
  end

  defp handle_request_error(%Mint.TransportError{} = transport_error, _url) do
    Logger.warning("[HTTP] Transport error: #{inspect(transport_error)}")

    {:error,
     Error.http_error(
       :transport_error,
       "Transport error: #{inspect(transport_error)}",
       true
     )}
  end

  defp handle_request_error(reason, _url) do
    Logger.error("[HTTP] Request failed: #{inspect(reason)}")
    {:error, Error.http_error(:request_failed, "Request failed: #{inspect(reason)}", false)}
  end

  defp ensure_content_type(headers) do
    if Enum.any?(headers, fn {k, _} -> String.downcase(k) == "content-type" end) do
      headers
    else
      [{"content-type", "application/json"} | headers]
    end
  end

  defp maybe_parse_json(body, headers) do
    content_type =
      Enum.find_value(headers, fn
        {key, value} when is_binary(key) ->
          if String.downcase(key) == "content-type", do: value

        _ ->
          nil
      end)

    if content_type && String.contains?(content_type, "application/json") do
      case Jason.decode(body) do
        {:ok, parsed} -> parsed
        {:error, _} -> body
      end
    else
      body
    end
  end

  defp handle_response(status, body, headers) when status >= 200 and status < 300 do
    {:ok, %{status: status, body: body, headers: headers}}
  end

  defp handle_response(404, body, _headers) do
    {:error, Error.not_found_error("Resource not found", %{body: body})}
  end

  defp handle_response(429, body, headers) do
    # Extract retry-after header if present (prefer retry-after over x-esi-error-limit-reset)
    headers_map = Map.new(headers, fn {k, v} -> {String.downcase(k), v} end)

    retry_after_ms =
      cond do
        # First check for standard Retry-After header
        Map.has_key?(headers_map, "retry-after") ->
          parse_retry_after(headers_map["retry-after"])

        # Then check for ESI-specific reset header
        Map.has_key?(headers_map, "x-esi-error-limit-reset") ->
          parse_retry_after(headers_map["x-esi-error-limit-reset"])

        # Fall back to nil if neither header is present
        true ->
          nil
      end

    Logger.warning("[HTTP] Rate limited, retry_after: #{retry_after_ms}ms")

    {:error,
     Error.rate_limit_error("Rate limit exceeded", %{
       body: body,
       retry_after: headers_map["retry-after"] || headers_map["x-esi-error-limit-reset"],
       retry_after_ms: retry_after_ms
     })}
  end

  defp handle_response(status, body, _headers) when status >= 400 and status < 500 do
    {:error,
     Error.http_error(:client_error, "Client error: #{status}", false, %{
       status: status,
       body: body
     })}
  end

  defp handle_response(status, body, _headers) when status >= 500 do
    {:error,
     Error.http_error(:server_error, "Server error: #{status}", true, %{
       status: status,
       body: body
     })}
  end

  defp handle_response(status, _body, headers) when status in [301, 302, 303, 307, 308] do
    # Handle redirect by returning the Location header
    location =
      Enum.find_value(headers, fn
        {key, value} when is_binary(key) ->
          if String.downcase(key) == "location", do: value, else: nil

        _ ->
          nil
      end)

    if location do
      {:redirect, location}
    else
      {:error,
       Error.http_error(
         :redirect_no_location,
         "#{status} redirect without Location header",
         false
       )}
    end
  end

  defp handle_response(status, body, _headers) do
    {:error,
     Error.http_error(:unknown_status, "Unknown status: #{status}", false, %{
       status: status,
       body: body
     })}
  end

  defp emit_telemetry(url, start_time, result) do
    duration = System.monotonic_time(:millisecond) - start_time

    metadata = %{
      url: url,
      duration_ms: duration,
      status:
        case result do
          {:ok, %{status: status}} -> status
          _ -> nil
        end,
      error:
        case result do
          {:error, error} -> error
          _ -> nil
        end
    }

    :telemetry.execute(
      [:wanderer_kills, :http, :request],
      %{duration: duration},
      metadata
    )
  end

  defp extract_status_from_result({:ok, %{status: status}}), do: status
  defp extract_status_from_result({:error, %{details: %{status: status}}}), do: status
  defp extract_status_from_result({:error, %{meta: %{status: status}}}), do: status
  defp extract_status_from_result({:error, _}), do: 0
  defp extract_status_from_result(_), do: 0

  defp parse_retry_after(value) when is_binary(value) do
    # Try to parse as integer seconds
    case Integer.parse(value) do
      {seconds, _} ->
        seconds * 1000

      _ ->
        # Try to parse as HTTP date
        # For now, default to 5 seconds
        5000
    end
  end

  defp parse_retry_after(value) when is_integer(value), do: value * 1000
  defp parse_retry_after(_), do: 5000

  # Helper to make Finch requests
  defp do_finch_request(request, finch_name, options) do
    Finch.request(request, finch_name, options)
  end
end
