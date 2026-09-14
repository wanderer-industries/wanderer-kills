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

  # R2Z2 API timeout
  @r2z2_timeout_ms Application.compile_env(
                     :wanderer_kills,
                     [:r2z2, :request_timeout_ms],
                     15_000
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
      case SmartRateLimiter.reserve_token(url) do
        {:ok, reservation_id} ->
          result = do_get(url, headers, options)

          status_code = extract_status_from_result(result)
          SmartRateLimiter.report_request_result(reservation_id, status_code, url)

          result

        {:error, error} ->
          {:error, error}
      end
    else
      do_get(url, headers, options)
    end
  end

  @doc """
  Performs a POST request.
  """
  @spec post(url, body :: term(), headers, options) :: response
  def post(url, body, headers \\ [], options \\ []) do
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
  GET request specifically for R2Z2 API endpoints.
  """
  @spec get_r2z2(url, headers, options) :: response
  def get_r2z2(url, headers \\ [], options \\ []) do
    options = Keyword.put_new(options, :timeout, @r2z2_timeout_ms)
    get_with_rate_limit(url, headers, options)
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

    request = Finch.build(:get, url, headers)
    finch_name = get_finch_name()
    start_time = System.monotonic_time(:millisecond)

    result =
      request
      |> do_finch_request(finch_name, receive_timeout: timeout)
      |> handle_finch_response(url, start_time, headers, options, redirect_count)

    emit_telemetry(url, start_time, result)

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

    encoded_body =
      case body do
        body when is_binary(body) -> body
        _ -> Jason.encode!(body)
      end

    request = Finch.build(:post, url, headers, encoded_body)
    finch_name = get_finch_name()
    start_time = System.monotonic_time(:millisecond)

    result =
      case do_finch_request(request, finch_name, receive_timeout: timeout) do
        {:ok, %Finch.Response{status: status, body: resp_body, headers: resp_headers}} ->
          elapsed = System.monotonic_time(:millisecond) - start_time
          Logger.debug("[HTTP] Response #{status} in #{elapsed}ms")

          parsed_body = maybe_parse_json(resp_body, resp_headers)

          handle_response(status, parsed_body, resp_headers)

        {:error, reason} ->
          Logger.error("[HTTP] POST request failed: #{inspect(reason)}")

          {:error,
           Error.http_error(:request_failed, "POST request failed: #{inspect(reason)}", false)}
      end

    emit_telemetry(url, start_time, result)

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
    WandererKills.Finch
  end

  defp should_rate_limit?(url) do
    String.contains?(url, "esi.evetech.net") or
      String.contains?(url, "zkillboard.com")
  end

  defp build_headers(headers) do
    default_headers = [
      {"user-agent", @user_agent},
      {"accept", "application/json"}
    ]

    Enum.uniq_by(headers ++ default_headers, fn {key, _} -> String.downcase(key) end)
  end

  defp resolve_redirect_url(original_url, location) do
    case URI.parse(location) do
      %URI{scheme: scheme} when not is_nil(scheme) ->
        location

      _ ->
        original_uri = URI.parse(original_url)
        location_uri = URI.parse(location)
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

    parsed_body = maybe_parse_json(body, resp_headers)

    case handle_response(status, parsed_body, resp_headers) do
      {:redirect, location} ->
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
    ConnectionMonitor.report_timeout(url)
    {:error, Error.http_error(:timeout, "Request to #{url} timed out", true)}
  end

  defp handle_request_error(%{reason: :econnrefused}, url) do
    ConnectionMonitor.report_failure(url, :connection_refused)
    {:error, Error.http_error(:connection_failed, "Connection refused for #{url}", true)}
  end

  defp handle_request_error(%{__struct__: module, reason: :closed}, _url)
       when module in [Mint.TransportError, Finch.TransportError] do
    Logger.warning("[HTTP] Connection closed - will retry")
    {:error, Error.http_error(:connection_closed, "Connection closed", true)}
  end

  # Finch < 0.22 returns Mint errors and does not define Finch.TransportError.
  defp handle_request_error(%{__struct__: module} = transport_error, _url)
       when module in [Mint.TransportError, Finch.TransportError] do
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
    headers_map = Map.new(headers, fn {k, v} -> {String.downcase(k), v} end)

    retry_after_ms =
      cond do
        Map.has_key?(headers_map, "retry-after") ->
          parse_retry_after(headers_map["retry-after"])

        Map.has_key?(headers_map, "x-esi-error-limit-reset") ->
          parse_retry_after(headers_map["x-esi-error-limit-reset"])

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
  defp extract_status_from_result({:error, _}), do: 0

  defp parse_retry_after(value) when is_binary(value) do
    case Integer.parse(value) do
      {seconds, _} ->
        seconds * 1000

      _ ->
        5000
    end
  end

  defp parse_retry_after(value) when is_integer(value), do: value * 1000
  defp parse_retry_after(_), do: 5000

  defp do_finch_request(request, finch_name, options) do
    do_finch_request_with_retry(request, finch_name, options, 0)
  end

  defp do_finch_request_with_retry(request, finch_name, options, retry_count)
       when retry_count < 3 do
    case Finch.request(request, finch_name, options) do
      {:error, %{__struct__: module, reason: :closed}}
      when module in [Mint.TransportError, Finch.TransportError] and
             request.method == "GET" and retry_count < 2 ->
        delay_ms = 100 * (retry_count + 1)

        Logger.debug(
          "[HTTP] Connection closed, retrying in #{delay_ms}ms (attempt #{retry_count + 1}/2)"
        )

        Process.sleep(delay_ms)
        do_finch_request_with_retry(request, finch_name, options, retry_count + 1)

      result ->
        result
    end
  rescue
    error ->
      # Never replay POST: an exception does not prove that the peer missed the body.
      if request.method == "GET" and process_unavailable_error?(error) and retry_count < 2 do
        delay_ms = 100 * (retry_count + 1)

        Logger.warning(
          "[HTTP] Finch temporarily unavailable, retrying in #{delay_ms}ms (attempt #{retry_count + 1}/2)"
        )

        Process.sleep(delay_ms)
        do_finch_request_with_retry(request, finch_name, options, retry_count + 1)
      else
        reraise error, __STACKTRACE__
      end
  end

  defp do_finch_request_with_retry(_request, _finch_name, _options, _retry_count) do
    {:error, :max_retries_exceeded}
  end

  defp process_unavailable_error?(%{message: message}) when is_binary(message) do
    String.contains?(message, "no process") or
      String.contains?(message, "not alive") or
      String.contains?(message, "application isn't started")
  end

  defp process_unavailable_error?(_), do: false
end
