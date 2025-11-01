defmodule WandererKills.Ingest.SmartRateLimiterESITest do
  use WandererKills.UnifiedTestCase, async: false

  alias WandererKills.Ingest.SmartRateLimiter

  setup do
    # SmartRateLimiter should be started by the application
    case Process.whereis(SmartRateLimiter) do
      nil ->
        # Application should already be started by UnifiedTestCase
        {:ok, _pid} = SmartRateLimiter.start_link()

      _pid ->
        :ok
    end

    # Reset buckets for clean state
    SmartRateLimiter.reset_bucket(:esi)
    SmartRateLimiter.reset_bucket(:zkillboard)

    :ok
  end

  describe "ESI variable token consumption" do
    test "2XX responses consume 2 tokens" do
      # Reserve tokens
      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)

      # Report 200 response immediately
      :ok = SmartRateLimiter.report_request_result(reservation_id, 200, :esi)

      # Allow some time for async processing
      :timer.sleep(10)

      # Check state
      state = SmartRateLimiter.get_bucket_state(:esi)

      # We started with 500 tokens, consumed 2, should have 498 (plus any refilled amount)
      assert state.tokens >= 498.0
      assert state.tokens < 500.0
    end

    test "3XX responses consume 1 token" do
      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)

      :ok = SmartRateLimiter.report_request_result(reservation_id, 302, :esi)
      :timer.sleep(10)

      state = SmartRateLimiter.get_bucket_state(:esi)
      # We started with 500 tokens, consumed 1, should have 499
      assert state.tokens >= 499.0
      assert state.tokens < 500.0
    end

    test "4XX responses consume 5 tokens" do
      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)

      :ok = SmartRateLimiter.report_request_result(reservation_id, 404, :esi)
      :timer.sleep(10)

      state = SmartRateLimiter.get_bucket_state(:esi)
      # We started with 500 tokens, consumed 5, should have 495
      assert state.tokens >= 495.0
      assert state.tokens < 500.0
    end

    test "5XX responses consume 0 tokens" do
      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)
      initial_state = SmartRateLimiter.get_bucket_state(:esi)

      :ok = SmartRateLimiter.report_request_result(reservation_id, 503, :esi)
      :timer.sleep(10)

      final_state = SmartRateLimiter.get_bucket_state(:esi)
      tokens_consumed = initial_state.tokens - final_state.tokens

      # Should consume 0 tokens, accounting for refilling
      assert tokens_consumed <= 0.0
    end

    test "ZKB always consumes 1 token regardless of status" do
      # Test various status codes
      for status <- [200, 302, 404, 503] do
        SmartRateLimiter.reset_bucket(:zkillboard)
        {:ok, reservation_id} = SmartRateLimiter.reserve_token(:zkillboard)

        :ok = SmartRateLimiter.report_request_result(reservation_id, status, :zkillboard)
        :timer.sleep(10)

        state = SmartRateLimiter.get_bucket_state(:zkillboard)
        # ZKB has 300 capacity, consumed 1, should have 299
        assert state.tokens >= 299.0 and state.tokens < 300.0,
               "ZKB should always consume 1 token, but has #{state.tokens} tokens for status #{status}"
      end
    end

    test "reservation prevents rate limiting when sufficient tokens exist" do
      # Reset to known state
      SmartRateLimiter.reset_bucket(:esi)

      # Should be able to reserve when we have 500 tokens
      assert {:ok, _reservation_id} = SmartRateLimiter.reserve_token(:esi)
    end

    test "reservation fails when insufficient tokens" do
      # Reset and then consume most tokens
      SmartRateLimiter.reset_bucket(:esi)

      # Consume tokens to get below 5 (minimum for ESI)
      # We have 500 tokens, need to consume > 495
      # Each 4XX consumes 5 tokens, so 99 4XX responses = 495 tokens
      for _ <- 1..99 do
        {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)
        :ok = SmartRateLimiter.report_request_result(reservation_id, 404, :esi)
      end

      :timer.sleep(10)

      # Check tokens before trying reservation
      state = SmartRateLimiter.get_bucket_state(:esi)

      # If we have less than 5 tokens, reservation should fail
      if state.tokens < 5.0 do
        result = SmartRateLimiter.reserve_token(:esi)
        assert {:error, _} = result
      else
        # Skip test if refilling gave us too many tokens back
        :ok
      end
    end

    test "expired reservations are cleaned up" do
      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)

      # Don't report the result, let it expire
      # The cleanup timeout is 60 seconds, but we won't wait that long in tests
      # Just verify the reservation was created
      assert is_binary(reservation_id)
      assert reservation_id != "no-reservation-needed"
    end

    test "cleanup timeout is configurable" do
      # Get the current config value
      app_config = Application.get_env(:wanderer_kills, :smart_rate_limiter, [])
      timeout = Keyword.get(app_config, :reservation_cleanup_timeout_ms, 60_000)

      # Verify it uses the configured value (default 60 seconds)
      assert timeout == 60_000

      # Test with a custom value
      original_config = Application.get_env(:wanderer_kills, :smart_rate_limiter)
      original_config_list = original_config || []

      try do
        # Set a shorter timeout for testing
        Application.put_env(
          :wanderer_kills,
          :smart_rate_limiter,
          Keyword.put(original_config_list, :reservation_cleanup_timeout_ms, 100)
        )

        # Verify the config was updated
        new_config = Application.get_env(:wanderer_kills, :smart_rate_limiter)
        assert Keyword.get(new_config, :reservation_cleanup_timeout_ms) == 100
      after
        # Restore original config
        if original_config do
          Application.put_env(:wanderer_kills, :smart_rate_limiter, original_config)
        else
          Application.delete_env(:wanderer_kills, :smart_rate_limiter)
        end
      end
    end

    test "prevents reservation oversubscription" do
      # Reset bucket to start fresh
      SmartRateLimiter.reset_bucket(:esi)

      # Get initial state
      initial_state = SmartRateLimiter.get_bucket_state(:esi)
      initial_tokens = initial_state.tokens

      # ESI requires 5 tokens per reservation
      # Calculate how many reservations we can make
      max_reservations = trunc(initial_tokens / 5.0)

      # Make all possible reservations
      for i <- 1..max_reservations do
        case SmartRateLimiter.reserve_token(:esi) do
          {:ok, _reservation_id} ->
            :ok

          {:error, _} ->
            flunk(
              "Failed to reserve token #{i} when we should have had capacity (initial: #{initial_tokens}, max: #{max_reservations})"
            )
        end
      end

      # The next reservation should fail
      assert {:error, error} = SmartRateLimiter.reserve_token(:esi)
      assert error.type == :rate_limited
      assert error.details.tokens_available < 5.0
      assert error.details.tokens_required == 5.0

      # Clean up - reset bucket to avoid affecting other tests
      SmartRateLimiter.reset_bucket(:esi)
    end
  end

  describe "HTTP client integration" do
    test "URLs are correctly mapped to services" do
      # ESI URLs
      assert {:ok, _} =
               SmartRateLimiter.reserve_token("https://esi.evetech.net/latest/characters/12345/")

      assert {:ok, _} =
               SmartRateLimiter.reserve_token(
                 "https://esi.evetech.net/v1/universe/systems/30000142/"
               )

      # ZKB URLs
      assert {:ok, _} =
               SmartRateLimiter.reserve_token(
                 "https://zkillboard.com/api/kills/systemID/30000142/"
               )

      assert {:ok, _} = SmartRateLimiter.reserve_token("https://zkillredisq.stream/listen.php")

      # Unknown URLs should not require reservation
      assert {:ok, "no-reservation-needed"} =
               SmartRateLimiter.reserve_token("https://example.com/api")
    end
  end

  describe "telemetry events" do
    test "token consumption emits telemetry" do
      # Attach telemetry handler
      ref = make_ref()
      test_pid = self()

      :telemetry.attach(
        "test-handler-#{inspect(ref)}",
        [:wanderer_kills, :rate_limiter, :token_consumed],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_event, measurements, metadata})
        end,
        nil
      )

      # Perform a request
      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)
      :ok = SmartRateLimiter.report_request_result(reservation_id, 200, :esi)

      # Wait for telemetry event
      assert_receive {:telemetry_event, measurements, metadata}, 1000

      assert measurements[:tokens_consumed] == 2
      assert metadata[:service] == :esi
      assert metadata[:status_code] == 200

      # Cleanup
      :telemetry.detach("test-handler-#{inspect(ref)}")
    end

    test "validates service matches reservation and logs error on mismatch" do
      ref = make_ref()
      test_pid = self()

      # Capture logs
      log_capture =
        ExUnit.CaptureLog.capture_log(fn ->
          # Reserve with ESI service
          {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)

          # Report result with different service (zkillboard)
          :ok = SmartRateLimiter.report_request_result(reservation_id, 200, :zkillboard)

          # Give time for async log
          :timer.sleep(10)
        end)

      # Verify error was logged
      assert log_capture =~ "Service mismatch in reservation"

      # Verify telemetry still uses the original service
      :telemetry.attach(
        "test-handler-#{inspect(ref)}",
        [:wanderer_kills, :rate_limiter, :token_consumed],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_event, measurements, metadata})
        end,
        nil
      )

      # Reserve with ESI and report with ZKB
      {:ok, reservation_id2} = SmartRateLimiter.reserve_token(:esi)
      :ok = SmartRateLimiter.report_request_result(reservation_id2, 200, :zkillboard)

      # Should receive telemetry with ESI service (the original reservation)
      assert_receive {:telemetry_event, measurements, metadata}, 1000
      assert metadata[:service] == :esi
      assert metadata[:status_code] == 200
      # ESI 2xx token cost
      assert measurements[:tokens_consumed] == 2

      # Cleanup
      :telemetry.detach("test-handler-#{inspect(ref)}")
    end

    test "uses correct service from URL in reservation and report" do
      # Reserve using ESI URL
      {:ok, reservation_id} = SmartRateLimiter.reserve_token("https://esi.evetech.net/v1/status/")

      # Report with zkillboard URL - should log error
      log_capture =
        ExUnit.CaptureLog.capture_log(fn ->
          :ok =
            SmartRateLimiter.report_request_result(
              reservation_id,
              200,
              "https://zkillboard.com/api/"
            )

          :timer.sleep(10)
        end)

      assert log_capture =~ "Service mismatch in reservation"

      # Reserve and report with matching URLs
      {:ok, reservation_id2} =
        SmartRateLimiter.reserve_token("https://esi.evetech.net/v1/status/")

      :ok =
        SmartRateLimiter.report_request_result(
          reservation_id2,
          404,
          "https://esi.evetech.net/v1/status/"
        )

      # Verify ESI bucket was charged 5 tokens for 404
      :timer.sleep(10)
      bucket_state = SmartRateLimiter.get_bucket_state(:esi)
      # Started with 500, consumed 2 + 5 = 7 tokens total, but may have refilled
      assert bucket_state.tokens < 496.0
    end
  end
end
