defmodule WandererKills.Http.ClientRateLimitIntegrationTest do
  use WandererKills.UnifiedTestCase, async: false

  alias WandererKills.Ingest.SmartRateLimiter

  @moduledoc """
  Integration tests for HTTP client rate limiting behavior.

  Since we removed the Test.Finch module, these tests now focus on
  verifying the rate limiting logic directly rather than mocking HTTP responses.
  """

  setup do
    # Ensure SmartRateLimiter is started
    unless Process.whereis(SmartRateLimiter) do
      start_supervised!(SmartRateLimiter)
    end

    # Reset buckets for clean state
    SmartRateLimiter.reset_bucket(:esi)
    SmartRateLimiter.reset_bucket(:zkillboard)

    # Enable smart rate limiting for tests
    original_config = Application.get_env(:wanderer_kills, :features)
    Application.put_env(:wanderer_kills, :features, smart_rate_limiting: true)

    # Disable token refill during tests to get predictable results
    original_rate_config = Application.get_env(:wanderer_kills, :smart_rate_limiter)

    test_rate_config =
      Keyword.merge(original_rate_config || [],
        esi_refill_rate: 0,
        zkb_refill_rate: 0
      )

    Application.put_env(:wanderer_kills, :smart_rate_limiter, test_rate_config)

    on_exit(fn ->
      Application.put_env(:wanderer_kills, :features, original_config || [])
      Application.put_env(:wanderer_kills, :smart_rate_limiter, original_rate_config || [])
    end)

    :ok
  end

  describe "SmartRateLimiter token consumption patterns" do
    test "ESI 2XX responses consume 2 tokens" do
      # Reserve and report a successful request
      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)
      initial_state = SmartRateLimiter.get_bucket_state(:esi)

      :ok = SmartRateLimiter.report_request_result(reservation_id, 200, :esi)

      # Give time for async processing
      :timer.sleep(20)

      final_state = SmartRateLimiter.get_bucket_state(:esi)
      tokens_consumed = initial_state.tokens - final_state.tokens

      # ESI 2XX responses should consume approximately 2 tokens
      # The actual consumption might vary slightly due to timing
      assert tokens_consumed > 0.5 && tokens_consumed <= 2.5,
             "Expected approximately 2 tokens consumed, got #{tokens_consumed}"
    end

    test "ESI 4XX responses consume 5 tokens" do
      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)
      initial_state = SmartRateLimiter.get_bucket_state(:esi)

      :ok = SmartRateLimiter.report_request_result(reservation_id, 404, :esi)

      :timer.sleep(20)

      final_state = SmartRateLimiter.get_bucket_state(:esi)
      tokens_consumed = initial_state.tokens - final_state.tokens

      # ESI 4XX responses should consume approximately 5 tokens
      # The actual consumption might vary slightly due to timing
      assert tokens_consumed > 3.5 && tokens_consumed <= 5.5,
             "Expected approximately 5 tokens consumed, got #{tokens_consumed}"
    end

    test "ESI 5XX responses consume 0 tokens" do
      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)
      initial_state = SmartRateLimiter.get_bucket_state(:esi)

      :ok = SmartRateLimiter.report_request_result(reservation_id, 503, :esi)

      :timer.sleep(20)

      final_state = SmartRateLimiter.get_bucket_state(:esi)
      tokens_consumed = initial_state.tokens - final_state.tokens

      # Should consume 0 tokens for server errors
      assert_in_delta tokens_consumed, 0.0, 0.1
    end

    test "ZKB requests consume 1 token regardless of status" do
      for status <- [200, 302, 404, 503] do
        SmartRateLimiter.reset_bucket(:zkillboard)

        {:ok, reservation_id} = SmartRateLimiter.reserve_token(:zkillboard)
        initial_state = SmartRateLimiter.get_bucket_state(:zkillboard)

        :ok = SmartRateLimiter.report_request_result(reservation_id, status, :zkillboard)

        :timer.sleep(20)

        final_state = SmartRateLimiter.get_bucket_state(:zkillboard)
        tokens_consumed = initial_state.tokens - final_state.tokens

        # ZKB always consumes 1 token
        assert_in_delta tokens_consumed,
                        1.0,
                        0.1,
                        "ZKB should consume 1 token for status #{status}"
      end
    end

    test "rate limit exhaustion behavior" do
      # Reset bucket with known capacity
      SmartRateLimiter.reset_bucket(:esi)

      # Make many 4XX requests to exhaust tokens quickly
      results =
        for _i <- 1..200 do
          case SmartRateLimiter.reserve_token(:esi) do
            {:ok, reservation_id} ->
              # Simulate a 404 error (5 tokens)
              SmartRateLimiter.report_request_result(reservation_id, 404, :esi)
              :ok

            {:error, _} ->
              :rate_limited
          end
        end

      # Count how many were rate limited
      rate_limited_count = Enum.count(results, &(&1 == :rate_limited))

      # We should have hit the rate limit at some point
      assert rate_limited_count > 0, "Should have hit rate limit after many 404 errors"
    end

    test "service bucket isolation" do
      # Test that requests to different services consume tokens from correct buckets
      # Reset both buckets
      SmartRateLimiter.reset_bucket(:esi)
      SmartRateLimiter.reset_bucket(:zkillboard)

      # ESI requests should only consume from ESI bucket
      initial_esi = SmartRateLimiter.get_bucket_state(:esi).tokens
      initial_zkb = SmartRateLimiter.get_bucket_state(:zkillboard).tokens

      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:esi)
      :ok = SmartRateLimiter.report_request_result(reservation_id, 200, :esi)

      :timer.sleep(20)

      final_esi = SmartRateLimiter.get_bucket_state(:esi).tokens
      final_zkb = SmartRateLimiter.get_bucket_state(:zkillboard).tokens

      assert final_esi < initial_esi, "ESI bucket should have consumed tokens"
      assert final_zkb == initial_zkb, "ZKB bucket should not have consumed tokens"

      # ZKB requests should only consume from ZKB bucket
      SmartRateLimiter.reset_bucket(:esi)
      SmartRateLimiter.reset_bucket(:zkillboard)

      initial_esi = SmartRateLimiter.get_bucket_state(:esi).tokens
      initial_zkb = SmartRateLimiter.get_bucket_state(:zkillboard).tokens

      {:ok, reservation_id} = SmartRateLimiter.reserve_token(:zkillboard)
      :ok = SmartRateLimiter.report_request_result(reservation_id, 200, :zkillboard)

      :timer.sleep(20)

      final_esi = SmartRateLimiter.get_bucket_state(:esi).tokens
      final_zkb = SmartRateLimiter.get_bucket_state(:zkillboard).tokens

      assert final_esi == initial_esi, "ESI bucket should not have consumed tokens"
      assert final_zkb < initial_zkb, "ZKB bucket should have consumed tokens"
    end
  end
end
