defmodule WandererKills.Ingest.Historical.HistoricalStreamerRetryTest do
  use WandererKills.DataCase, async: false
  import Mox
  import WandererKills.TestHelpers

  alias WandererKills.Ingest.Historical.HistoricalStreamer
  alias WandererKills.Ingest.Killmails.ZkbClient.Mock, as: ZkbClientMock

  @moduletag :capture_log

  # Helper function for polling until condition is met
  defp wait_until(condition_fn, timeout_ms) do
    wait_until_loop(condition_fn, timeout_ms, System.monotonic_time(:millisecond))
  end

  defp wait_until_loop(condition_fn, timeout_ms, start_time) do
    current_time = System.monotonic_time(:millisecond)

    if current_time - start_time >= timeout_ms do
      raise "Timeout waiting for condition after #{timeout_ms}ms"
    end

    if condition_fn.() do
      :ok
    else
      Process.sleep(10)
      wait_until_loop(condition_fn, timeout_ms, start_time)
    end
  end

  setup do
    # Ensure TaskSupervisor is running
    ensure_task_supervisor()

    # Set up mox for cross-process calls
    set_mox_global()

    # Configure with custom retry settings
    test_config = %{
      enabled: true,
      start_date: "20240101",
      daily_limit: 10,
      batch_size: 5,
      batch_interval_ms: 100,
      max_retries: 2,
      retry_delay_ms: 50
    }

    Application.put_env(:wanderer_kills, :historical_streaming, test_config)

    on_exit(fn ->
      Application.delete_env(:wanderer_kills, :historical_streaming)
    end)

    %{config: test_config}
  end

  describe "retry configuration" do
    test "respects max_retries configuration on fetch failures" do
      # Set up expectations - it should try 2 times total (1 initial + 1 retry, then max_retries=2 exceeded)
      ZkbClientMock
      |> expect(:fetch_history, 2, fn
        "20240101" ->
          {:error, :timeout}
      end)

      {:ok, pid} = HistoricalStreamer.start_link([])

      # Allow the mock to be called by the HistoricalStreamer process
      allow(ZkbClientMock, self(), pid)

      # Wait for retries to happen - need more time for initial delay (1000ms) + 3 attempts with delays
      Process.sleep(1500)

      # Should still be alive after retries
      assert Process.alive?(pid)

      GenServer.stop(pid)
    end

    test "uses configured retry_delay_ms between attempts" do
      test_pid = self()

      ZkbClientMock
      |> expect(:fetch_history, 2, fn
        "20240101" ->
          send(test_pid, {:fetch_attempted, System.monotonic_time(:millisecond)})
          {:error, :network_error}
      end)

      {:ok, pid} = HistoricalStreamer.start_link([])

      # Allow the mock to be called by the HistoricalStreamer process
      allow(ZkbClientMock, self(), pid)

      # Collect the timing of attempts - increase timeout for first call to account for 1000ms startup delay
      call_times =
        [
          # First call - needs extra time for startup delay
          receive do
            {:fetch_attempted, time} -> time
          after
            2000 -> nil
          end,
          # Second call (first retry)
          receive do
            {:fetch_attempted, time} -> time
          after
            200 -> nil
          end
        ]
        |> Enum.reject(&is_nil/1)

      # Verify we got 2 attempts (initial + 1 retry, then max_retries=2 exceeded)
      assert length(call_times) == 2

      # Check delays between attempts (should be around 50ms)
      if length(call_times) >= 2 do
        [time1, time2 | _] = call_times
        delay = time2 - time1
        # Allow some variance (20-100ms) for timing flexibility
        assert delay >= 20 and delay <= 100, "Delay was #{delay}ms, expected ~50ms"
      end

      GenServer.stop(pid)
    end

    test "advances to next day after max retries exceeded" do
      _historical_data_day1 = %{}

      historical_data_day2 = %{
        "123456" => "hash123"
      }

      # Expect 2 failures for day 1 (1 initial + 1 retry, then max_retries=2 exceeded), then success for day 2
      ZkbClientMock
      |> expect(:fetch_history, 2, fn
        "20240101" ->
          {:error, :server_error}
      end)
      |> expect(:fetch_history, 1, fn
        "20240102" ->
          {:ok, historical_data_day2}
      end)

      {:ok, pid} = HistoricalStreamer.start_link([])

      # Allow the mock to be called by the HistoricalStreamer process
      allow(ZkbClientMock, self(), pid)

      # Wait for retries and advancement using polling
      # Increase timeout to allow for startup delay + retries + day advancement + day 2 call
      wait_until(
        fn ->
          status = HistoricalStreamer.status()
          # Wait a bit longer to ensure day 2 call happens
          Date.compare(status.current_date, ~D[2024-01-02]) in [:eq, :gt]
        end,
        8000
      )

      # Give additional time for the day 2 call to happen
      Process.sleep(1000)

      # Check status - should have moved to day 2
      status = HistoricalStreamer.status()
      assert Date.compare(status.current_date, ~D[2024-01-02]) in [:eq, :gt]

      GenServer.stop(pid)
    end
  end
end
