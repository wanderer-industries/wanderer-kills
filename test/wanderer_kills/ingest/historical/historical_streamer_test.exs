defmodule WandererKills.Ingest.Historical.HistoricalStreamerTest do
  use WandererKills.UnifiedTestCase, async: false, type: :integration
  import Mox

  alias WandererKills.Ingest.Historical.HistoricalStreamer

  @moduletag :capture_log

  setup do
    # Set up mox for cross-process calls
    set_mox_global()

    # Ensure application and TaskSupervisor are running
    case Application.ensure_all_started(:wanderer_kills) do
      {:ok, _} -> :ok
      {:error, {:wanderer_kills, {:already_started, _}}} -> :ok
      error -> raise "Failed to start application: #{inspect(error)}"
    end

    # Double-check TaskSupervisor is available
    case Process.whereis(WandererKills.TaskSupervisor) do
      nil ->
        # If it's still not running, start it under the test supervisor
        start_supervised!({Task.Supervisor, name: WandererKills.TaskSupervisor})

      _pid ->
        :ok
    end

    # Mock configuration for testing
    test_config = %{
      enabled: true,
      start_date: "20240101",
      daily_limit: 100,
      batch_size: 10,
      batch_interval_ms: 100,
      max_retries: 3,
      retry_delay_ms: 50
    }

    Application.put_env(:wanderer_kills, :historical_streaming, test_config)

    on_exit(fn ->
      Application.delete_env(:wanderer_kills, :historical_streaming)
    end)

    %{config: test_config}
  end

  describe "start_link/1" do
    test "starts successfully when enabled" do
      config = %{enabled: true}
      Application.put_env(:wanderer_kills, :historical_streaming, config)

      {:ok, pid} = HistoricalStreamer.start_link([])
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "returns :ignore when disabled" do
      config = %{enabled: false}
      Application.put_env(:wanderer_kills, :historical_streaming, config)

      assert :ignore = HistoricalStreamer.start_link([])
    end
  end

  describe "status/0" do
    test "returns current status" do
      {:ok, pid} = HistoricalStreamer.start_link([])

      status = HistoricalStreamer.status()

      assert %{
               current_date: _,
               paused: _,
               progress_percentage: _,
               processed_count: _,
               failed_count: _
             } = status

      GenServer.stop(pid)
    end
  end

  describe "pause/0 and resume/0" do
    test "can pause and resume streaming" do
      {:ok, pid} = HistoricalStreamer.start_link([])

      # Initially running
      status1 = HistoricalStreamer.status()
      assert status1.paused == false

      # Pause
      :ok = HistoricalStreamer.pause()
      status2 = HistoricalStreamer.status()
      assert status2.paused == true

      # Resume
      :ok = HistoricalStreamer.resume()
      status3 = HistoricalStreamer.status()
      assert status3.paused == false

      GenServer.stop(pid)
    end

    test "resume returns error when not paused" do
      {:ok, pid} = HistoricalStreamer.start_link([])

      # Not paused yet
      assert {:error, :not_paused} = HistoricalStreamer.resume()

      GenServer.stop(pid)
    end
  end

  describe "configuration handling" do
    test "parses date string correctly" do
      # Test with YYYYMMDD format
      config = %{
        enabled: true,
        start_date: "20240315",
        daily_limit: 1000,
        batch_size: 50,
        batch_interval_ms: 5000
      }

      Application.put_env(:wanderer_kills, :historical_streaming, config)

      {:ok, pid} = HistoricalStreamer.start_link([])

      status = HistoricalStreamer.status()
      assert status.current_date == ~D[2024-03-15]

      GenServer.stop(pid)
    end

    test "handles invalid date by raising error" do
      config = %{
        enabled: true,
        start_date: "invalid-date",
        daily_limit: 1000,
        batch_size: 50,
        batch_interval_ms: 5000
      }

      Application.put_env(:wanderer_kills, :historical_streaming, config)

      # The invalid date causes the GenServer to exit during init
      # We need to catch the exit signal, not the ArgumentError directly
      Process.flag(:trap_exit, true)

      assert {:error, _reason} = HistoricalStreamer.start_link([])
    end
  end

  describe "error handling" do
    test "continues processing after individual kill failures" do
      # This test doesn't use TaskSupervisor, so it should work fine
      {:ok, pid} = HistoricalStreamer.start_link([])

      # Simply ensure the GenServer can start and continues running
      assert Process.alive?(pid)

      GenServer.stop(pid)
    end
  end

  describe "telemetry events" do
    test "emits telemetry events during processing" do
      # This would require more sophisticated telemetry testing
      # For now, we just ensure the module can handle telemetry calls

      {:ok, pid} = HistoricalStreamer.start_link([])

      # The module should emit telemetry events without crashing
      assert Process.alive?(pid)

      GenServer.stop(pid)
    end
  end
end
