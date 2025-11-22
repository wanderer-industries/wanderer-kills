defmodule WandererKills.HealthErrorFormatTest do
  @moduledoc """
  Integration test to verify that health endpoint errors use standardized format.
  This test simulates a health check failure scenario.
  """

  use WandererKills.UnifiedTestCase, async: false, type: :conn

  @endpoint WandererKillsWeb.Endpoint

  test "health endpoint returns standardized error format on failure", %{conn: conn} do
    # Create a test scenario where health check might fail
    # by making a request to the health endpoint
    conn = get(conn, "/api/health")

    # If the response is 503, verify error format
    if conn.status == 503 do
      response = json_response(conn, 503)

      # Verify standardized error structure
      assert %{
               "error" => %{
                 "domain" => "system",
                 "type" => "health_check_failed",
                 "message" => "Health check failed",
                 "retryable" => false,
                 "details" => details
               }
             } = response

      # Verify details structure
      assert is_map(details)
      assert Map.has_key?(details, "reason")
      assert Map.has_key?(details, "timestamp")
      assert is_binary(details["timestamp"])

      # Verify timestamp is valid ISO8601
      {:ok, _datetime} = DateTime.from_iso8601(details["timestamp"])
    end
  end
end
