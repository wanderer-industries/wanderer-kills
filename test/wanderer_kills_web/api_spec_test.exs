defmodule WandererKillsWeb.ApiSpecTest do
  use WandererKills.UnifiedTestCase, async: false, type: :conn, clear_caches: false

  import WandererKills.OpenApiAssertions

  test "GET /api/openapi serves resolved request and response schemas", %{conn: conn} do
    conn
    |> get("/api/openapi")
    |> json_response(200)
    |> assert_resolved_spec()
  end
end
