defmodule Mix.Tasks.Openapi.GenTest do
  use WandererKills.UnifiedTestCase, async: false, type: :unit, mocks: false, clear_caches: false

  import ExUnit.CaptureIO
  import WandererKills.OpenApiAssertions

  alias WandererKillsWeb.ApiSpec

  setup do
    output = Path.join(System.tmp_dir!(), "openapi-#{System.unique_integer([:positive])}.json")
    Mix.Task.reenable("openapi.gen")

    on_exit(fn ->
      File.rm(output)
      Mix.Task.reenable("openapi.gen")
    end)

    %{output: output}
  end

  test "writes resolved request and response schemas as JSON", %{output: output} do
    capture_io(fn -> Mix.Task.run("openapi.gen", ["--output", output]) end)

    output
    |> File.read!()
    |> Jason.decode!()
    |> assert_resolved_spec()
  end

  test "explicit CI resolution does not change the generated document", %{output: output} do
    capture_io(fn -> Mix.Task.run("openapi.gen", ["--output", output]) end)
    generated = output |> File.read!() |> Jason.decode!()

    resolved_again =
      ApiSpec.spec()
      |> OpenApiSpex.resolve_schema_modules()
      |> Jason.encode!()
      |> Jason.decode!()

    assert generated == resolved_again
  end
end
