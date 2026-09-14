defmodule WandererKills.Ingest.Killmails.Pipeline.ValidationTest do
  use WandererKills.UnifiedTestCase, async: true, mocks: false, clear_caches: false

  alias WandererKills.Core.Support.Error
  alias WandererKills.Domain.Killmail
  alias WandererKills.Ingest.Killmails.UnifiedProcessor

  @cutoff ~U[2026-09-13 00:00:00Z]
  @recent "2026-09-14T00:00:00Z"
  @older "2026-09-12T00:00:00Z"

  test "rejects improper attackers before building a recent killmail" do
    assert {:error, %Error{type: :invalid_attackers}} =
             process([%{}, %{} | :invalid], @recent)
  end

  test "rejects improper attackers before filtering an older killmail" do
    assert {:error, %Error{type: :invalid_attackers}} = process([%{} | :invalid], @older)
  end

  test "rejects empty and non-list attackers regardless of killmail age" do
    for attackers <- [[], nil, %{}, :invalid, "invalid"], time <- [@recent, @older] do
      assert {:error, %Error{type: :invalid_attackers}} = process(attackers, time)
    end
  end

  test "accepts nonempty proper attacker lists and retains age filtering" do
    for attackers <- [[%{}], [%{}, %{}]] do
      assert {:ok, %Killmail{killmail_id: 123_456}} = process(attackers, @recent)
      assert {:ok, :kill_older} = process(attackers, @older)
    end
  end

  defp process(attackers, time) do
    killmail = %{
      "killmail_id" => 123_456,
      "solar_system_id" => 30_000_142,
      "killmail_time" => time,
      "victim" => %{"character_id" => 95_465_499, "ship_type_id" => 670},
      "attackers" => attackers,
      "zkb" => %{"hash" => "1234567890abcdef1234567890abcdef12345678"}
    }

    UnifiedProcessor.process_killmail(killmail, @cutoff, validate_only: true)
  end
end
