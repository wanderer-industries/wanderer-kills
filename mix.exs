defmodule WandererKills.MixProject do
  use Mix.Project

  def project do
    [
      app: :wanderer_kills,
      version: "1.6.2",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      compilers: Mix.compilers() ++ [:boundary],
      deps: deps(),
      description:
        "A standalone service for retrieving and caching EVE Online killmails from zKillboard",
      package: package(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),

      # Coverage configuration
      test_coverage: [tool: ExCoveralls],

      # Boundary configuration
      boundary: [
        default: [
          check: [
            apps: [:wanderer_kills, :wanderer_kills_web]
          ]
        ]
      ],

      # Dialyzer configuration
      dialyzer: [
        ignore_warnings: ".dialyzer_ignore.exs",
        plt_add_apps: [:ex_unit, :mix]
      ]
    ]
  end

  # CLI configuration. Replaces the `:preferred_cli_env` project key, which
  # Elixir 1.17 deprecated and newer versions ignore outright — leaving plain
  # `mix test` to run in :dev, where test-only deps such as :mox do not exist.
  def cli do
    [
      preferred_envs: [
        test: :test,
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        "coveralls.xml": :test,
        "test.headless": :test,
        "test.core": :test
      ]
    ]
  end

  # The OTP application entrypoint:
  def application do
    [
      extra_applications: [
        :logger,
        :telemetry_poller
      ],
      mod: {WandererKills.Application, []}
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Phoenix framework (optional - can be excluded for headless operation)
      {:phoenix, "~> 1.8", optional: true},
      {:plug_cowboy, "~> 2.9", optional: true},

      # JSON parsing
      {:jason, "~> 1.4"},

      # Caching
      {:cachex, "~> 4.1"},

      # HTTP client. Finch is used directly by WandererKills.Http.Client; it was
      # previously pulled in only as a transitive dependency of :req, which
      # nothing in this codebase ever called.
      {:finch, "~> 0.20"},
      {:backoff, "~> 1.1"},

      # CSV parsing
      {:nimble_csv, "~> 1.3"},

      # Parallel processing
      {:flow, "~> 1.2"},

      # Telemetry
      {:telemetry_poller, "~> 1.2"},

      # Phoenix PubSub for real-time killmail distribution
      {:phoenix_pubsub, "~> 2.2"},

      # Server-Sent Events with PubSub integration
      {:sse_phoenix_pubsub, "~> 1.0"},

      # OpenAPI specification
      {:open_api_spex, "~> 3.22"},

      # Development and test tools
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:boundary, "~> 0.10", runtime: false},
      {:mox, "~> 1.2", only: :test},

      # Code coverage
      {:excoveralls, "~> 0.18", only: :test},

      # Property-based testing
      {:stream_data, "~> 1.4", only: [:test, :dev]}
    ]
  end

  defp package do
    [
      name: "wanderer_kills",
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/guarzo/wanderer_kills"}
    ]
  end

  defp aliases do
    [
      check: [
        "format --check-formatted",
        "credo",
        "dialyzer"
      ],
      "test.coverage": ["coveralls.html"],
      "test.coverage.ci": ["coveralls.json"],
      # `mix test` may only run in :test, so headless mode is selected via the
      # WANDERER_KILLS_HEADLESS env var that Application.start_web_components?/0
      # already checks ahead of app config — no separate MIX_ENV needed.
      # Tests tagged :web need WandererKillsWeb.Endpoint, which headless mode
      # does not start, so they are excluded rather than left to fail.
      "test.headless": [&set_headless/1, "test --exclude web"],
      "test.core": [&set_headless/1, "test --exclude web test/wanderer_kills/"],
      "test.perf": ["test --include perf test/performance/"]
    ]
  end

  defp set_headless(_args), do: System.put_env("WANDERER_KILLS_HEADLESS", "true")
end
