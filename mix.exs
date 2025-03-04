defmodule Tortoise311.MixProject do
  use Mix.Project

  @version "0.12.1"
  @source_url "https://github.com/smartrent/tortoise311"

  def project do
    [
      app: :tortoise311,
      version: @version,
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      dialyzer: dialyzer(),
      docs: docs(),
      preferred_cli_env: [
        docs: :docs,
        "hex.publish": :docs,
        "hex.build": :docs
      ]
    ]
  end

  defp description() do
    "An MQTT 3.1.1 client for Elixir"
  end

  def application do
    [
      extra_applications: [:logger, :ssl],
      mod: {Tortoise311.App, []}
    ]
  end

  defp deps do
    [
      {:gen_state_machine, "~> 2.0 or ~> 3.0"},
      {:telemetry, "~> 1.0"},
      {:dialyxir, "~> 1.3", only: :dev, runtime: false},
      {:ex_doc, "~> 0.19", only: :docs, runtime: false},
      {:ct_helper, github: "ninenines/ct_helper", only: :test},
      {:credo, "~> 1.6", only: [:dev, :test], runtime: false}
    ]
  end

  defp package() do
    [
      name: "tortoise311",
      maintainers: ["Jean-Francois Cloutier"],
      licenses: ["Apache 2.0"],
      files: ["lib", "mix.exs", "README*", "CHANGELOG*", "LICENSE*"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs() do
    [
      name: "Tortoise311",
      source_ref: "v#{@version}",
      main: "introduction",
      canonical: "http://hexdocs.pm/tortoise311",
      source_url: @source_url,
      extras: [
        "docs/introduction.md",
        "docs/connecting_to_a_mqtt_broker.md",
        "docs/connection_supervision.md",
        "docs/publishing_messages.md"
      ]
    ]
  end

  defp dialyzer() do
    [
      ignore_warnings: "dialyzer.ignore",
      flags: [:unmatched_returns, :error_handling, :missing_return, :extra_return, :underspecs]
    ]
  end
end
