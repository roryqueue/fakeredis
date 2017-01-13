defmodule FakeRedis.Mixfile do
  use Mix.Project

  def project do
    [app: :fakeredis,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),
     description: description(),
     package: package()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    []
  end


  defp deps do
    [{:ex_doc, ">= 0.0.0", only: :dev}]
  end

  defp description do
    """
    FakeRedis recreates the Redis API using only native Erlang/Elixir features,
    especially ETS.
    """
  end

  defp package do
    [# These are the default files included in the package
     name: :fakeredis,
     files: ["lib", "mix.exs", "README.md"],
     maintainers: ["Rory Quinlan"],
     licenses: ["MIT"],
     links: %{"GitHub" => "https://github.com/roryqueue/fakeredis"}]
  end
end
