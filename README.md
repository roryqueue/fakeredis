# redETS

**redETS recreates the Redis API using only native Erlang/Elixir features, especially [ETS](http://erlang.org/doc/man/ets.html#lookup_element-3)**

The redis API is useful and well-known to many developers. The short-term goal of this project is to recreate that API using primarily Erlang Term Storage and no outside dependencies. The longer-term goal is to provide one package which can interact with ETS, DETS, and Redis itself using the same Redis-based API, so developers can switch freely when moving between development, test, and production environments, or when their needs and priorities change.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `redets` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:redets, "~> 0.1.0"}]
    end
    ```

  2. Ensure `redets` is started before your application:

    ```elixir
    def application do
      [applications: [:redets]]
    end
    ```

