# redETS

**redETS recreates the Redis API using only native Erlang/Elixir features, especially [ETS](http://erlang.org/doc/man/ets.html#lookup_element-3)**

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

