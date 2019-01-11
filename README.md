[![Build Status](https://travis-ci.org/roryqueue/fakeredis.svg)](https://travis-ci.org/roryqueue/fakeredis)
[![Hex.pm](https://img.shields.io/hexpm/v/fakeredis.svg)](https://hex.pm/packages/fakeredis)


# FakeRedis

**FakeRedis recreates the Redis API using only native Erlang/Elixir features, especially [ETS](http://erlang.org/doc/man/ets.html#lookup_element-3).**

This package recreates the Redis API for development and testing purposes, ignoring thread safety and using Erlang Term Storage and no outside dependencies. This can be useful in development and testing environments because it removes the dependency on an actual Redis instance.

## Usage

Commands can be used either through command/2 or as functions. Per elixir standard, functions ending in a bang (!) will throw an exception is something goes wrong, while those without will return standard :ok/:error format:


Command behavior matches the [Redis command API](https://redis.io/commands/), using elixir list, map, and bitstring types to replace the Redis array, hash, and string types repectively.

  1. Using named command functions without bang:

```elixir
iex(1)> {status, result} = FakeRedis.set(connection, ["key", "value"])
{:ok, "OK"}
iex(1)> {status, result} = FakeRedis.get(connection, "key")
{:ok, "value"}
```

  2. Using named command functions with bang:

```elixir
iex(1)> result = FakeRedis.set!(connection, ["key", "value"])
"OK"
iex(1)> result = FakeRedis.get!(connection, "key")
"value"
```

  3. Using command/2 (without bang):

```elixir
iex(1)> {status, result} = FakeRedis.command(connection, ~w(SET key value))
{:ok, "OK"}
iex(1)> {status, result} = FakeRedis.command(connection, ~w(GET key value))
{:ok, "value"}
```

  4. Using command!/2 (with bang):

```elixir
iex(1)> {status, result} = FakeRedis.command!(connection, ~w(SET key value))
"OK"
iex(1)> {status, result} = FakeRedis.command!(connection, ~w(GET key value))
"value"
```

Supported commands are `keys`, `set`, `setnx`, `setex`, `psetex`, `mset`, `msetnx`, `get`, `getset`, `mget`, `expire`, `expireat`, `pexpire`, `pexpireat`, `ttl`, `pttl`, `exists`, `del`, `persist`, `incr`, `incrby`, `decr`, `decrby`, `strlen`, `append`, `getrange`, `setrange`, `hget`, `hgetall`, `hmget`, `hkeys`, `hvals`, `hexists`, `hlen`, `hdel`, `hset`, `hsetnx`, `hincrby`, `lpushall`, `lpush`, `lpushx`, `rpush`, `rpushx`, `llen`, `lpop`, `rpop`, `rpoplpush`, `lset`, `lindex`, `linsert`, `ltrim`, `lrem`.

## Installation

This package can be installed as:

  1. Add `fakeredis` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:fakeredis, "~> 0.2.0"}]
end
```

  2. Ensure `fakeredis` is started before your application:

```elixir
def application do
  [applications: [:fakeredis]]
end
```

