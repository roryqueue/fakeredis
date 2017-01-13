[![Build Status](https://travis-ci.org/roryqueue/fakeredis.svg)](https://travis-ci.org/roryqueue/fakeredis)
[![Hex.pm](https://img.shields.io/hexpm/v/fakeredis.svg)](https://hex.pm/packages/fakeredis)


# FakeRedis

**FakeRedis recreates the Redis API using only native Erlang/Elixir features, especially [ETS](http://erlang.org/doc/man/ets.html#lookup_element-3)**

The redis API is useful and well-known to many developers. The short-term goal of this project is to recreate that API, ignoring thread safety, using Erlang Term Storage and no outside dependencies for development and testing. This can be useful in development and testing environments because it removes the outside dependecy. The longer-term goal is to add thread safety and provide one package which can interact with ETS, DETS, and Redis itself using the same Redis-based API, so developers can switch freely when moving between development, test, and production environments, or when their needs and priorities change.

Command behavior matches the [Redis command API](https://redis.io/commands/), using elixir list, map, and bitstring types to replace the Redis array, hash, and string types repectively. Supported commands are keys, set, setnx, setex, psetex, mset, msetnx, get, getset, mget, expire, expireat, pexpire, pexpireat, ttl, pttl, exists, del, persist, incr, incrby, decr, decrby, strlen, append, getrange, setrange, hget, hgetall, hmget, hkeys, hvals, hexists, hlen, hdel, hset, hsetnx, hincrby, lpushall, lpush, lpushx, rpush, rpushx, llen, lpop, rpop, rpoplpush, lset, lindex, linsert, ltrim, lrem.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `fakeredis` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:fakeredis, "~> 0.1.0"}]
    end
    ```

  2. Ensure `fakeredis` is started before your application:

    ```elixir
    def application do
      [applications: [:fakeredis]]
    end
    ```

