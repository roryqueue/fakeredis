defmodule FakeRedisTest do
  use ExUnit.Case
  doctest FakeRedis

  setup do
    {:ok, connection} = FakeRedis.start_link

    %{conn: connection}
  end

  test "set/2, get/1, getset/2: set and get basic values", %{conn: conn} do
    test_key = "TESTKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, first_val])
    assert first_val === FakeRedis.get!(conn, test_key)
    assert first_val === FakeRedis.getset!(conn, [test_key, second_val])
    assert second_val === FakeRedis.get!(conn, test_key)
    assert nil === FakeRedis.get!(conn, empty_key)
  end

  test "mset/2, mget/1: set and get many values", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    third_key = "THIRDKEY"
    empty_key = "EMPTYKEY"

    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"

    assert "OK" = FakeRedis.mset!(
      conn,
      [first_key, first_val, second_key, second_val, third_key, third_val]
    )
    assert [first_val, second_val, third_val, nil] === FakeRedis.mget!(
      conn,
      [first_key, second_key, third_key, empty_key]
    )
  end

  test "set/2 with EX, ttl/2: expiring keys on set and checking in secs", %{conn: conn} do
    example_key = "TTLKEY"
    example_val = "TTLVAL"
    empty_key = "EMPTYKEY"
    wait_secs = 1

    assert "OK" = FakeRedis.set!(
      conn,
      [example_key, example_val, "EX", wait_secs]
    )
    assert example_val === FakeRedis.get!(conn, example_key)
    :timer.sleep(1)

    first_ttl = FakeRedis.ttl!(conn, example_key)
    assert first_ttl < wait_secs
    refute first_ttl < 0

    :timer.sleep(wait_secs * 1000)
    assert nil === FakeRedis.get!(conn, example_key)

    assert -2 = FakeRedis.ttl!(conn, empty_key)
  end

  test "set/2 with PX, pttl/2: checking ttl in milliseconds", %{conn: conn} do
    example_key = "PTTLKEY"
    example_val = "PTTLVAL"
    empty_key = "EMPTYKEY"
    wait_msecs = 500

    assert "OK" = FakeRedis.set!(
      conn,
      ~w(#{example_key} #{example_val} PX #{wait_msecs})
    )
    assert example_val === FakeRedis.get!(conn, example_key)

    :timer.sleep(1)
    intermediate_ttl = FakeRedis.pttl!(conn, example_key)
    assert intermediate_ttl < wait_msecs
    refute intermediate_ttl < 0

    :timer.sleep(wait_msecs)
    assert nil === FakeRedis.get!(conn, example_key)

    assert -2 = FakeRedis.pttl!(conn, empty_key)
  end

  test "persist/2: removing key ttls and preventing expiration", %{conn: conn} do
    example_key = "EXPIREKEY"
    example_val = "EXPIREVAL"
    empty_key = "EMPTYKEY"
    wait_secs = 1

    assert "OK" = FakeRedis.set!(
      conn,
      [example_key, example_val, "EX", wait_secs]
    )
    assert 1 = FakeRedis.persist!(conn, example_key)

    assert -1 = FakeRedis.ttl!(conn, example_key)

    :timer.sleep((wait_secs * 1000) + 1)
    assert example_val === FakeRedis.get!(conn, example_key)

    assert 0 = FakeRedis.persist!(conn, empty_key)
  end

  test "exists/2: checking if a key exists", %{conn: conn} do
    example_key = "EXPIREKEY"
    example_val = "EXPIREVAL"
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [example_key, example_val])
    assert 1 = FakeRedis.exists!(conn, example_key)

    assert 0 = FakeRedis.exists!(conn, empty_key)
  end

  test "keys/1: list all keys in an instance", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    third_key = "THIRDKEY"

    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"

    assert [] = FakeRedis.keys!(conn)
    assert "OK" = FakeRedis.mset!(
      conn,
      [first_key, first_val, second_key, second_val, third_key, third_val]
    )
    assert ([first_key, second_key, third_key] |> Enum.sort) ===
      (FakeRedis.keys!(conn) |> Enum.sort)
  end

  test "pexpire/2, pttl/1: expiring keys in ms after set", %{conn: conn} do
    example_key = "PEXPIREKEY"
    example_val = "PEXPIREVAL"
    empty_key = "EMPTYKEY"
    wait_msecs = 500

    assert "OK" = FakeRedis.set!(conn, [example_key, example_val])
    assert 1 = FakeRedis.pexpire!(conn, [example_key, wait_msecs])

    assert example_val === FakeRedis.get!(conn, example_key)

    :timer.sleep(1)
    intermediate_ttl = FakeRedis.pttl!(conn, example_key)
    assert intermediate_ttl < wait_msecs
    refute intermediate_ttl < 0

    :timer.sleep(wait_msecs)
    assert nil === FakeRedis.get!(conn, example_key)

    assert 0 === FakeRedis.pexpire!(conn, [empty_key, wait_msecs])
  end

  test "expire/2, ttl/1: expiring keys in seconds after set", %{conn: conn} do
    example_key = "EXPIREKEY"
    example_val = "EXPIREVAL"
    empty_key = "EMPTYKEY"
    wait_secs = 1

    assert "OK" = FakeRedis.set!(conn, [example_key, example_val])
    assert 1 = FakeRedis.expire!(conn, [example_key, wait_secs])

    assert example_val === FakeRedis.get!(conn, example_key)
    :timer.sleep(1)

    intermediate_ttl = FakeRedis.ttl!(conn, example_key)
    assert intermediate_ttl < wait_secs
    refute intermediate_ttl < 0

    :timer.sleep(wait_secs * 1000)
    assert nil === FakeRedis.get!(conn, example_key)

    assert 0 === FakeRedis.expire!(conn, [empty_key, wait_secs])
  end

end
