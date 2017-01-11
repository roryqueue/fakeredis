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

  test "set/2 with NX: setting only if does not exist", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"

    assert "OK" = FakeRedis.set!(conn, [first_key, first_val])
    assert first_val === FakeRedis.get!(conn, first_key)
    assert nil === FakeRedis.set!(conn, [first_key, second_val, "NX"])
    assert first_val === FakeRedis.get!(conn, first_key)
    assert "OK" === FakeRedis.set!(conn, [second_key, second_val, "NX"])
    assert second_val === FakeRedis.get!(conn, second_key)
  end

  test "setnx/2: setting only if does not exist", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"

    assert "OK" = FakeRedis.set!(conn, [first_key, first_val])
    assert first_val === FakeRedis.get!(conn, first_key)
    assert nil === FakeRedis.setnx!(conn, [first_key, second_val])
    assert first_val === FakeRedis.get!(conn, first_key)
    assert "OK" === FakeRedis.setnx!(conn, [second_key, second_val])
    assert second_val === FakeRedis.get!(conn, second_key)
  end

  test "msetnx/2: setting multiple only if none exist", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    third_key = "THIRDKEY"
    fourth_key = "FOURTHKEY"

    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"
    fourth_val = "FOURTHVAL"

    assert "OK" = FakeRedis.set!(conn, [second_key, second_val])
    assert 0 = FakeRedis.msetnx!(
      conn,
      [first_key, first_val, second_key, second_val]
    )
    assert 1 = FakeRedis.msetnx!(
      conn,
      [third_key, third_val, fourth_key, fourth_val]
    )

    assert nil === FakeRedis.get!(conn, first_key)
    assert third_val === FakeRedis.get!(conn, third_key)
    assert fourth_val === FakeRedis.get!(conn, fourth_key)
  end

  test "set/2 with XX: setting only if does exist", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"

    assert "OK" = FakeRedis.set!(conn, [first_key, first_val])
    assert first_val === FakeRedis.get!(conn, first_key)
    assert "OK" === FakeRedis.set!(conn, [first_key, second_val, "XX"])
    assert second_val === FakeRedis.get!(conn, first_key)
    assert nil === FakeRedis.set!(conn, [second_key, second_val, "XX"])
    assert nil === FakeRedis.get!(conn, second_key)
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

  test "incr/2: incrementing a key", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    third_key = "THIRDKEY"

    first_val = 5
    second_val = "7"

    assert "OK" = FakeRedis.set!(conn, [first_key, first_val])
    assert "OK" = FakeRedis.set!(conn, [second_key, second_val])

    assert 6 = FakeRedis.incr!(conn, first_key)
    assert 6 = FakeRedis.get!(conn, first_key)
    assert 8 = FakeRedis.incr!(conn, second_key)
    assert 8 = FakeRedis.get!(conn, second_key)
    assert 1 = FakeRedis.incr!(conn, third_key)
    assert 1 = FakeRedis.get!(conn, third_key)
  end

  test "decr/2: decrementing a key", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    third_key = "THIRDKEY"

    first_val = 5
    second_val = "7"

    assert "OK" = FakeRedis.set!(conn, [first_key, first_val])
    assert "OK" = FakeRedis.set!(conn, [second_key, second_val])

    assert 4 = FakeRedis.decr!(conn, first_key)
    assert 4 = FakeRedis.get!(conn, first_key)
    assert 6 = FakeRedis.decr!(conn, second_key)
    assert 6 = FakeRedis.get!(conn, second_key)
    assert -1 = FakeRedis.decr!(conn, third_key)
    assert -1 = FakeRedis.get!(conn, third_key)
  end

  test "incrby/2: incrementing a key by more than one", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    third_key = "THIRDKEY"

    first_val = 5
    second_val = "7"

    first_increment = "3"
    second_increment = 4
    third_increment = 5

    assert "OK" = FakeRedis.set!(conn, [first_key, first_val])
    assert "OK" = FakeRedis.set!(conn, [second_key, second_val])

    assert 8 = FakeRedis.incrby!(conn, [first_key, first_increment])
    assert 8 = FakeRedis.get!(conn, first_key)
    assert 11 = FakeRedis.incrby!(conn, [second_key, second_increment])
    assert 11 = FakeRedis.get!(conn, second_key)
    assert 5 = FakeRedis.incrby!(conn, [third_key, third_increment])
    assert 5 = FakeRedis.get!(conn, third_key)
  end

  test "decrby/2: decrementing a key by more than one", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    third_key = "THIRDKEY"

    first_val = 5
    second_val = "7"

    first_decrement = "3"
    second_decrement = 4
    third_decrement = 5

    assert "OK" = FakeRedis.set!(conn, [first_key, first_val])
    assert "OK" = FakeRedis.set!(conn, [second_key, second_val])

    assert 2 = FakeRedis.decrby!(conn, [first_key, first_decrement])
    assert 2 = FakeRedis.get!(conn, first_key)
    assert 3 = FakeRedis.decrby!(conn, [second_key, second_decrement])
    assert 3 = FakeRedis.get!(conn, second_key)
    assert -5 = FakeRedis.decrby!(conn, [third_key, third_decrement])
    assert -5 = FakeRedis.get!(conn, third_key)
  end

  test "strlen/2: checking the length of a value", %{conn: conn} do
    test_key = "TESTKEY"
    test_val = "TESTVAL"
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_val])
    assert 7 === FakeRedis.strlen!(conn, test_key)
    assert 0 === FakeRedis.strlen!(conn, empty_key)
  end

  test "append/2: appending to a string value", %{conn: conn} do
    first_key = "FIRSTKEY"
    first_val = "FIRSTVAL+"
    first_append = "FIRSTAPPEND"
    second_key = "SECONDKEY"
    second_append = "SECONDAPPEND"

    assert "OK" = FakeRedis.set!(conn, [first_key, first_val])
    assert first_val === FakeRedis.get!(conn, first_key)
    assert String.length(first_val <> first_append) === FakeRedis.append!(conn, [first_key, first_append])
    assert first_val <> first_append === FakeRedis.get!(conn, first_key)
    assert String.length(second_append) === FakeRedis.append!(conn, [second_key, second_append])
    assert second_append === FakeRedis.get!(conn, second_key)
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
