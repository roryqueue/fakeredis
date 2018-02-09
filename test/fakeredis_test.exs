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

  test "command/2 can be used with first argument string mapping to function name", %{conn: conn} do
    test_key = "TESTKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.command!(conn, ["SET", test_key, first_val])
    assert first_val === FakeRedis.command!(conn, ["GET", test_key])
    assert first_val === FakeRedis.command!(conn, ["GETSET", test_key, second_val])
    assert second_val === FakeRedis.command!(conn, ["GET", test_key])
    assert nil === FakeRedis.command!(conn, ["GET", empty_key])
  end

  test "command/3 does the exact same thing as command/2, with third argument discarded", %{conn: conn} do
    test_key = "TESTKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.command!(conn, ["SET", test_key, first_val], "blah")
    assert first_val === FakeRedis.command!(conn, ["GET", test_key], %{ timeout: 100 })
    assert first_val === FakeRedis.command!(conn, ["GETSET", test_key, second_val], 87.12)
    assert second_val === FakeRedis.command!(conn, ["GET", test_key], ["garbage", "data"])
    assert nil === FakeRedis.command!(conn, ["GET", empty_key], FakeRedis)
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

  test "getrange/2: get a substring from a string value", %{conn: conn} do
    test_key = "TESTKEY"
    test_val = "TESTVAL"
    start_index = 1
    end_index = -2
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_val])
    assert String.slice(test_val, start_index..end_index) ===
      FakeRedis.getrange!(conn, [test_key, start_index, end_index])
    assert_raise RuntimeError, "Key is empty", fn ->
      FakeRedis.getrange!(
        conn,
        [empty_key, start_index, end_index]
      )
    end
  end

  test "setrange/2: get a substring from a string value", %{conn: conn} do
    test_key = "TESTKEY"
    test_val = "TESTVAL"
    test_overwrite = "XYZ"
    start_index = 2
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_val])
    assert String.slice(test_val, 0..(start_index - 1)) <>
      test_overwrite <>
      String.slice(test_val, (start_index + String.length(test_overwrite))..-1) ===
      FakeRedis.setrange!(conn, [test_key, start_index, test_overwrite])
    assert String.pad_leading(
      test_overwrite,
      start_index + String.length(test_overwrite),
      <<0>>
    ) === FakeRedis.setrange!(conn, [empty_key, start_index, test_overwrite])
  end

  test "append/2: appending to a string value", %{conn: conn} do
    first_key = "FIRSTKEY"
    first_val = "FIRSTVAL+"
    first_append = "FIRSTAPPEND"
    second_key = "SECONDKEY"
    second_append = "SECONDAPPEND"

    assert "OK" = FakeRedis.set!(conn, [first_key, first_val])
    assert first_val === FakeRedis.get!(conn, first_key)
    assert String.length(first_val <> first_append) ===
      FakeRedis.append!(conn, [first_key, first_append])
    assert first_val <> first_append === FakeRedis.get!(conn, first_key)
    assert String.length(second_append) ===
      FakeRedis.append!(conn, [second_key, second_append])
    assert second_append === FakeRedis.get!(conn, second_key)
  end

  test "hget/2: accessing subkeys of map entries", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{first_subkey: "first_subval", second_subkey: "second_subval"}
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert "first_subval" === FakeRedis.hget!(conn, [test_key, :first_subkey])
    assert "second_subval" === FakeRedis.hget!(conn, [test_key, "second_subkey"])
    assert nil === FakeRedis.hget!(conn, [test_key, :empty_subkey])
    assert nil === FakeRedis.hget!(conn, [empty_key, :first_subkey])
  end

  test "hmget/2: accessing multiple subkeys of map entries", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{first_subkey: "first_subval", second_subkey: "second_subval"}
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert ["first_subval", "second_subval", nil] ===
      FakeRedis.hmget!(conn, [test_key, :first_subkey, "second_subkey", :empty_subkey])
    assert [nil, nil, nil] ===
      FakeRedis.hmget!(conn, [empty_key, :first_subkey, "second_subkey", :empty_subkey])
  end

  test "hgetall/2: accessing all subkeys of a map entry", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{first_subkey: "first_subval", second_subkey: "second_subval"}
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert [:first_subkey, "first_subval", :second_subkey, "second_subval"] ===
      FakeRedis.hgetall!(conn, test_key)
    assert [] === FakeRedis.hgetall!(conn, [empty_key])
  end

  test "hkeys/2: listing keys of a map entry", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{first_subkey: "first_subval", second_subkey: "second_subval"}
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert [:first_subkey, :second_subkey] === FakeRedis.hkeys!(conn, test_key)
    assert [] === FakeRedis.hkeys!(conn, [empty_key])
  end

  test "hvals/2: listing values of a map entry", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{first_subkey: "first_subval", second_subkey: "second_subval"}
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert ["first_subval", "second_subval"] === FakeRedis.hvals!(conn, test_key)
    assert [] === FakeRedis.hvals!(conn, [empty_key])
  end

  test "hexists/2: checking if a subvalue exists in a hash entry", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{first_subkey: "first_subval", second_subkey: "second_subval"}
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert 1 === FakeRedis.hexists!(conn, [test_key, :first_subkey])
    assert 0 === FakeRedis.hexists!(conn, [test_key, :fake_subkey])
    assert 0 === FakeRedis.hexists!(conn, [empty_key, :first_subkey])
  end

  test "hlen/2: checking the size of a hash entry", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{first_subkey: "first_subval", second_subkey: "second_subval"}
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert 2 === FakeRedis.hlen!(conn, test_key)
    assert 0 === FakeRedis.hlen!(conn, empty_key)
  end

  test "hdel/2: deleting a subkey of a hash entry", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{
      first_subkey: "first_subval",
      second_subkey: "second_subval",
      third_subkey: "third_subval"
    }
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert 2 === FakeRedis.hdel!(conn, [test_key, :first_subkey, :second_subkey])
    assert nil === FakeRedis.hget!(conn, [test_key, :first_subkey])
    assert test_map[:third_subkey] === FakeRedis.hget!(conn, [test_key, :third_subkey])
    assert 0 === FakeRedis.hdel!(conn, empty_key)
  end

  test "hset/2: setting a subkey within a hash field", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{first_subkey: "first_subval", second_subkey: "second_subval"}
    update_subkey = :update_subkey
    update_subvalue = "update_subvalue"
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert 0 === FakeRedis.hset!(conn, [test_key, :first_subkey, update_subvalue])
    assert 1 === FakeRedis.hset!(conn, [test_key, update_subkey, update_subvalue])
    assert %{
      first_subkey: update_subvalue,
      second_subkey: "second_subval",
      update_subkey: update_subvalue
    } === FakeRedis.get!(conn, test_key)
    assert 1 === FakeRedis.hset!(conn, [empty_key, update_subkey, update_subvalue])
    assert %{update_subkey => update_subvalue} === FakeRedis.get!(conn, empty_key)
  end

  test "hsetnx/2: setting a subkey within a hash field if it does not exist", %{conn: conn} do
    test_key = "TESTKEY"
    test_map = %{first_subkey: "first_subval", second_subkey: "second_subval"}
    update_subkey = :update_subkey
    update_subvalue = "update_subvalue"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert 0 === FakeRedis.hsetnx!(conn, [test_key, :first_subkey, update_subvalue])
    assert 1 === FakeRedis.hsetnx!(conn, [test_key, update_subkey, update_subvalue])
    assert %{
      first_subkey: "first_subval",
      second_subkey: "second_subval",
      update_subkey: update_subvalue
    } === FakeRedis.get!(conn, test_key)
  end

  test "hincrby/2: incrementing a subelement in a hash field", %{conn: conn} do
    test_key = "TESTKEY"
    first_subval = 4
    second_subval = 7
    test_map = %{first_subkey: first_subval, second_subkey: second_subval}
    empty_subkey = :empty_subkey
    empty_key = "EMPTYKEY"
    increment = 2

    assert "OK" = FakeRedis.set!(conn, [test_key, test_map])
    assert first_subval + increment ===
      FakeRedis.hincrby!(conn, [test_key, :first_subkey, increment])
    assert increment === FakeRedis.hincrby!(conn, [test_key, empty_subkey, increment])
    assert %{
      first_subkey: first_subval + increment,
      second_subkey: second_subval,
      empty_subkey: increment
    } === FakeRedis.get!(conn, test_key)
    assert increment ===
      FakeRedis.hincrby!(conn, [empty_key, empty_subkey, increment])
    assert %{empty_subkey: increment} === FakeRedis.get!(conn, empty_key)
  end

  test "lpush/2: prepend to an array value", %{conn: conn} do
    populated_key = "POPULATEDKEY"
    empty_key = "EMPTYKEY"

    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    first_added_val = "FIRSTADDEDVAL"
    second_added_val = "SECONDADDEDVAL"
    full_array = [first_val, second_val]

    assert "OK" = FakeRedis.set!(conn, [populated_key, full_array])
    assert 4 = FakeRedis.lpush!(
      conn,
      [populated_key, first_added_val, second_added_val]
    )
    assert [second_added_val, first_added_val | full_array] ===
      FakeRedis.get!(conn, populated_key)
    assert 2 = FakeRedis.lpush!(
      conn,
      [empty_key, first_added_val, second_added_val]
    )
    assert [second_added_val, first_added_val] ===
      FakeRedis.get!(conn, empty_key)
  end

  test "lpushx/2: prepend to an array value if array exists", %{conn: conn} do
    populated_key = "POPULATEDKEY"
    empty_key = "EMPTYKEY"

    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    first_added_val = "FIRSTADDEDVAL"
    second_added_val = "SECONDADDEDVAL"
    full_array = [first_val, second_val]

    assert "OK" = FakeRedis.set!(conn, [populated_key, full_array])
    assert 4 = FakeRedis.lpushx!(
      conn,
      [populated_key, first_added_val, second_added_val]
    )
    assert [second_added_val, first_added_val | full_array] ===
      FakeRedis.get!(conn, populated_key)
    assert 0 = FakeRedis.lpushx!(
      conn,
      [empty_key, first_added_val, second_added_val]
    )
    assert nil === FakeRedis.get!(conn, empty_key)
  end

  test "rpush/2: append to an array value", %{conn: conn} do
    populated_key = "POPULATEDKEY"
    empty_key = "EMPTYKEY"

    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    first_added_val = "FIRSTADDEDVAL"
    second_added_val = "SECONDADDEDVAL"
    full_array = [first_val, second_val]

    assert "OK" = FakeRedis.set!(conn, [populated_key, full_array])
    assert 4 = FakeRedis.rpush!(
      conn,
      [populated_key, first_added_val, second_added_val]
    )
    assert full_array ++ [first_added_val, second_added_val] ===
      FakeRedis.get!(conn, populated_key)
    assert 2 = FakeRedis.rpush!(
      conn,
      [empty_key, first_added_val, second_added_val]
    )
    assert [first_added_val, second_added_val] ===
      FakeRedis.get!(conn, empty_key)
  end

  test "rpushx/2: append to an array value if array exists", %{conn: conn} do
    populated_key = "POPULATEDKEY"
    empty_key = "EMPTYKEY"

    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    first_added_val = "FIRSTADDEDVAL"
    second_added_val = "SECONDADDEDVAL"
    full_array = [first_val, second_val]

    assert "OK" = FakeRedis.set!(conn, [populated_key, full_array])
    assert 4 = FakeRedis.rpushx!(
      conn,
      [populated_key, first_added_val, second_added_val]
    )
    assert full_array ++ [first_added_val, second_added_val] ===
      FakeRedis.get!(conn, populated_key)
    assert 0 = FakeRedis.rpushx!(
      conn,
      [empty_key, first_added_val, second_added_val]
    )
    assert nil === FakeRedis.get!(conn, empty_key)
  end

  test "llen/2: checking the size of a array entry", %{conn: conn} do
    test_key = "TESTKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    test_array = [first_val, second_val]
    empty_key = "EMPTYKEY"
    wrong_type_key = "WRONGTYPEKEY"
    wrong_type_val = "WRONGTYPEVAL"

    assert "OK" = FakeRedis.set!(conn, [test_key, test_array])
    assert 2 = FakeRedis.llen!(conn, test_key)
    assert 0 = FakeRedis.llen!(conn, empty_key)
    assert "OK" = FakeRedis.set!(conn, [wrong_type_key, wrong_type_val])
    assert_raise RuntimeError, "llen only applies to lists and tuples", fn ->
      FakeRedis.llen!(conn, wrong_type_key)
    end
  end

  test "lpop/2: pop the leftmost subvalue of an array", %{conn: conn} do
    test_key = "TESTKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"
    full_array = [first_val, second_val, third_val]
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, full_array])
    assert first_val === FakeRedis.lpop!(conn, test_key)
    assert [second_val, third_val] === FakeRedis.get!(conn, test_key)
    assert nil === FakeRedis.lpop!(conn, empty_key)
  end

  test "rpop/2: pop the rightmost subvalue of an array", %{conn: conn} do
    test_key = "TESTKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"
    full_array = [first_val, second_val, third_val]
    empty_key = "EMPTYKEY"

    assert "OK" = FakeRedis.set!(conn, [test_key, full_array])
    assert third_val === FakeRedis.rpop!(conn, test_key)
    assert [first_val, second_val] === FakeRedis.get!(conn, test_key)
    assert nil === FakeRedis.rpop!(conn, empty_key)
  end

  test "rpoplpush/2: pop the rightmost subvalue to another array", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"
    fourth_val = "FOURTHVAL"
    first_array = [first_val, second_val]
    second_array = [third_val, fourth_val]

    assert "OK" = FakeRedis.set!(conn, [first_key, first_array])
    assert "OK" = FakeRedis.set!(conn, [second_key, second_array])
    assert second_val === FakeRedis.rpoplpush!(conn, [first_key, second_key])
    assert [first_val] === FakeRedis.get!(conn, first_key)
    assert [second_val, third_val, fourth_val] ===
      FakeRedis.get!(conn, second_key)
  end

  test "lset/2: set a subelement of an array val by index", %{conn: conn} do
    test_key = "FIRSTKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"
    fourth_val = "FOURTHVAL"
    starting_array = [first_val, second_val, third_val]

    assert "OK" = FakeRedis.set!(conn, [test_key, starting_array])
    assert "OK" = FakeRedis.lset!(conn, [test_key, 1, fourth_val])
    assert [first_val, fourth_val, third_val] ===
      FakeRedis.get!(conn, test_key)
  end

  test "lindex/2: find the index of a subelement of an array", %{conn: conn} do
    test_key = "FIRSTKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"
    starting_array = [first_val, second_val, third_val]

    assert "OK" = FakeRedis.set!(conn, [test_key, starting_array])
    assert second_val === FakeRedis.lindex!(conn, [test_key, 1])
    assert nil === FakeRedis.lindex!(conn, [test_key, 3])
  end

  test "linsert/2: add a subelement to an array by index", %{conn: conn} do
    test_key = "FIRSTKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"
    fourth_val = "FOURTHVAL"
    starting_array = [first_val, second_val]

    assert "OK" = FakeRedis.set!(conn, [test_key, starting_array])
    assert 3 = FakeRedis.linsert!(conn, [test_key, "BEFORE", first_val, third_val])
    assert [third_val, first_val, second_val] ===
      FakeRedis.get!(conn, test_key)
    assert 4 = FakeRedis.linsert!(conn, [test_key, "AFTER", first_val, fourth_val])
    assert [third_val, first_val, fourth_val, second_val] ===
      FakeRedis.get!(conn, test_key)
  end

  test "ltrim/2: trim an array value by indices", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"
    fourth_val = "FOURTHVAL"
    starting_array = [first_val, second_val, third_val, fourth_val]

    assert "OK" = FakeRedis.set!(conn, [first_key, starting_array])
    assert "OK" = FakeRedis.ltrim!(conn, [first_key, 1, 2])
    assert [second_val, third_val] === FakeRedis.get!(conn, first_key)

    assert "OK" = FakeRedis.set!(conn, [second_key, starting_array])
    assert "OK" = FakeRedis.ltrim!(conn, [second_key, 2, 99])
    assert [third_val, fourth_val] === FakeRedis.get!(conn, second_key)
  end

  test "lrem/2: remove elements of a list by value", %{conn: conn} do
    first_key = "FIRSTKEY"
    second_key = "SECONDKEY"
    third_key = "THIRDKEY"
    fourth_key = "FOURTHKEY"
    first_val = "FIRSTVAL"
    second_val = "SECONDVAL"
    third_val = "THIRDVAL"
    fourth_val = "FOURTHVAL"
    starting_array = [
      third_val,
      first_val,
      first_val,
      second_val,
      fourth_val,
      second_val,
      first_val
    ]

    assert "OK" = FakeRedis.set!(conn, [first_key, starting_array])
    assert 1 = FakeRedis.lrem!(conn, [first_key, 1, first_val])
    assert [
      third_val,
      first_val,
      second_val,
      fourth_val,
      second_val,
      first_val
    ] === FakeRedis.get!(conn, first_key)

    assert "OK" = FakeRedis.set!(conn, [second_key, starting_array])
    assert 3 = FakeRedis.lrem!(conn, [second_key, 7, first_val])
    assert [
      third_val,
      second_val,
      fourth_val,
      second_val
    ] === FakeRedis.get!(conn, second_key)

    assert "OK" = FakeRedis.set!(conn, [third_key, starting_array])
    assert 1 = FakeRedis.lrem!(conn, [third_key, -1, second_val])
    assert [
      third_val,
      first_val,
      first_val,
      second_val,
      fourth_val,
      first_val
    ] === FakeRedis.get!(conn, third_key)

    assert "OK" = FakeRedis.set!(conn, [fourth_key, starting_array])
    assert 2 = FakeRedis.lrem!(conn, [fourth_key, 0, second_val])
    assert [
      third_val,
      first_val,
      first_val,
      fourth_val,
      first_val
    ] === FakeRedis.get!(conn, fourth_key)

  end

end
