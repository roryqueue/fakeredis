defmodule FakeRedisTest do
  use ExUnit.Case
  doctest FakeRedis

  setup do
    {:ok, connection} = FakeRedis.start_link

    %{conn: connection}
  end

  test "set/2 and get/1: basic set and get val", %{conn: conn} do
    testval = "TESTVAL"

    set_response = FakeRedis.command!(conn, ~w(SET TESTKEY #{testval}))
    get_response = FakeRedis.command!(conn, ~w(GET TESTKEY))

    assert "OK" = set_response
    assert testval === get_response
  end

end
