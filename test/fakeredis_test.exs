defmodule FakeRedisTest do
  use ExUnit.Case
  doctest FakeRedis

  setup do
    {:ok, conn} = FakeRedis.start_link

    on_exit fn ->
      FakeRedis.stop(conn)
    end

    [conn: conn]
  end
end
