defmodule Redets do

  defp random_name( length \\ 8 ) do
    :crypto.strong_rand_bytes(length)
    |> Base.url_encode64
    |> binary_part(0, length)
    |> String.to_atom
  end


  def start_link, do: start_link(random_name)

  def start_link(name, options \\ [:named_table, :public]) do
    name
    |> Atom.to_string
    |> Kernel.<>("_redets_table")
    |> String.to_atom
    |> :ets.new(options)
  end


  def command(conn, [ command_type | command_args ]) do
    case String.upcase(command_type) do
      "SET" -> set(conn, command_args)
      "SETNX" -> setnx(conn, command_args)
      "SETEX" -> setex(conn, command_args)
      "PSETEX" -> psetex(conn, command_args)
      "GET" -> get(conn, command_args)
      "GETSET" -> getset(conn, command_args)
      "EXPIRE" -> expire(conn, command_args)
      "EXPIREAT" -> expireat(conn, command_args)
      "PEXPIRE" -> pexpire(conn, command_args)
      "PEXPIREAT" -> pexpireat(conn, command_args)
      "TTL" -> ttl(conn, command_args)
      "PTTL" -> pttl(conn, command_args)
      "EXISTS" -> exists(conn, command_args)
      "DEL" -> del(conn, command_args)
      "PERSIST" -> persist(conn, command_args)
      _ -> raise ArgumentError, "Can't match command"
    end
  end

  def set!(conn, command_args), do: command!(conn, ["SET" | command_args])
  def setnx!(conn, command_args), do: command!(conn, ["SETNX" | command_args])
  def setex!(conn, command_args), do: command!(conn, ["SETEX" | command_args])
  def psetex!(conn, command_args), do: command!(conn, ["PSETEX" | command_args])
  def get!(conn, command_args), do: command!(conn, ["GET" | command_args])
  def getset!(conn, command_args), do: command!(conn, ["GETSET" | command_args])
  def expire!(conn, command_args), do: command!(conn, ["EXPIRE" | command_args])
  def expireat!(conn, command_args), do: command!(conn, ["EXPIREAT" | command_args])
  def pexpire!(conn, command_args), do: command!(conn, ["PEXPIRE" | command_args])
  def pexpireat!(conn, command_args), do: command!(conn, ["PEXPIREAT" | command_args])
  def ttl!(conn, command_args), do: command!(conn, ["TTL" | command_args])
  def pttl!(conn, command_args), do: command!(conn, ["PTTL" | command_args])
  def exists!(conn, command_args), do: command!(conn, ["EXISTS" | command_args])
  def del!(conn, command_args), do: command!(conn, ["DEL" | command_args])
  def persist!(conn, command_args), do: command!(conn, ["PERSIST" | command_args])

  def command!(conn, command) do
    case command(conn, command) do
      {:ok, resp} ->
        resp
      {:error, error} ->
        raise error
    end
  end


  defp map_extra_args(raw_args, mapped_args \\ %{}, _pending_key \\ nil)
  defp map_extra_args([], mapped_args, _pending_key), do: mapped_args

  defp map_extra_args([next_arg | remainder], mapped_args, pending_key) do
    cond do
      !is_nil(pending_key) ->
        map_extra_args(remainder, Map.put(mapped_args, pending_key, next_arg))
      next_arg in ["NX", "nx", "XX", "xx"] ->
        map_extra_args(remainder, Map.put(mapped_args, String.upcase(next_arg), true))
      next_arg in ["EX", "ex", "PX", "px"] ->
        map_extra_args(remainder, mapped_args, String.upcase(next_arg))        
      true -> raise ArgumentError, "Can't match extra arg"
    end
  end


  defp set(conn, key, value, extra_args) do
    arg_keys = Map.keys(extra_args)
    ttl = cond do
      "EX" in arg_keys ->
        (Map.get(extra_args, "EX") * 1000) + :os.system_time(:milli_seconds)
      "PX" in arg_keys ->
        Map.get(extra_args, "EX") + :os.system_time(:milli_seconds)
      true -> nil
    end

    cond do
      "NX" in arg_keys ->
        # matching redis's API, we will return "OK" if the key is set
        # of nil if it is not
        if :ets.insert_new(conn, {key, {value, ttl}}) do
          "OK"
        else
          nil
        end
      "XX" in arg_keys ->
        # if the key is currently empty, lookup will return an empty list
        # so in the case of "XX" we don't want to set
        if :ets.lookup(conn, key) === [] do
          nil
        else
          :ets.insert(conn, {key, {value, ttl}})
          "OK"
        end
      true ->
        :ets.insert(conn, {key, {value, ttl}})
        "OK"
    end
  end

  def set(conn, [key, value | remaining_args]) do
    extra_args = map_extra_args(remaining_args)
    set(conn, key, value, extra_args)
  end
 

  def setnx(conn, command_args), do: set(conn, command_args ++ ["NX"])


  def set_with_exp(conn, [key, exp_val, value | _remainder], exp_key \\ "EX") do
    set(conn, [key, value, exp_key, exp_val])
  end

  def setex(conn, command_args), do: set_with_exp(conn, command_args, "EX")
  def psetex(conn, command_args), do: set_with_exp(conn, command_args, "PX")

  # get only has one argument outside the reference to our redets instance (the key)
  # so we'll allow a one-element list for consistency but also the key itself
  def get(conn, [key | _tail]), do: get(conn, key)

  def get(conn, key) do
    value_list = :ets.lookup(conn, key)
    if value_list === [] do
      nil
    else
      [{value, ttl} | _tail] = value_list
      if ttl < :os.system_time(:milli_seconds) do
        nil
      else
        value
      end
    end
  end


  def getset(conn, command_args) do
    return_val = get(conn, command_args)
    set(conn, command_args)
    return_val
  end


  def expire(conn, [key, ttl]) do
    pexpire(conn, [key, ttl * 1000])
  end

  def expireat(conn, [key, expiry_time]) do
    pexpireat(conn, [key, expiry_time * 1000])
  end

  def pexpire(conn, [key, ttl]) do
    pexpireat(conn, [key, ttl + :os.system_time(:milli_seconds)])
  end

  def pexpireat(conn, [key, expiry_time]) do
    :ets.update_element(conn, key, {1, expiry_time})
  end


  def ttl(conn, [key | _tail]), do: ttl(conn, key)

  def ttl(conn, key) do
    pttl(conn, key) / 1000
  end

  def pttl(conn, [key | _tail]), do: pttl(conn, key)

  def pttl(conn, key) do
    value_list = :ets.lookup(conn, key)
    if value_list === [] do
      -2
    else
      [{value, ttl} | _tail] = value_list
      if is_nil(ttl) do
        -1
      else
        ttl
      end
    end
  end


  def exists(conn, keys, counter \\ 0)
  def exists(_conn, [], counter), do: counter

  def exists(conn, [next_key, remaining_keys], counter) do
    if get(conn, next_key) do
      exists(conn, remaining_keys, counter + 1)
    else
      exists(conn, remaining_keys, counter)
    end
  end


  def del(conn, keys, counter \\ 0)
  def del(_conn, [], counter), do: counter

  def del(conn, [next_key, remaining_keys], counter) do
    next_value = get(conn, next_key)
    :ets.delete(conn, next_key)
    if next_value do
      del(conn, remaining_keys, counter + 1)
    else
      del(conn, remaining_keys, counter)
    end
  end


  def persist(conn, [key | _tail]), do: persist(conn, key)

  def persist(conn, key) do
    :ets.update_element(conn, key, {1, nil})
  end

end
