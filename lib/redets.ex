defmodule Redets do

  def command!(conn, command) do
    case command(conn, command) do
      {:ok, resp} ->
        resp
      {:error, error} ->
        raise error
      _ -> raise "Could not match command return to :ok or :error"
    end
  end

  Enum.each(
    [
      :set, :setnx, :setex, :psetex, :mset, :msetnx,
      :get, :getset, :mget, :expire, :expireat, :pexpire,
      :pexpireat, :ttl, :pttl, :exists, :del,
      :persist, :keys, :incr, :incrby, :decr,
      :decrby, :strlen, :append, :getrange, :setrange,
      :hget, :hgetall, :hmget, :hkeys, :hvals, :hexists,
      :hlen, :hdel, :hset, :hsetnx, :hincr
    ], fn(name) ->
      commandified_name = name |> Atom.to_string |> String.upcase

      def command(conn, [unquote(commandified_name) | command_args]) do
        unquote(name)(conn, command_args)
      end

      def unquote(:"#{name}!")(conn, command_args) do
        command!(conn, [unquote(commandified_name), command_args])
      end
    end
  )

  def command(_conn, _command) do
    raise "Could not match first word in command list to a redets command"
  end

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

  def end_link(conn_or_name), do: :ets.delete(conn_or_name)


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
          {:ok, "OK"}
        else
          {:ok, nil}
        end
      "XX" in arg_keys ->
        # if the key is currently empty, lookup will return an empty list
        # so in the case of "XX" we don't want to set
        if :ets.lookup(conn, key) === [] do
          {:ok, nil}
        else
          :ets.insert(conn, {key, {value, ttl}})
          {:ok, "OK"}
        end
      true ->
        :ets.insert(conn, {key, {value, ttl}})
        {:ok, "OK"}
    end
  end

  def set(conn, [key, value | remaining_args]) do
    set(conn, key, value, map_extra_args(remaining_args))
  end
 
  def setnx(conn, command_args), do: set(conn, command_args ++ ["NX"])

  def set_with_exp(conn, [key, exp_val, value | _remainder], exp_key \\ "EX") do
    set(conn, [key, value, exp_key, exp_val])
  end

  def setex(conn, command_args), do: set_with_exp(conn, command_args, "EX")
  def psetex(conn, command_args), do: set_with_exp(conn, command_args, "PX")

  def mset(conn, command_args, key \\ nil)
  def mset(_conn, [], _key), do: {:ok, "OK"}

  def mset(conn, [next_arg | remaining_args], key) do
    if is_nil(key) do
      mset(conn, remaining_args, next_arg)
    else
      {status, result} = set(conn, [key, next_arg])
      if status === :ok do
        mset(conn, remaining_args)
      else
        {status, result}
      end
    end
  end

  def msetnx(conn, command_args, return_val \\ true, key \\ nil)
  def msetnx(_conn, [], return_val, _key), do: {:ok, return_val}

  def msetnx(conn, [next_arg | remaining_args], return_val, key) do
    if is_nil(key) do
      msetnx(conn, remaining_args, return_val, next_arg)
    else
      {status, result} = setnx(conn, [key, next_arg])
      if status === :ok do
        updated_return_val = if(is_nil(result), do: false, else: return_val)
        msetnx(conn, remaining_args, updated_return_val)
      else
        {status, result}
      end
    end
  end

  # get only has one argument outside the reference to our redets instance (the key)
  # so we'll allow a one-element list for consistency but also the key itself
  def get(conn, [key | _tail]), do: get(conn, key)

  def get(conn, key) do
    value_list = :ets.lookup(conn, key)
    if value_list === [] do
      {:ok, nil}
    else
      [{value, ttl} | _tail] = value_list
      if ttl < :os.system_time(:milli_seconds) do
        :ets.delete(conn, key)
        {:ok, nil}
      else
        {:ok, value}
      end
    end
  end


  # needs lock
  def getset(conn, command_args) do
    {get_status, get_result} = get(conn, command_args)
    if get_status === :ok do
      {set_status, set_result} = set(conn, command_args)
      if set_status == :ok do
        {get_status, get_result}
      else
        {set_status, set_result}
      end
    else
      {get_status, get_result}
    end
  end

  def mget(conn, command_args, results \\ [])
  def mget(_conn, [], results), do: {:ok, results}

  def mget(conn, [next_arg | remaining_args], results) do
    {status, result} = get(conn, next_arg)
    if status === :ok do
      mget(conn, remaining_args, [result | results])
    else
      {status, result}
    end
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
    {:ok, :ets.update_element(conn, key, {1, expiry_time})}
  end


  def ttl(conn, [key | _tail]), do: ttl(conn, key)

  def ttl(conn, key) do
    {status, result} = pttl(conn, key)
    if status === :ok do
      {status, result}
    else
      {status, result / 1000}
    end
  end

  def pttl(conn, [key | _tail]), do: pttl(conn, key)

  def pttl(conn, key) do
    value_list = :ets.lookup(conn, key)
    if value_list === [] do
      {:ok, -2}
    else
      [{value, ttl} | _tail] = value_list
      if is_nil(ttl) do
        {:ok, -1}
      else
        {:ok, ttl}
      end
    end
  end


  def exists(conn, keys, counter \\ 0)
  def exists(_conn, [], counter), do: {:ok, counter}

  def exists(conn, [next_key, remaining_keys], counter) do
    {status, value} = get(conn, next_key)
    if status === :ok and !is_nil(value) do
      exists(conn, remaining_keys, counter + 1)
    else
      exists(conn, remaining_keys, counter)
    end
  end


  def del(conn, keys, counter \\ 0)
  def del(_conn, [], counter), do: {:ok, counter}

  def del(conn, [next_key, remaining_keys], counter) do
    key_exists = :ets.member(conn, next_key)
    :ets.delete(conn, next_key)
    if key_exists do
      del(conn, remaining_keys, counter + 1)
    else
      del(conn, remaining_keys, counter)
    end
  end


  def persist(conn, [key | _tail]), do: persist(conn, key)

  def persist(conn, key) do
    {:ok, :ets.update_element(conn, key, {1, nil})}
  end


  defp keys(conn, keylist) do
    next_key = :ets.next(conn)
    if next_key === '$end_of_table' do
      {:ok, keylist}
    else
      keys(conn, [next_key | keylist])
    end
  end

  def keys(conn) do
    first_key = :ets.first(conn)
    if first_key === '$end_of_table' do
      {:ok, []}
    else
      keys(conn, [first_key])
    end
  end


  def incr(conn, [key | _tail]), do: incr(conn, key)

  def incr(conn, key) do
    incrby(conn, [key, 1])
  end

  # needs lock
  def incrby(conn, [key, increment]) do
    {status, result} = setnx(conn, [key, 0])
    if status === :ok do
      {:ok, :ets.update_counter(conn, key, {0, increment})}
    else
      {status, result}
    end
  end

  def decr(conn, [key | _tail]), do: decr(conn, key)

  def decr(conn, key) do
    decrby(conn, [key, 1])
  end

  def decrby(conn, [key, decrement]) do
    incrby(conn, [key, -decrement])
  end


  def strlen(conn, [key | _tail]), do: strlen(conn, key)

  def strlen(conn, key) do
    {status, value} = get(conn, key)
    if status === :ok do
      {status, if(is_nil(value), do: 0, else: String.length(value))}
    else
      {status, value}
    end
  end


  # needs lock
  def append(conn, [key, value]) do
    {status, result} = get(conn, key)
    if status === :ok do
      if is_nil(value) do
        :ets.insert(conn, {key, {value, nil}})
        {:ok, String.length(value)}
      else
        {initial_value, ttl} = result
        new_value = initial_value <> value
        :ets.update_element(conn, key, {0, new_value})
        {:ok, String.length(new_value)}
      end
    else
      {status, result}
    end
  end

  def getrange(conn, [key, start_index, end_index]) do
    {status, result} = get(conn, key)
    if status === :ok do
      if is_nil(result) do
        {:error, "Key is empty"}
      else
        {initial_value, ttl} = result
        {:ok, String.slice(initial_value, start_index..end_index)}
      end
    else
      {status, result}
    end
  end

  # needs lock
  def setrange(conn, [key, offset, addition]) do
    {status, result} = get(conn, key)
    if status === :ok do
      if is_nil(result) do
        set(conn, [key, String.pad_leading(addition, offset, <<0>>)])
      else
        {initial_value, ttl} = result
        initial_length = String.length(initial_value)
        new_value = if initial_length > offset do
          String.slice(initial_value, 0..offset) <> addition
        else
          initial_value <>
            String.pad_leading(addition, offset - initial_length, <<0>>)
        end
        :ets.update_element(conn, key, {0, new_value})
        {:ok, String.length(new_value)}
      end
    else
      {status, result}
    end
  end


  defp to_untupled_list(initial_map) when is_map(initial_map) do
    initial_map |> Map.to_list |> to_untupled_list
  end

  defp to_untupled_list([{key, value} | tail], result \\ []) do
    to_untupled_list(tail, [key, value | result])
  end


  def hget(conn, [hash_key, element_key]) do
    {status, result} = get(conn, hash_key)
    if status === :ok do
      {status, result[element_key]}
    else
      {status, result}
    end
  end

  defp hmget(conn, command_args, get_result, return_array \\ [])
  defp hmget(_conn, [], _get_result, return_array), do: {:ok, return_array}

  defp hmget(conn, [next_arg | remaining_args], get_result, return_array) do
    hmget(conn, remaining_args, get_result, [get_result[next_arg] | return_array])
  end

  def hmget(conn, [next_arg | remaining_args]) do
    {status, result} = get(conn, next_arg)
    if status === :ok do
      hmget(conn, remaining_args, result)
    else
      {status, result}
    end
  end

  def hgetall(conn, [key | _tail]), do: hgetall(conn, key)

  def hgetall(conn, key) do
    {status, result} = get(conn, key)
    if status === :ok do
      hgetall(conn, to_untupled_list(result))
    else
      {status, result}
    end
  end


  def hkeys(conn, [key | _tail]), do: hkeys(conn, key)

  def hkeys(conn, key) do
    {status, result} = get(conn, key)
    if status === :ok do
      {status, Map.keys(result)}
    else
      {status, result}
    end
  end

  def hvals(conn, [key | _tail]), do: hvals(conn, key)

  def hvals(conn, key) do
    {status, result} = get(conn, key)
    if status === :ok do
      {status, Map.values(result)}
    else
      {status, result}
    end
  end

  def hexists(conn, [hash_key, element_key]) do
    {status, result} = hget(conn, [hash_key, element_key])
    if status === :ok do
      {status, !is_nil(result)}
    else
      {status, result}
    end
  end

  def hlen(conn, [key | _tail]), do: hlen(conn, key)

  def hlen(conn, key) do
    {status, result} = get(conn, key)
    if status === :ok do
      {status, result |> Map.to_list |> length}
    else
      {status, result}
    end
  end

  # needs lock
  def hdel(conn, [hash_key, element_key]) do
    {status, result} = get(conn, hash_key)
    if status === :ok do
      if is_nil(result) do
        {status, result}
      else
        {element, remaining_map} = Map.pop(result, element_key)
        :ets.update_element(conn, hash_key, {0, remaining_map})
        {status, element}
      end
    else
      {status, result}
    end
  end

  # needs lock
  def hset(conn, [hash_key, element_key, element_value], nx \\ false) do
    {status, result} = get(conn, hash_key)
    if status === :ok do
      if is_nil(result) do
        {status, result}
      else
        key_exists = Map.has_key?(result, element_key)
        unless key_exists and nx do
          updated_map = Map.put(result, element_key, element_value)
          :ets.update_element(conn, hash_key, {0, updated_map})
        end        
        {status, !key_exists}
      end
    else
      {status, result}
    end
  end

  def hsetnx(conn, [hash_key, element_key, element_value]) do
    hset(conn, [hash_key, element_key, element_value], true)
  end

  # needs lock
  def hincr(conn, [hash_key, element_key, increment]) do
    {status, result} = get(conn, hash_key)
    if status === :ok do
      if is_nil(result) do
        {status, result}
      else
          updated_value = Map.get(result, element_key, 0) + increment
          updated_map = Map.put(result, element_key, updated_value)
          :ets.update_element(conn, hash_key, {0, updated_map})
        {status, updated_value}
      end
    else
      {status, result}
    end
  end

end













