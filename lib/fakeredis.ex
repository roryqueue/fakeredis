defmodule FakeRedis do

  def command!(conn, command) do
    case command(conn, command) do
      {:ok, resp} ->
        resp
      {:error, error} ->
        raise error
      true -> raise "Could not match command return to :ok or :error"
    end
  end

  def command!(conn, command, _opts), do: command!(conn, command)

  Enum.each(
    [
      :set, :setnx, :setex, :psetex, :mset, :msetnx, :get, :getset,
      :mget, :expire, :expireat, :pexpire, :pexpireat, :ttl, :pttl,
      :exists, :del, :persist, :incr, :incrby, :decr, :decrby,
      :strlen,:append, :getrange, :setrange, :hget, :hgetall, :hmget,
      :hkeys, :hvals, :hexists, :hlen, :hdel, :hset, :hsetnx, :hincrby,
      :lpushall, :lpush, :lpushx, :rpush, :rpushx, :llen, :lpop, :rpop,
      :rpoplpush, :lset, :lindex, :linsert, :ltrim, :lrem
    ], fn (name) ->
      commandified_name = name |> Atom.to_string |> String.upcase

      # point calls to command/2 to the function specified with
      # the first string in the wordlist passed as the second arg
      def command(conn, [unquote(commandified_name) | command_args]) do
        unquote(name)(conn, command_args)
      end

      # then create a bang function for each command function that sends
      # the command through the command!/2 -> command/2 -> {named_command} path
      def unquote(:"#{name}!")(conn, command_args) do
        command_args = if(is_list(command_args), do: command_args, else: [command_args])
        command!(conn, [unquote(commandified_name) | command_args])
      end
    end
  )

  # since keys doesn't take any args except the fakeredis instance
  # we need to define this behavior statically-- it won't be
  # taken care of dynamically above
  def command(conn, ["KEYS"]), do: keys(conn)
  def command(conn, "KEYS"), do: keys(conn)

  def command(_conn, _command) do
    raise "Could not match first word in command list to a fakeredis command"
  end

  # redix uses a third argument to set the timeout
  # we dont care about it, but add command/3 to match that interface
  def command(conn, command, _opts), do: command(conn, command)

  defp random_name(length \\ 8) do
    :crypto.strong_rand_bytes(length)
    |> Base.url_encode64
    |> binary_part(0, length)
    |> String.to_atom
  end


  def start_link, do: start_link(random_name())

  def start_link(name, options \\ [:named_table, :public]) do
    conn =
      name
      |> Atom.to_string
      |> Kernel.<>("_fakeredis")
      |> String.to_atom
      |> :ets.new(options)
    {:ok, conn}
  end


  def end_link(conn_or_name), do: :ets.delete(conn_or_name)

  def stop(conn_or_name), do: end_link(conn_or_name)


  defp map_extra_args(raw_args, mapped_args \\ %{}, _pending_key \\ nil)
  defp map_extra_args([], mapped_args, _pending_key), do: mapped_args

  defp map_extra_args([next_arg | remainder], mapped_args, pending_key) do
    existence_args = ["NX", "nx", "XX", "xx"]
    expiration_args = ["EX", "ex", "PX", "px"]
    cond do
      !is_nil(pending_key) ->
        map_extra_args(remainder, Map.put(mapped_args, pending_key, next_arg))
      next_arg in existence_args ->
        map_extra_args(remainder, Map.put(mapped_args, String.upcase(next_arg), true))
      next_arg in expiration_args ->
        map_extra_args(remainder, mapped_args, String.upcase(next_arg))        
      true -> raise ArgumentError, "Can't match extra arg"
    end
  end


  defp bool_to_int(val) when is_boolean(val), do: if(val, do: 1, else: 0)
  defp bool_to_int(_val), do: raise "bool_to_int only takes booleans"

  defp make_sure_is_int (expiration_num) do
    if is_bitstring(expiration_num) do
      String.to_integer(expiration_num)
    else
      expiration_num
    end
  end

  defp set(conn, key, value, extra_args) do

    arg_keys = Map.keys(extra_args)
    ttl = cond do
      "EX" in arg_keys ->
        extra_args
        |> Map.get("EX")
        |> make_sure_is_int
        |> Kernel.*(1000)
        |> Kernel.+(:os.system_time(:milli_seconds))
      "PX" in arg_keys ->
        extra_args
        |> Map.get("PX")
        |> make_sure_is_int
        |> Kernel.+(:os.system_time(:milli_seconds))
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

  defp set_with_exp(conn, [key, exp_val, value | _remainder], exp_key) do
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


  def msetnx(conn, command_args) do
    keys = Enum.take_every(command_args, 2)
    {get_status, get_result} = mget(conn, keys)

    if get_status === :ok do
      any_vals? =
        get_result
        |> Enum.filter(fn(x) -> !is_nil(x) end)
        |> Kernel.length
        |> Kernel.>(0)

      if any_vals? do
        {:ok, 0}
      else
        {set_status, set_result} = mset(conn, command_args)

        if set_status === :ok do
          {:ok, 1}
        else
          {set_status, set_result}
        end
      end
    else
      {get_status, get_result}
    end
  end

  # get only has one argument outside the reference to our fakeredis instance (the key)
  # so we'll allow a one-element list for consistency but also the key itself
  def get(conn, [key | _tail]), do: get(conn, key)

  def get(conn, key) do
    value_list = :ets.lookup(conn, key)
    if value_list === [] do
      {:ok, nil}
    else
      [{_testkey, {value, expire_time}} | _tail] = value_list

      if expire_time < :os.system_time(:milli_seconds) do
        :ets.delete(conn, key)
        {:ok, nil}
      else
        {:ok, value}
      end
    end
  end

  defp get_with_exp(conn, key) do
    value_list = :ets.lookup(conn, key)
    if value_list === [] do
      {:ok, {nil, nil}}
    else
      [{_testkey, {value, expire_time}} | _tail] = value_list

      if expire_time < :os.system_time(:milli_seconds) do
        :ets.delete(conn, key)
        {:ok, {nil, nil}}
      else
        {:ok, {value, expire_time}}
      end
    end
  end

  # thread unsafe
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
  def mget(_conn, [], results), do: {:ok, Enum.reverse(results)}

  def mget(conn, [next_arg | remaining_args], results) do
    {status, result} = get(conn, next_arg)
    if status === :ok do
      mget(conn, remaining_args, [result | results])
    else
      {status, result}
    end
  end

  
  def expire(conn, [key, ttl]) do
    ttl = if(is_bitstring(ttl), do: String.to_integer(ttl), else: ttl)
    pexpire(conn, [key, ttl * 1000])
  end

  def expireat(conn, [key, expiry_time]) do
    expiry_time = cond do
      is_bitstring(expiry_time) ->
        String.to_integer(expiry_time) * 1000
      is_integer(expiry_time) ->
        expiry_time * 1000
      is_nil(expiry_time) ->
        expiry_time
      true -> raise "Only integer, string, and nil types are accepted"
    end
    pexpireat(conn, [key, expiry_time])
  end

  def pexpire(conn, [key, ttl]) do
    ttl = if(is_bitstring(ttl), do: String.to_integer(ttl), else: ttl)
    pexpireat(
      conn,
      [key, ttl + :os.system_time(:milli_seconds)]
    )
  end

  # thread unsafe
  def pexpireat(conn, [key, expiry_time]) do
    expiry_time = if is_bitstring(expiry_time) do
      String.to_integer(expiry_time)
    else
      expiry_time
    end

    {status, value} = get(conn, key)
    if status !== :ok do
      {status, value}
    else
      if value === nil do
        {:ok, 0}
      else
        {
          :ok,
          :ets.update_element(
            conn,
            key,
            {2, {value, expiry_time}}
          ) |> bool_to_int
        }
      end      
    end
  end


  def ttl(conn, [key | _tail]), do: ttl(conn, key)

  def ttl(conn, key) do
    {status, result} = pttl(conn, key)
    # the (< 0) clause accounts for cases when the key is empty
    # or has no ttl, so we pass those special values back directly
    if status !== :ok or result < 0 do
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
      [{_key, {_value, expire_time}} | _tail] = value_list
      if is_nil(expire_time) do
        {:ok, -1}
      else
        current_time = :os.system_time(:milli_seconds)
        if expire_time < current_time do
          {:ok, -2}
        else
          {:ok, expire_time - current_time}
        end
      end
    end
  end


  def exists(conn, keys, counter \\ 0)
  def exists(_conn, [], counter), do: {:ok, counter}

  def exists(conn, [next_key | remaining_keys], counter) do
    {status, value} = get(conn, next_key)
    if status === :ok and !is_nil(value) do
      exists(conn, remaining_keys, counter + 1)
    else
      exists(conn, remaining_keys, counter)
    end
  end


  def del(conn, keys, counter \\ 0)
  def del(_conn, [], counter), do: {:ok, counter}

  def del(conn, [next_key | remaining_keys], counter) do
    key_exists = :ets.member(conn, next_key)
    :ets.delete(conn, next_key)
    if key_exists do
      del(conn, remaining_keys, counter + 1)
    else
      del(conn, remaining_keys, counter)
    end
  end


  def persist(conn, [key | _tail]), do: persist(conn, key)

  # thread unsafe
  def persist(conn, key) do
    pexpireat(conn, [key, nil])
  end


  defp keys(conn, [last_key | keylist]) do
    next_key = :ets.next(conn, last_key)

    checked_keylist = if get(conn, last_key) do
      [last_key | keylist]
    else
      keylist
    end

    if next_key === :"$end_of_table" do
      {:ok, checked_keylist}
    else
      keys(conn, [next_key | checked_keylist])
    end
  end

  def keys(conn) do
    first_key = :ets.first(conn)
    if first_key === :"$end_of_table" do
      {:ok, []}
    else
      keys(conn, [first_key])
    end
  end

  # since keys doesn't take any args except the fakeredis instance
  # we need to define this behavior statically-- it won't be
  # taken care of dynamically above
  def keys!(conn), do: command!(conn, "KEYS")


  def incr(conn, [key | _tail]), do: incr(conn, key)

  def incr(conn, key) do
    incrby(conn, [key, 1])
  end

  # thread unsafe
  def incrby(conn, [key, increment]) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {value, expire_time} = result
 
      if is_nil(value) do
        count = make_sure_is_int(increment)

        {set_status, set_result} = set(conn, [key, count])
        if set_status === :ok do
          {:ok, count}
        else
          {set_status, set_result}
        end
      else
        updated_count =
          make_sure_is_int(value) + make_sure_is_int(increment)

        :ets.update_element(
          conn,
          key,
          {2, {updated_count, expire_time}}
        )
        {:ok, updated_count}
      end
    else
      {status, result}
    end
  end

  def decr(conn, [key | _tail]), do: decr(conn, key)

  def decr(conn, key) do
    decrby(conn, [key, 1])
  end

  def decrby(conn, [key, decrement]) do
    incrby(conn, [key, -make_sure_is_int(decrement)])
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


  # thread unsafe
  def append(conn, [key, append_value]) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {original_value, expire_time} = result
      if is_nil(original_value) do
        {set_status, set_result} = set(conn, [key, append_value])
        if set_status === :ok do
          {:ok, String.length(append_value)}
        else
          {set_status, set_result}
        end
      else
        new_value = original_value <> append_value
        :ets.update_element(
          conn,
          key,
          {2, {new_value, expire_time}}
        )
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
        {:ok, String.slice(result, start_index..end_index)}
      end
    else
      {status, result}
    end
  end

  # thread unsafe
  def setrange(conn, [key, offset, addition]) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {value, expire_time} = result
      addition_length = String.length(addition)

      if is_nil(value) do
        new_value = String.pad_leading(
          addition,
          offset + addition_length,
          <<0>>
        )
        {set_status, set_result} = set(conn, [key, new_value])
        if set_status === :ok do
          {:ok, new_value}
        else
          {set_status, set_result}
        end
      else
        initial_length = String.length(value)

        updated_value = if initial_length > offset do
          String.slice(value, 0..(offset - 1)) <> addition <>
            String.slice(value, (offset + addition_length)..-1)
        else
          value <>
            String.pad_leading(
              addition,
              offset - initial_length + addition_length,
              <<0>>
            )
        end
        :ets.update_element(conn, key, {2, {updated_value, expire_time}})
        {:ok, updated_value}
      end
    else
      {status, result}
    end
  end




  defp to_untupled_list(input, result \\ [])
  defp to_untupled_list([], result), do: Enum.reverse(result)
  defp to_untupled_list([{key, value} | tail], result) do
    to_untupled_list(tail, [value, key | result])
  end

  defp to_untupled_list(initial_map, result) when is_map(initial_map) do
    initial_map |> Map.to_list |> to_untupled_list(result)
  end

  def hget(conn, [hash_key, element_key]) do
    {status, result} = get(conn, hash_key)
    if status === :ok do
      if is_nil(result) do
        {:ok, nil}
      else
        if is_nil(result[element_key]) and is_bitstring(element_key) do
          {:ok, result[String.to_atom(element_key)]}
        else
          {:ok, result[element_key]}
        end
      end
    else
      {status, result}
    end
  end

  defp hmget(_conn, [], _hash_key, return_array) do
    {:ok, Enum.reverse(return_array)}
  end

  defp hmget(conn, [next_subkey | remaining_subkeys], hash_key, return_array) do
    {status, result} = hget(conn, [hash_key, next_subkey])
    if status === :ok do
      hmget(conn, remaining_subkeys, hash_key, [result | return_array])
    else
      {status, result}
    end
  end

  def hmget(conn, [hash_key | subkeys]) do
    hmget(conn, subkeys, hash_key, [])
  end

  def hgetall(conn, [key | _tail]), do: hgetall(conn, key)

  def hgetall(conn, key) do
    {status, result} = get(conn, key)
    if status === :ok do
      if is_nil(result) do
        {:ok, []}
      else
        {:ok, to_untupled_list(result)}
      end
    else
      {status, result}
    end
  end

  def hkeys(conn, [key | _tail]), do: hkeys(conn, key)

  def hkeys(conn, key) do
    {status, result} = get(conn, key)
    if status === :ok do
      if is_map(result) do
        {:ok, Map.keys(result)}
      else
        {:ok, []}
      end
    else
      {status, result}
    end
  end

  def hvals(conn, [key | _tail]), do: hvals(conn, key)

  def hvals(conn, key) do
    {status, result} = get(conn, key)
    if status === :ok do
      if is_map(result) do
        {:ok, Map.values(result)}
      else
        {:ok, []}
      end
    else
      {status, result}
    end
  end

  def hexists(conn, [hash_key, element_key]) do
    {status, result} = hkeys(conn, hash_key)
    if status === :ok do
      {status, bool_to_int(element_key in result)}
    else
      {status, result}
    end
  end

  def hlen(conn, [key | _tail]), do: hlen(conn, key)

  def hlen(conn, key) do
    {status, result} = hkeys(conn, key)
    if status === :ok do
      {status, length(result)}
    else
      {status, result}
    end
  end


  defp popall(starting_map, keys_to_pop, popped_values \\ [])

  defp popall(ending_map, [], popped_values) do
    {popped_values, ending_map}
  end

  defp popall(starting_map, [next_key | remaining_keys], popped_values) do
    key_exists = Map.has_key?(starting_map, next_key)
    {value, remaining_map} = Map.pop(starting_map, next_key)

    updated_values = if key_exists do
      [value | popped_values]
    else
      popped_values
    end
    popall(remaining_map, remaining_keys, updated_values)
  end

  # thread unsafe
  def hdel(conn, [hash_key | element_keys]) do
    {status, result} = get_with_exp(conn, hash_key)

    if status === :ok do
      {value, expire_time} = result

      if is_nil(value) do
        {:ok, 0}
      else
        {popped_elements, updated_hash} = popall(value, element_keys)
        :ets.update_element(
          conn,
          hash_key,
          {2, {updated_hash, expire_time}}
        )
        {:ok, length(popped_elements)}
      end
    else
      {status, result}
    end
  end

  # thread unsafe
  def hset(conn, [hash_key, element_key, element_value], nx \\ false) do
    {status, result} = get_with_exp(conn, hash_key)
    if status === :ok do
      {value, expire_time} = result

      if is_nil(value) do
        {set_status, set_value} = set(
          conn,
          [hash_key, %{element_key => element_value}]
        )
        if set_status === :ok do
          {:ok, 1}
        else
          {set_status, set_value}
        end

      else
        key_exists = Map.has_key?(value, element_key)
        unless key_exists and nx do
          updated_map = Map.put(value, element_key, element_value)
          :ets.update_element(
            conn,
            hash_key,
            {2, {updated_map, expire_time}}
          )
        end
        {:ok, bool_to_int(!key_exists)}
      end
    else
      {status, result}
    end
  end

  def hsetnx(conn, [hash_key, element_key, element_value]) do
    hset(conn, [hash_key, element_key, element_value], true)
  end

  # thread unsafe
  def hincrby(conn, [hash_key, element_key, increment]) do
    {status, result} = get_with_exp(conn, hash_key)
    if status === :ok do
      {value, expire_time} = result

      if is_nil(value) do
        {set_status, set_value} = set(
          conn,
          [hash_key, %{element_key => increment}]
        )
        if set_status === :ok do
          {:ok, increment}
        else
          {set_status, set_value}
        end
      else
        updated_count = 
          Map.get(value, element_key, 0) + make_sure_is_int(increment)
        updated_map = Map.put(value, element_key, updated_count)

        :ets.update_element(
          conn,
          hash_key,
          {2, {updated_map, expire_time}}
        )
        {:ok, updated_count}
      end
    else
      {status, result}
    end
  end


  defp lpushall([], final_array), do: final_array

  defp lpushall([next_value | remaining_values], target_array) do
    target_array = if(is_nil(target_array), do: [], else: target_array)
    lpushall(remaining_values, [next_value | target_array])
  end

  defp rpushall(pushed_array, target_array) do
    target_array ++ pushed_array
  end

  # thread unsafe
  def push(conn, [key | new_values], xx \\ false, pushall_func) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {original_array, expire_time} = result

      if is_nil(original_array) do
        if xx do
          {status, 0}
        else
          set_array = pushall_func.(new_values, [])
          {set_status, set_value} = set(conn, [key, set_array])
          if set_status === :ok do
            {:ok, length(set_array)}
          else
            {set_status, set_value}
          end
        end
      else
        updated_array = pushall_func.(new_values, original_array)

        :ets.update_element(
          conn,
          key,
          {2, {updated_array, expire_time}}
        )
        {status, length(updated_array)}
      end
    else
      {status, result}
    end
  end

  def lpush(conn, command_args, xx \\ false) do
    push(conn, command_args, xx, &lpushall/2)
  end

  def lpushx(conn, command_args), do: lpush(conn, command_args, true)

  # thread unsafe
  def rpush(conn, command_args, xx \\ false) do
    push(conn, command_args, xx, &rpushall/2)
  end

  def rpushx(conn, command_args), do: rpush(conn, command_args, true)


  def llen(conn, [key | _tail]), do: llen(conn, key)

  # thread unsafe
  def llen(conn, key) do
    {status, result} = get(conn, key)
    if status === :ok do
      if !is_nil(result) and !is_list(result) and !(is_tuple(result)) do
        {:error, "llen only applies to lists and tuples"}
      else
        {status, length(if(is_nil(result), do: [], else: result))}
      end
    else
      {status, result}
    end   
  end

  def lpop(conn, [key | _tail]), do: lpop(conn, key)

  # thread unsafe
  def lpop(conn, key) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {value, expire_time} = result

      if is_nil(value) or value === [] do
        {status, nil}
      else
        [return_val | updated_array] = value
        :ets.update_element(
          conn,
          key,
          {2, {updated_array, expire_time}}
        )
        {status, return_val}
      end
    else
      {status, result}
    end
  end

  def rpop(conn, [key | _tail]), do: rpop(conn, key)

  # thread unsafe
  def rpop(conn, key) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {value, expire_time} = result

      if is_nil(value) or value === [] do
        {status, nil}
      else
        # when elixir 1.4 is stable, use pop_at instead
        last_item = Enum.at(value, -1)

        :ets.update_element(
          conn,
          key,
          {2, {List.delete_at(value, -1), expire_time}}
        )
        {status, last_item}
      end
    else
      {status, result}
    end
  end

  # thread unsafe
  def rpoplpush(conn, [pop_key, push_key]) do
    {pop_status, pop_result} = rpop(conn, pop_key)
    if pop_status === :ok do
      if is_nil(pop_result) do
        {pop_status, pop_result}
      else
        {push_status, push_result} = lpush(conn, [push_key, pop_result])
        if push_status === :ok do
          {:ok, pop_result}
        else
          {push_status, push_result}
        end
      end
    else
      {pop_status, pop_result}
    end
  end


  # thread unsafe
  def lset(conn, [key, index, value]) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {starting_array, expire_time} = result

      :ets.update_element(
        conn,
        key,
        {2, {List.replace_at(starting_array, index, value), expire_time}}
      )
      {:ok, "OK"}
    else
      {status, result}
    end
  end


  def lindex(conn, [key, index]) do
    {status, result} = get(conn, key)
    if status === :ok do
      {status, Enum.at(result, index)}
    else
      {status, result}
    end
  end

  # thread unsafe
  def linsert(conn, [key, before_or_after, pivot, value]) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {starting_list, expire_time} = result

      pivot_index = Enum.find_index(starting_list, fn (element) -> element === pivot end)

      if is_nil(pivot_index) do
        {:ok, -1}
      else
        insert_index = if(before_or_after == "AFTER", do: pivot_index + 1, else: pivot_index)
        updated_list = List.insert_at(starting_list, insert_index, value)
        :ets.update_element(conn, key, {2, {updated_list, expire_time}})
        {:ok, length(updated_list)}
      end
    else
      {status, result}
    end
  end

  # thread unsafe
  def ltrim(conn, [key, start_index, end_index]) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {starting_list, expire_time} = result

      :ets.update_element(
        conn,
        key,
        {
          2,
          {Enum.slice(starting_list, start_index..end_index), expire_time}
        }
      )
      {:ok, "OK"}
    else
      {status, result}
    end
  end

  # thread unsafe
  def lrem(conn, [key, count, term]) do
    {status, result} = get_with_exp(conn, key)
    if status === :ok do
      {starting_list, expire_time} = result


      if is_nil(starting_list) do
        {status, 0}
      else
        # for negative counts passed, we want to move from left to right
        # so in that case we'll reverse the list before and after our filter
        reverse_if_negcount = fn (lst, cnt) ->
          if(cnt < 0, do: Enum.reverse(lst), else: lst)
        end

        starting_list = reverse_if_negcount.(starting_list, count)
        
        {pared_list, return_count} = Enum.flat_map_reduce(
          starting_list,
          0,
          fn (element, accumulator) ->
            # if the count passed is zero, we want to iterate through
            # the whole list, removing every match
            # otherwise, we'll count down from the absolute value of our
            # count and only remove matches before we hit zero
            if (count === 0 or accumulator < abs(count)) and (element === term) do
              {[], accumulator + 1}
            else
              {[element], accumulator}
            end
          end
        )
        final_list = reverse_if_negcount.(pared_list, count)

        :ets.update_element(conn, key, {2, {final_list, expire_time}})
        {status, return_count}
      end
    else
      {status, result}
    end   
  end
end
