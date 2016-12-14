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
      _ -> raise ArgumentError, "Can't match command"
    end
  end

  def set!(conn, command_args), do: command!(conn, ["SET" | command_args])
  def setnx!(conn, command_args), do: command!(conn, ["SETNX" | command_args])
  def setex!(conn, command_args), do: command!(conn, ["SETEX" | command_args])
  def psetex!(conn, command_args), do: command!(conn, ["PSETEX" | command_args])

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
        :ets.insert_new(conn, {key, {ttl, value}})
      "XX" in arg_keys ->
        # if the key is currently empty, lookup will return an empty list
        # so in the case of "XX" we don't want to set
        if :ets.lookup(conn, key) === [] do
          false
        else
          :ets.insert(conn, {key, {ttl, value}})
        end
      true ->
        :ets.insert(conn, {key, {ttl, value}})
    end
  end

  def set(conn, command_args) do
    [key | command_args] = command_args
    [value | command_args] = command_args
    extra_args = map_extra_args(command_args)
    set(conn, key, value, extra_args)
  end
 

  def setnx(conn, command_args), do: set(conn, command_args ++ ["NX"])


  def set_with_exp(conn, command_args, exp_key \\ "EX") do
    [key | command_args] = command_args
    [exp_val | command_args] = command_args
    [value | _remainder] = command_args
    set(conn, [key, value, exp_key, exp_val])
  end

  def setex(conn, command_args), do: set_with_exp(conn, command_args, "EX")
  def psetex(conn, command_args), do: set_with_exp(conn, command_args, "PX")

end
