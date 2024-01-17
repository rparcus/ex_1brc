defmodule JustElixir do
  @measurement_file "./data/measurements.txt"

  def run() do
    hash_space = System.schedulers_online() * 5

    tables =
      for hash_key <- 0..hash_space do
        :ets.new(int2atom(hash_key), [:public, :named_table])
      end

    File.stream!(@measurement_file, :line)
    |> Stream.chunk_every(50_000)
    |> Task.async_stream(
      fn lines ->
        for line <- lines do
          {ws, temp} = parse_line(line)
          hash_key = :erlang.phash2(ws, hash_space) |> int2atom()

          case :ets.lookup(hash_key, ws) do
            [] ->
              :ets.insert(hash_key, {ws, {temp, temp, temp, 1}})

            [{_, {current_min, current_sum, current_max, count}}] ->
              :ets.insert(
                hash_key,
                {ws,
                 {min(current_min, temp), current_sum + temp, max(current_max, temp), count + 1}}
              )
          end
        end

        :ok
      end,
      max_concurrency: hash_space,
      ordered: false
    )
    |> Stream.run()

    tables
    |> Enum.flat_map(&:ets.tab2list(&1))
    |> Enum.sort(fn {ws1, _}, {ws2, _} -> ws1 < ws2 end)
    |> display()

    # cleanup
    Enum.each(tables, fn table ->
      :ets.info(table, :memory) |> IO.inspect(label: table)
      :ets.delete(table)
    end)
  end

  defp display(results) do
    [
      "{",
      results
      |> Enum.map(fn {ws, {min, sum, max, count}} ->
        [
          ws,
          "=",
          :erlang.float_to_binary(min / 10, decimals: 1),
          "/",
          :erlang.float_to_binary(sum / count, decimals: 1),
          "/",
          :erlang.float_to_binary(max / 10, decimals: 1)
        ]
      end)
      |> Enum.intersperse(", "),
      "}"
    ]
    |> IO.puts()
  end

  @compile {:inline, parse_line: 1}
  defp parse_line(line) do
    {ws, temp_string} = split(line)
    temp = temp2float(temp_string)
    {ws, temp}
  end

  @compile {:inline, temp2float: 1}
  for int <- 0..99 do
    for dec <- 0..9 do
      str_int = Integer.to_string(int)
      str_dec = Integer.to_string(dec)
      the_match = "#{str_int}.#{str_dec}"
      fixed_point_value = int * 10 + dec
      defp temp2float(<<"-", unquote(the_match), _>>), do: -unquote(fixed_point_value)
      defp temp2float(<<unquote(the_match), _>>), do: unquote(fixed_point_value)
      defp temp2float(<<"-", unquote(the_match)>>), do: -unquote(fixed_point_value)
      defp temp2float(unquote(the_match)), do: unquote(fixed_point_value)
    end
  end

  # Generates a function that takes binary strings and splits them in 2 parts:
  # - the first part can have max length of 100 bytes and is delimited by a semicolon.
  # - the second part is a float number that may be followed by a newline character.
  @compile {:inline, split: 1}
  for x <- 1..100 do
    ws_length_in_bits = x * 8

    def split(<<ws::bitstring-size(unquote(ws_length_in_bits)), ";", temp::binary>>),
      do: {ws, temp}
  end

  @compile {:inline, int2atom: 1}
  for i <- 0..128 do
    res = :"#{i}"
    def int2atom(unquote(i)), do: unquote(res)
  end
end
