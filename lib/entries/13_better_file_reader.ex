defmodule BetterFileReader do
  @measurement_file "./data/measurements.txt"

  # takes @IceDragon worker logic and uses a hash to avoid aggregating at the end
  # https://github.com/IceDragon200/1brc_ex/commit/2931163e19fdd5658274569ac3ca92cd5c448f85

  def run do
    hash_space = :erlang.system_info(:logical_processors) * 8

    # creates n ets tables named 0..n
    tables =
      for hash_key <- 0..hash_space do
        :ets.new(int2atom(hash_key), [:public, :named_table])
      end

    parent = self()

    workers =
      Enum.map(1..hash_space, fn _ ->
        spawn_link(fn ->
          worker_main(parent, "", hash_space)
        end)
      end)

    {:ok, file} = :prim_file.open(@measurement_file, [:raw, :read])

    try do
      read_file(file, workers)
    after
      :prim_file.close(file)
    end

    tables
    |> Enum.flat_map(&:ets.tab2list(&1))
    |> Enum.sort(fn {ws1, _}, {ws2, _} -> ws1 < ws2 end)
    |> display()

    # cleanup
    Enum.each(tables, fn table ->
      # Uncomment IO.inspect to see how keys are well distributed across ets tables
      :ets.info(table, :memory) # |> IO.inspect(label: table)
      :ets.delete(table)
    end)
  end

  def worker_main(parent, <<>>, hash_space) do
    send(parent, {:checkin, self()})

    receive do
      :eos ->
        send(parent, {:result, :ok})
        :ok

      {:chunk, bin} ->
        worker_main(parent, bin, hash_space)
    end
  end

  def worker_main(parent, rest, hash_space) do
    {ws, temp, <<"\n", rest::binary>>} = parse_line(rest)
    # with this, results from a specific ws should go to the same ets table,
    # no matter what worker is processing the measurement
    hash_key = :erlang.phash2(ws, hash_space) |> int2atom()

    case :ets.lookup(hash_key, ws) do
      [] ->
        :ets.insert(hash_key, {ws, {temp, temp, temp, 1}})

      [{_, {current_min, current_sum, current_max, count}}] ->
        :ets.insert(
          hash_key,
          {ws, {min(current_min, temp), current_sum + temp, max(current_max, temp), count + 1}}
        )
    end

    worker_main(parent, rest, hash_space)
  end

  def read_file(file, workers) do
    :ok = do_read_file(file)

    Enum.map(workers, fn _worker ->
      receive do
        {:checkin, _} ->
          :ok
      end
    end)
  end

  def do_read_file(file) do
    case :prim_file.read(file, 0x1_000_000) do
      :eof ->
        :ok

      {:ok, bin} ->
        bin =
          case :prim_file.read_line(file) do
            # file is over
            :eof ->
              bin

            # append rest of line to bin so that we can parse it normally
            {:ok, line} ->
              <<bin::binary, line::binary>>
          end

        receive do
          {:checkin, worker} ->
            send(worker, {:chunk, bin})
        end

        do_read_file(file)
    end
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

  # A bunch of generated functions to parse binaries.
  @compile {:inline, parse_line: 1}
  defp parse_line(line) do
    {ws, temp_string} = split(line)
    {temp, rest} = temp2float(temp_string)
    {ws, temp, rest}
  end

  # parses numbers from -99.9 to 99.9 into integers.
  @compile {:inline, temp2float: 1}
  for int <- 0..99 do
    for dec <- 0..9 do
      str_int = Integer.to_string(int)
      str_dec = Integer.to_string(dec)
      the_match = "#{str_int}.#{str_dec}"
      fixed_point_value = int * 10 + dec

      defp temp2float(<<"-", unquote(the_match), rest::binary>>),
        do: {-unquote(fixed_point_value), rest}

      defp temp2float(<<unquote(the_match), rest::binary>>),
        do: {unquote(fixed_point_value), rest}
    end
  end

  # generates a function to split ws name from the rest of the line.
  # Names can be up to 100 characters long.
  @compile {:inline, split: 1}
  for x <- 1..100 do
    ws_length_in_bits = x * 8

    def split(<<ws::bitstring-size(unquote(ws_length_in_bits)), ";", temp::binary>>),
      do: {ws, temp}
  end

  @compile {:inline, int2atom: 1}
  # The range below should match 0..:erlang.system_info(:logical_processors) * 8
  for i <- 0..192 do
    res = :"#{i}"
    def int2atom(unquote(i)), do: unquote(res)
  end
end

# Ex1brc.run_with_timer([BetterFileReader])
