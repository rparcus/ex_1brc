defmodule RuntimeCompiled do
  @moduledoc """
  Many ideas from what has been tried at https://github.com/gunnarmorling/1brc/discussions/93
  Special mentions to @jesperes and @onno-vos-dev for their erlang contributions, I have learned a lot from them.

  Ofc, thanks to @IceDragon200 who started the whole conversation and has been running the benchmarks.

  The main new idea here is to generate a splitter based on the input file at run time, so we
  end up with something like

  def split_lines(<<"city1", ?;, ...>>) ...
  def split_lines(<<"city2", ?;, ...>>) ...
  ...

  This is a very naive implementation, but it's already faster than the previous ones I had.
  I'm sure that there is room for improvement in the parser but I see no point in working on it now.
  At any given time, on my machine the producer process has ~16 messages in the queue, so improvements should probably happen there instead...


  Note: The `Code.compile_string` part is quite cursed, I do not reccomend anyone to actually do this in real life.
  Note2: It was more fun to code than an average macro, though.
  """
  @measurement_file "./data/measurements.txt"

  @workers_count :erlang.system_info(:schedulers_online)
  @chunk_size 1024 * 1024 * 8

  defmodule CityMatcher do
    @moduledoc """
    This module must be re-compiled at runtime, based on the input file.
    The functions are generated based on the cities actually found.
    """
    def process_lines(_), do: raise("Not implemented")
    def idx(_), do: raise("Not implemented")
  end

  def run() do
    # Dinamically compile the CityMatcher to pattern match the n cities 1:1 and simply use 1..n as indexes.
    # takes ~360ms
    [{__MODULE__.CityMatcher, _}] = build_index()

    workers = spawn_workers()

    {:ok, file} = :prim_file.open(@measurement_file, [:read])
    :ok = suply_chunks(<<>>, file)

    Enum.each(workers, fn _worker ->
      receive do
        {:demand, worker} ->
          send(worker, :stop)
      end
    end)

    gather_results()
    |> display()
  end

  def suply_chunks(prev, file, new_line \\ :binary.compile_pattern(<<?\n>>))

  # Handling the first chunk separately
  # this can work only if the first chunk is big enough
  def suply_chunks(<<>>, file, new_line) do
    receive do
      {:demand, worker} ->
        case :prim_file.read(file, @chunk_size) do
          :eof ->
            raise "Unexpected EOF on first read"

          {:ok, chunk} ->
            # This is way faster than using :prim_file.read_line on every chunk.
            # Taken from @jesperes' solution
            # The very first chunk contains only one line, but from the second chunk on
            # this method ensures that we're always sending complete lines to the workers
            [current, next] = :binary.split(chunk, new_line)
            send(worker, {:suply, <<current::binary, ?\n>>})
            suply_chunks(next, file, new_line)
        end
    end
  end

  def suply_chunks(prev, file, new_line) do
    # IO.inspect Process.info(self()) |> Keyword.get(:message_queue_len)
    receive do
      {:demand, worker} ->
        case :prim_file.read(file, @chunk_size) do
          :eof ->
            send(worker, {:suply, prev})
            :ok

          {:ok, chunk} ->
            case :binary.split(chunk, new_line) do
              [current, next] ->
                send(worker, {:suply, <<prev::binary, current::binary, ?\n>>})
                suply_chunks(next, file, new_line)

              # Last chunk
              [current] ->
                send(worker, {:suply, <<prev::binary, current::binary, ?\n>>})
                :ok
            end
        end
    end
  end

  def gather_results() do
    Enum.reduce(1..@workers_count, %{}, fn _, acc ->
      receive do
        {:results, result} ->
          result
          |> Enum.reduce(acc, fn {ws, {rmin, rsum, rmax, rcount} = row}, acc ->
            case Map.fetch(acc, ws) do
              :error ->
                Map.put(acc, ws, row)

              {:ok, {rmin2, rsum2, rmax2, rcount2}} ->
                Map.put(
                  acc,
                  ws,
                  {min(rmin, rmin2), rsum + rsum2, max(rmax, rmax2), rcount + rcount2}
                )
            end
          end)
      end
    end)
    |> Enum.sort(fn {ws1, _}, {ws2, _} -> ws1 < ws2 end)
  end

  def spawn_workers do
    parent = self()

    for _ <- 1..@workers_count do
      spawn_link(fn ->
        worker_loop(parent)
      end)
    end
  end

  def worker_loop(parent) do
    send(parent, {:demand, self()})

    receive do
      {:suply, chunk} ->
        :ok = CityMatcher.process_lines(chunk)
        worker_loop(parent)

      :stop ->
        send(parent, {:results, :erlang.get()})
    end
  end

  def build_index() do
    {:ok, file} = :prim_file.open(@measurement_file, [:read])

    try do
      {:ok, bin} = :prim_file.read(file, 1024 * 1024 * 2)

      {match_funs, idx_funs} =
        city_extractor(bin, [])
        |> Enum.uniq()
        |> Enum.sort(fn a, b -> byte_size(a) < byte_size(b) and a < b end)
        |> Enum.with_index()
        |> Enum.map(fn {city, idx} ->
          city_chars = String.to_charlist(city)

          match_funs = """
          def process_lines(<<"#{city_chars}", ?;, rest::binary>>) do
            get_temp(rest, #{idx})
          end
          """

          idx_fun = "def idx(#{idx}), do: \"#{city}\""

          {match_funs, idx_fun}
        end)
        |> Enum.unzip()

      new_city_matcher =
        """
        defmodule #{__MODULE__}.CityMatcher do
          @compile {:inline, idx: 1}
          defmacrop store_temp_macro(idx, temp) do
            quote do
              case :erlang.get(unquote(idx)) do
                :undefined ->
                  :erlang.put(unquote(idx), {unquote(temp), unquote(temp), unquote(temp), 1})

                {current_min, current_sum, current_max, count} ->
                  :erlang.put(
                    unquote(idx),
                    {min(current_min, unquote(temp)), current_sum + unquote(temp), max(current_max, unquote(temp)), count + 1}
                  )
              end
            end
          end

          defp get_temp(<<?-, c1, c2, ?., c3, ?\\n, rest::binary>>, idx) do
            temp = -1 * ((c1 - ?0) * 100 + (c2 - ?0) * 10 + (c3 - ?0))
            store_temp_macro(idx, temp)

            process_lines(rest)
          end
          defp get_temp(<<?-, c2, ?., c3, ?\\n, rest::binary>>, idx) do
            temp = -1 * ((c2 - ?0) * 10 + (c3 - ?0))
            store_temp_macro(idx, temp)

            process_lines(rest)
          end
          defp get_temp(<<c1, c2, ?., c3, ?\\n, rest::binary>>, idx) do
            temp = (c1 - ?0) * 100 + (c2 - ?0) * 10 + (c3 - ?0)
            store_temp_macro(idx, temp)

            process_lines(rest)
          end
          defp get_temp(<<c2, ?., c3, ?\\n, rest::binary>>, idx) do
            temp = (c2 - ?0) * 10 + (c3 - ?0)
            store_temp_macro(idx, temp)

            process_lines(rest)
          end

          #{Enum.join(match_funs, "\n")}
          def process_lines(<<>>), do: :ok
          #{Enum.join(idx_funs, "\n")}
          def idx(_), do: raise "Index not found"
        end
        """

      # |> IO.inspect(printable_limit: :infinity)

      Code.compile_string(new_city_matcher)
    after
      :prim_file.close(file)
    end
  end

  for n <- 1..100, m <- [24, 32, 40] do
    n_bytes = n * 8

    def city_extractor(<<city::unquote(n_bytes), ?;, _temp::unquote(m), ?\n, rest::binary>>, acc) do
      city_extractor(rest, [<<city::unquote(n_bytes)>> | acc])
    end
  end

  def city_extractor(_finished_the_chunk, acc), do: acc

  defp display(results) do
    [
      "{",
      results
      |> Enum.map(fn {ws, {min, sum, max, count}} ->
        [
          "#{__MODULE__.CityMatcher.idx(ws)}",
          "=",
          :erlang.float_to_binary(min / 10, decimals: 1),
          "/",
          :erlang.float_to_binary(sum / count / 10, decimals: 1),
          "/",
          :erlang.float_to_binary(max / 10, decimals: 1)
        ]
      end)
      |> Enum.intersperse(", "),
      "}"
    ]
    |> IO.puts()
  end
end
