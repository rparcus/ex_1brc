defmodule NormalParser do
  def run() do
    filename = "./data/measurements.txt"
    parent = self()

    comma = :binary.compile_pattern(";")

    File.stream!(filename, [:line, read_ahead: 10])
    |> Flow.from_enumerable()
    |> Flow.map(fn str ->
      [wx, temp] = String.split(str, comma)
      {wx, temp |> Parsers.binary_to_fixed_point()}
    end)
    |> Flow.partition(key: fn {ws, _} -> ws end, window: Flow.Window.global())
    # ETS
    |> Flow.reduce(fn -> :ets.new(NormalParser, []) end, &reducer/2)
    |> Flow.on_trigger(fn ets ->
      :ets.give_away(ets, parent, [])

      # Emit the ETS
      {[ets], :new_reduce_state_which_wont_be_used}
    end)
    # |> Enum.to_list()
    |> Enum.flat_map(fn ref ->
      :ets.tab2list(ref)
    end)
    |> Enum.sort(fn {ws1, _}, {ws2, _} -> ws1 < ws2 end)
    |> Enum.map(fn {ws, {min, mean, max, count}} ->
      "#{ws}=#{min / 10}/#{:erlang.float_to_binary(mean / count, decimals: 2)}/#{max / 10}"
    end)
  end

  def reducer({ws, temp}, ets) do
    case :ets.lookup(ets, ws) do
      [] ->
        :ets.insert(ets, {ws, {temp, temp, temp, 1}})

      [{_, {current_min, current_mean, current_max, count}}] ->
        :ets.insert(
          ets,
          {ws, {min(current_min, temp), current_mean + temp, max(current_max, temp), count + 1}}
          # {ws, {min(current_min, temp), (current_mean + temp) / 2, max(current_max, temp)}}
        )
    end

    ets
  end
end
