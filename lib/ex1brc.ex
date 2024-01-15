defmodule Ex1brc do
  @moduledoc """
  Documentation for `Ex1brc`.
  """

  @entries [
    WithExplorer
    # NormalParser,
  ]

  def run_with_timer(entries \\ @entries) do
    Enum.map(entries, fn entry ->
      {time, result} = :timer.tc(entry, :run, [])
      {entry, time / 1000, result}
    end)
  end
end
