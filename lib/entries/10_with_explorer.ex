defmodule WithExplorer do
  # Eager Results
  # [
  # 1_000_000_000: 675483.000ms,
  #   500_000_000: 58244.713ms,
  #   100_000_000: 10321.046ms,
  #    50_000_000: 5104.949ms,
  # ]
  # Lazy Results
  # [
  # 1_000_000_000: 389491.009ms,
  #    50_000_000: 5061.05ms,
  # ]

  # Lazy + f32 Results
  # [
  # 1_000_000_000: 53575.235ms,
  #    50_000_000: 1849.05ms,
  # ]
  require Explorer.DataFrame
  alias Explorer.{DataFrame, Series}

  @filename "./data/measurements.txt"

  def run() do
    csv_options = [
      header: false,
      delimiter: ";",
      eol_delimiter: "\n",
      # lazy: true,
      dtypes: [column_1: :category, column_2: {:f, 32}]
    ]

    results =
      @filename
      |> DataFrame.from_csv!(csv_options)
      |> DataFrame.group_by("column_1")
      |> DataFrame.summarise(
        min: Series.min(column_2),
        mean: Series.mean(column_2),
        max: Series.max(column_2)
      )
      |> DataFrame.arrange(column_1)

    # |> DataFrame.collect()

    for idx <- 0..(results["column_1"] |> Series.to_list() |> length() |> Kernel.-(1)) do
      min = :erlang.float_to_binary(results["min"][idx], decimals: 2)
      mean = :erlang.float_to_binary(results["mean"][idx], decimals: 2)
      max = :erlang.float_to_binary(results["max"][idx], decimals: 2)
      "#{results["column_1"][idx]}=#{min}/#{mean}/#{max}"
    end
  end
end
