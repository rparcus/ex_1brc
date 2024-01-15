defmodule Parsers do
  # Generates a function that takes binary strings representing numbers from -99 to 99
  # and returns the corresponding integer. The numbers may be followed by a more binary information such as
  # a newline character which will be ignored.
  @compile {:inline, temp2float: 1, split: 1, char_to_num: 1}

  for int <- 0..99 do
    for dec <- 0..9 do
      str_int = Integer.to_string(int)
      str_dec = Integer.to_string(dec)
      the_match = "#{str_int}.#{str_dec}"
      float_val = String.to_float("#{str_int}.#{str_dec}")
      def temp2float(<<"-", unquote(the_match), _>>), do: -unquote(float_val)
      def temp2float(<<unquote(the_match), _>>), do: unquote(float_val)
      def temp2float(<<"-", unquote(the_match)>>), do: -unquote(float_val)
      def temp2float(unquote(the_match)), do: unquote(float_val)
    end
  end

  # Generates a function that takes binary strings and splits them in 2 parts:
  # - the first part can have max length of 100 bytes and is delimited by a semicolon.
  # - the second part is a float number that may be followed by a newline character.
  for x <- 1..100 do
    ws_length_in_bits = x * 8

    def split(<<ws::bitstring-size(unquote(ws_length_in_bits)), ";", temp::binary>>),
      do: {ws, temp}
  end

  def normal_split(str) do
    [ws, temp] = :binary.split(str, ";")
    {ws, temp}
  end

  def binary_to_fixed_point(<<?-, d2, d1, ?., d01>>) do
    -(char_to_num(d2) * 100 + char_to_num(d1) * 10 + char_to_num(d01))
  end

  def binary_to_fixed_point(<<?-, d1, ?., d01>>) do
    -(char_to_num(d1) * 10 + char_to_num(d01))
  end

  def binary_to_fixed_point(<<d2, d1, ?., d01>>) do
    char_to_num(d2) * 100 + char_to_num(d1) * 10 + char_to_num(d01)
  end

  def binary_to_fixed_point(<<d1, ?., d01>>) do
    char_to_num(d1) * 10 + char_to_num(d01)
  end

  def binary_to_fixed_point(str), do: String.trim(str) |> binary_to_fixed_point()

  defp char_to_num(?0), do: 0
  defp char_to_num(?1), do: 1
  defp char_to_num(?2), do: 2
  defp char_to_num(?3), do: 3
  defp char_to_num(?4), do: 4
  defp char_to_num(?5), do: 5
  defp char_to_num(?6), do: 6
  defp char_to_num(?7), do: 7
  defp char_to_num(?8), do: 8
  defp char_to_num(?9), do: 9
end

# Parsers.temp2float("12.0\n")
# Parsers.temp2float("12.2")
# Parsers.temp2float("-12.2\n")
# Parsers.split("Petropavlovsk-Kamchatsky;76.8")
# Parsers.split("Iqaluit;-68.2\n")
