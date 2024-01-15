defmodule Ex1brcTest do
  use ExUnit.Case
  doctest Ex1brc

  test "greets the world" do
    assert Ex1brc.hello() == :world
  end
end
