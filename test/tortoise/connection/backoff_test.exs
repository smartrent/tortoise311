defmodule Tortoise311.Connection.BackoffTest do
  use ExUnit.Case, async: true
  doctest Tortoise311.Connection.Backoff

  alias Tortoise311.Connection.Backoff

  test "should not exceed maximum interval time" do
    min = 100
    max = 300
    backoff = Backoff.new(min_interval: 100, max_interval: 300, jitter_percent: 0)
    assert {^min, backoff} = Backoff.next(backoff)
    {_, backoff} = Backoff.next(backoff)
    assert {^max, _} = Backoff.next(backoff)
  end

  test "reset" do
    backoff = Backoff.new(min_interval: 10, jitter_percent: 0)
    assert {_, backoff = snapshot} = Backoff.next(backoff)
    assert {_, backoff} = Backoff.next(backoff)
    assert %Backoff{} = backoff = Backoff.reset(backoff)
    assert {_, ^snapshot} = Backoff.next(backoff)
  end

  test "jitters backoff" do
    backoff = Backoff.new(min_interval: 100, max_interval: 100, jitter_percent: 0.5)

    {result, _backoff} =
      Enum.reduce(1..1000, {[], backoff}, fn _, {acc, b} ->
        {delay, b} = Backoff.next(b)
        {[delay | acc], b}
      end)

    mean = Enum.sum(result) / length(result)
    assert_in_delta mean, 100, 2

    unique_values = Enum.count(Enum.frequencies(result))
    assert unique_values > 1
  end
end
