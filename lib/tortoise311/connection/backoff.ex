defmodule Tortoise311.Connection.Backoff do
  @moduledoc false

  alias __MODULE__, as: State

  defstruct min_interval: 100, max_interval: 30_000, jitter_percent: 0.5, value: nil

  @doc """
  Create an opaque data structure that describe a incremental
  back-off.
  """
  def new(opts) do
    min_interval = Keyword.get(opts, :min_interval, 100)
    max_interval = Keyword.get(opts, :max_interval, 30_000)
    jitter_percent = Keyword.get(opts, :jitter_percent, 0)

    %State{min_interval: min_interval, max_interval: max_interval, jitter_percent: jitter_percent}
  end

  def next(%State{value: nil} = state) do
    current = state.min_interval
    {jitter(current, state.jitter_percent), %State{state | value: current}}
  end

  def next(%State{} = state) do
    current = min(state.value * 2, state.max_interval)
    {jitter(current, state.jitter_percent), %State{state | value: current}}
  end

  def reset(%State{} = state) do
    %State{state | value: nil}
  end

  defp jitter(delay, percent) do
    round(delay * (1 - percent * (0.5 - :rand.uniform())))
  end
end
