defmodule Tortoise311.Connection.Telemetry do
  @moduledoc """
  Per-connection telemetry capture.

  [:tortoise311, :connection, :tx_bytes] => Number of bytes sent by the connection identified by its client id
  [:tortoise311, :connection, :rx_bytes] => Number of bytes received by the connection identified by its client id

  Meta-data: %{client_id: <client_id>}
  """

  use GenServer

  alias __MODULE__, as: State

  require Logger

  @telemetry_delay 30_000

  @enforce_keys [:client_id, :socket_stats]
  defstruct [:client_id, :socket_stats]

  def start_link(opts \\ []) do
    client_id = Keyword.fetch!(opts, :client_id)

    init_state = %State{
      client_id: client_id,
      socket_stats: %{socket: nil, last_stats: []}
    }

    GenServer.start_link(__MODULE__, init_state, name: via_name(client_id))
  end

  @impl GenServer
  def init(state) do
    Process.send_after(self(), :capture_telemetry, @telemetry_delay)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:capture_telemetry, state) do
    Process.send_after(self(), :capture_telemetry, @telemetry_delay)
    {:noreply, capture_telemetry(state)}
  end

  @impl GenServer
  def terminate(_reason, state) do
    capture_telemetry(state)
  end

  defp via_name(client_id) do
    Tortoise311.Registry.via_name(__MODULE__, client_id)
  end

  defp capture_telemetry(state) do
    client_id = state.client_id
    registered_name = Tortoise311.Registry.via_name(Tortoise311.Connection, client_id)

    case Tortoise311.Registry.meta(registered_name) do
      {:ok, {transport, socket}} ->
        case transport.getstat(socket) do
          {:ok, stats} ->
            {received, sent} = new_stats(stats, state.socket_stats, socket)

            :telemetry.execute(
              [:tortoise311, :connection],
              %{
                rx_bytes: received,
                tx_bytes: sent
              },
              %{client_id: client_id}
            )

            %State{state | socket_stats: %{socket: socket, last_stats: stats}}

          other ->
            Logger.warning("[Tortoise311] Failed to get socket stats: #{inspect(other)}")
            state
        end

      other ->
        Logger.warning("[Tortoise311] Failed to capture telemetry on socket: #{inspect(other)}")
        state
    end
  end

  defp new_stats(stats, socket_stats, socket) do
    if socket == socket_stats.socket do
      {stats[:recv_oct] - Keyword.get(socket_stats.last_stats, :recv_oct, 0),
       stats[:send_oct] - Keyword.get(socket_stats.last_stats, :send_oct, 0)}
    else
      {stats[:recv_oct], stats[:send_oct]}
    end
  end
end
