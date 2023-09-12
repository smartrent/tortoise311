defmodule Tortoise311.SimpleBroker do
  @moduledoc """
  A simple MQTT broker that can be used for testing and development

  > #### Warning {: .warning}
  >
  > This is not indended for production use and makes no attempts for
  > large connection scaling and handling. It is also not considered
  > feature complete, but handles most simple use cases.

  This currently only supports simple, unencrypted TCP connections. You
  can specify custom `:gen_tcp` options in `:overrides` key when starting
  the server.

  ## Rule forwarding

  Many production setups will have a few topics with rules that forward
  messages to a handler such as an SQS queue or central service. This allows
  for scaling of many devices to communicate with a few nodes. For testing,
  you can specify rules when starting the broker which will forward
  `Tortoise311.Package.Publish` structs to a handler function or process in
  order to mimic this rule forwarding behavior. See `add_rule/3` for more
  information.

  **Example:**

  ```
  handler = fn pub ->
    Broadway.test_message(MyBroadway, pub.payload)
  end

  Tortoise311.SimpleBroker.start_link(rules: [{"my/+/test/topic", handler}])
  ```
  """
  use GenServer

  alias Tortoise311.Package

  require Logger

  @type rule() ::
          {Tortoise311.topic_filter(),
           pid()
           | (Package.Publish.t() -> any())
           | (Tortoise311.topic(), Tortoise311.payload() -> any())}

  @type opt() ::
          {:port, :inet.port_number()}
          | {:name, GenServer.name()}
          | {:overrides, :gen_tcp.option()}
          | {:rules, [rule()]}

  @default_opts [mode: :binary, packet: :raw, active: true, reuseaddr: true]

  @doc false
  @spec start_link([opt()]) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name] || __MODULE__
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Add a topic filter rule with handler

  In many broker implementations, there are a few routing rules that forward
  messages from specific topics to a handler such as an SQS queue or central
  service in a fan-in pattern for scaling many remote devices to a few nodes.

  Many times the topic runs through a full SQL query before forwarding the
  message on. However, that is unsupported in this simple server. Instead
  you can provide a simple topic filter and a handler function or process
  which will receive the Tortoise311.Package.Publish struct that can be split
  apart by your logic to formulate into the end result needed (such as mimicing
  an SQS queue message in your producer)
  """
  @spec add_rule(GenServer.server(), Tortoise311.topic_filter(), pid() | function()) :: :ok
  def add_rule(server \\ __MODULE__, topic, handler)
      when is_pid(handler) or is_function(handler, 1) or is_function(handler, 2) do
    GenServer.call(server, {:add_rule, topic, handler})
  end

  @doc """
  Publish a message to a topic.
  """
  @spec publish(GenServer.server(), Tortoise311.topic(), Tortoise311.payload()) :: :ok
  def publish(server \\ __MODULE__, topic, payload) do
    GenServer.call(server, {:publish, topic, payload})
  end

  @impl GenServer
  def init(opts) do
    port = opts[:port] || 1883
    tcp_opts = Keyword.merge(@default_opts, opts[:overrides] || [])
    {:ok, listen} = :gen_tcp.listen(port, tcp_opts)

    broker = self()
    acceptor = spawn(fn -> accept(listen, broker) end)

    state = %{opts: opts, acceptor: acceptor, sockets: %{}, rules: opts[:rules] || []}

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:add_rule, filter, handler}, _from, state) do
    {:reply, :ok, update_in(state.rules, &[{filter, handler} | &1])}
  end

  def handle_call({:publish, topic, payload}, _from, state) do
    packet = %Package.Publish{topic: topic, payload: payload, qos: 0}
    handle_publish(packet, state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({:tcp, socket, data}, state) do
    case Package.decode(data) do
      %Package.Connect{} = conn ->
        send_package(socket, %Package.Connack{session_present: false, status: :accepted})

        {:noreply, update_in(state, [:sockets, socket], &Map.put(&1 || %{}, :conn, conn))}

      %Package.Subscribe{} = sub ->
        acks = for {_topic, qos} <- sub.topics, do: {:ok, qos}

        send_package(socket, %Package.Suback{identifier: sub.identifier, acks: acks})

        {:noreply,
         update_in(
           state,
           [:sockets, socket, :subscriptions],
           &Enum.uniq((&1 || []) ++ sub.topics)
         )}

      %Package.Publish{} = pub ->
        # Handle QoS 2?
        if pub.qos == 1, do: send_package(socket, %Package.Puback{identifier: pub.identifier})

        handle_publish(pub, state)
        {:noreply, state}

      %Package.Pingreq{} ->
        send_package(socket, %Package.Pingresp{})
        {:noreply, state}

      data ->
        Logger.debug(
          "[Tortoise311.SimpleBroker] Unhandled packet - #{inspect(data, limit: :infinity)}"
        )

        {:noreply, state}
    end
  catch
    _, _ ->
      Logger.debug(
        "[Tortoise311.SimpleBroker] Failed to parse package - #{inspect(data, limit: :infinity)}"
      )

      {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, state) do
    {deleted, sockets} = Map.pop(state.sockets, socket)
    state = %{state | sockets: sockets}

    with %{conn: %{will: %Package.Publish{} = last_will}} <- deleted do
      handle_publish(last_will, state)
    end

    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.debug("[Tortoise311.SimpleBroker] Got unknown message #{inspect(msg)}")
    {:noreply, state}
  end

  defp send_package(socket, package) do
    case :gen_tcp.send(socket, Package.encode(package)) do
      {:error, err} ->
        Logger.warning(
          "[Tortoise311.SimpleBroker] Failed to send package (#{inspect(err)}): #{inspect(package)}"
        )

      ok ->
        ok
    end
  end

  defp handle_publish(packet, state) do
    device_subs =
      for {socket, %{subscriptions: subs}} <- state.sockets,
          {filter, _qos} <- subs,
          do: {filter, socket}

    _ =
      for {filter, where} <- device_subs ++ state.rules,
          topic_match?(filter, packet.topic) do
        case where do
          p when is_port(p) -> :gen_tcp.send(where, Package.encode(packet))
          f when is_function(f, 1) -> f.(packet)
          f when is_function(f, 2) -> f.(packet.topic, packet.payload)
          p when is_pid(p) -> send(p, packet)
          _ -> Logger.debug("[Tortoise311.SimpleBroker] Unhandled topic #{inspect(packet.topic)}")
        end
      end

    :ok
  end

  # Everything matched between topic and filter
  defp topic_match?(filter, topic) when is_binary(filter) do
    topic_match?(String.split(filter, "/", trim: true), topic)
  end

  defp topic_match?(filter, topic) when is_binary(topic) do
    topic_match?(filter, String.split(topic, "/", trim: true))
  end

  defp topic_match?([], []), do: true
  defp topic_match?(matched, matched), do: true

  # These positions match, continue checking
  defp topic_match?([a | filter_rem], [a | topic_rem]), do: topic_match?(filter_rem, topic_rem)

  # The filter allows any single value in the topic
  defp topic_match?(["+" | filter_rem], [_ | topic_rem]),
    do: topic_match?(filter_rem, topic_rem)

  # Multi-level topic filter requires additional levels but there
  # are no more left in the topic, so it does not match
  defp topic_match?(["#" | _], []), do: false

  # If we have gotten here, all other parts have matched and this
  # indicates all other parts of the topic are allowed
  defp topic_match?(["#" | _], _), do: true

  defp topic_match?(_, _), do: false

  defp accept(listen, broker) do
    # This function is called recursively to accept new connections
    # and pass off control to the broker process
    with {:ok, socket} <- :gen_tcp.accept(listen),
         :ok <- :gen_tcp.controlling_process(socket, broker) do
      :ok
    end

    accept(listen, broker)
  end
end
