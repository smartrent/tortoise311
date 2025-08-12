defmodule Tortoise311.Connection do
  @moduledoc """
  Establish a connection to a MQTT broker.

  Todo.
  """

  use GenServer

  alias __MODULE__, as: State
  alias Tortoise311.Connection.{Backoff, Controller, Inflight, Receiver}
  alias Tortoise311.Package.{Connack, Connect}
  alias Tortoise311.{Connection, Events, Handler, Package, Transport}

  require Logger

  defstruct [
    :client_id,
    :connect,
    :server,
    :status,
    :backoff,
    :subscriptions,
    :keep_alive,
    :keep_alive_timeout,
    :opts,
    :handler
  ]

  @doc """
  Start a connection process and link it to the current process.

  Read the documentation on `child_spec/1` if you want... (todo!)

  """
  @spec start_link(options, GenServer.options()) :: GenServer.on_start()
        when option:
               {:client_id, Tortoise311.client_id()}
               | {:server, {atom(), term()}}
               | {:user_name, String.t()}
               | {:password, String.t()}
               | {:keep_alive, non_neg_integer()}
               | {:keep_alive_timeout, non_neg_integer()}
               | {:will, Tortoise311.Package.Publish.t()}
               | {:subscriptions,
                  [{Tortoise311.topic_filter(), Tortoise311.qos()}]
                  | Tortoise311.Package.Subscribe.t()}
               | {:clean_session, boolean()}
               | {:enable_telemetry, boolean()}
               | {:handler, {atom(), term()}}
               | {:first_connect_delay, non_neg_integer()},
             options: [option]
  def start_link(connection_opts, opts \\ []) do
    client_id = Keyword.fetch!(connection_opts, :client_id)
    server = connection_opts |> Keyword.fetch!(:server) |> Transport.new()

    connect = %Package.Connect{
      client_id: client_id,
      user_name: Keyword.get(connection_opts, :user_name),
      password: Keyword.get(connection_opts, :password),
      keep_alive: Keyword.get(connection_opts, :keep_alive, 60),
      keep_alive_timeout: Keyword.get(connection_opts, :keep_alive_timeout, 5) * 1000,
      will: Keyword.get(connection_opts, :will),
      # if we re-spawn from here it means our state is gone
      clean_session: Keyword.get(connection_opts, :clean_session, true)
    }

    backoff = Keyword.get(connection_opts, :backoff, [])

    # This allow us to either pass in a list of topics, or a
    # subscription struct. Passing in a subscription struct is helpful
    # in tests.
    subscriptions =
      case Keyword.get(connection_opts, :subscriptions, []) do
        topics when is_list(topics) ->
          Enum.into(topics, %Package.Subscribe{})

        %Package.Subscribe{} = subscribe ->
          subscribe
      end

    # @todo, validate that the handler is valid
    connection_opts =
      Keyword.take(connection_opts, [
        :client_id,
        :handler,
        :enable_telemetry,
        :first_connect_delay
      ])

    initial = {server, connect, backoff, subscriptions, connection_opts}
    opts = Keyword.merge(opts, name: via_name(client_id))
    GenServer.start_link(__MODULE__, initial, opts)
  end

  @doc false
  @spec via_name(Tortoise311.client_id()) ::
          pid() | {:via, Registry, {Tortoise311.Registry, {atom(), Tortoise311.client_id()}}}
  def via_name(client_id) do
    Tortoise311.Registry.via_name(__MODULE__, client_id)
  end

  @spec child_spec(Keyword.t()) :: %{
          id: term(),
          start: {__MODULE__, :start_link, [Keyword.t()]},
          restart: :transient | :permanent | :temporary,
          type: :worker
        }
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      restart: Keyword.get(opts, :restart, :transient),
      type: :worker
    }
  end

  @doc """
  Close the connection to the broker.

  Given the `client_id` of a running connection it will cancel the
  inflight messages and send the proper disconnect message to the
  broker. The session will get terminated on the server.
  """
  @spec disconnect(Tortoise311.client_id()) :: :ok
  def disconnect(client_id) do
    GenServer.call(via_name(client_id), :disconnect)
  end

  @doc """
  Return the list of subscribed topics.

  Given the `client_id` of a running connection return its current
  subscriptions. This is helpful in a debugging situation.
  """
  @spec subscriptions(Tortoise311.client_id()) :: Tortoise311.Package.Subscribe.t()
  def subscriptions(client_id) do
    GenServer.call(via_name(client_id), :subscriptions)
  end

  @doc """
  Subscribe to one or more topics using topic filters on `client_id`

  The topic filter should be a 2-tuple, `{topic_filter, qos}`, where
  the `topic_filter` is a valid MQTT topic filter, and `qos` an
  integer value 0 through 2.

  Multiple topics can be given as a list.

  The subscribe function is asynchronous, so it will return `{:ok,
  ref}`. Eventually a response will get delivered to the process
  mailbox, tagged with the reference stored in `ref`. It will take the
  form of:

      {{Tortoise311, ^client_id}, ^ref, ^result}

  Where the `result` can be one of `:ok`, or `{:error, reason}`.

  Read the documentation for `Tortoise311.Connection.subscribe_sync/3`
  for a blocking version of this call.
  """
  @spec subscribe(Tortoise311.client_id(), topic | topics, [options]) :: {:ok, reference()}
        when topics: [topic],
             topic: {Tortoise311.topic_filter(), Tortoise311.qos()},
             options:
               {:timeout, timeout()}
               | {:identifier, Tortoise311.package_identifier()}
  def subscribe(client_id, topics, opts \\ [])

  def subscribe(client_id, [{_, n} | _] = topics, opts) when is_number(n) do
    caller = {_, ref} = {self(), make_ref()}
    {identifier, opts} = Keyword.pop_first(opts, :identifier, nil)
    subscribe = Enum.into(topics, %Package.Subscribe{identifier: identifier})
    GenServer.cast(via_name(client_id), {:subscribe, caller, subscribe, opts})
    {:ok, ref}
  end

  def subscribe(client_id, {_, n} = topic, opts) when is_number(n) do
    subscribe(client_id, [topic], opts)
  end

  def subscribe(client_id, topic, opts) when is_binary(topic) do
    case Keyword.pop_first(opts, :qos) do
      {nil, _opts} ->
        throw("Please specify a quality of service for the subscription")

      {qos, opts} when qos in 0..2 ->
        subscribe(client_id, [{topic, qos}], opts)
    end
  end

  @doc """
  Subscribe to topics and block until the server acknowledges.

  This is a synchronous version of the
  `Tortoise311.Connection.subscribe/3`. In fact it calls into
  `Tortoise311.Connection.subscribe/3` but will handle the selective
  receive loop, making it much easier to work with. Also, this
  function can be used to block a process that cannot continue before
  it has a subscription to the given topics.

  See `Tortoise311.Connection.subscribe/3` for configuration options.
  """
  @spec subscribe_sync(Tortoise311.client_id(), topic | topics, [options]) ::
          :ok | {:error, :timeout}
        when topics: [topic],
             topic: {Tortoise311.topic_filter(), Tortoise311.qos()},
             options:
               {:timeout, timeout()}
               | {:identifier, Tortoise311.package_identifier()}
  def subscribe_sync(client_id, topics, opts \\ [])

  def subscribe_sync(client_id, [{_, n} | _] = topics, opts) when is_number(n) do
    timeout = Keyword.get(opts, :timeout, 5000)
    {:ok, ref} = subscribe(client_id, topics, opts)

    receive do
      {{Tortoise311, ^client_id}, ^ref, result} -> result
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def subscribe_sync(client_id, {_, n} = topic, opts) when is_number(n) do
    subscribe_sync(client_id, [topic], opts)
  end

  def subscribe_sync(client_id, topic, opts) when is_binary(topic) do
    case Keyword.pop_first(opts, :qos) do
      {nil, _opts} ->
        throw("Please specify a quality of service for the subscription")

      {qos, opts} ->
        subscribe_sync(client_id, [{topic, qos}], opts)
    end
  end

  @doc """
  Unsubscribe from one of more topic filters. The topic filters are
  given as strings. Multiple topic filters can be given at once by
  passing in a list of strings.

      Tortoise311.Connection.unsubscribe(client_id, ["foo/bar", "quux"])

  This operation is asynchronous. When the operation is done a message
  will be received in mailbox of the originating process.
  """
  @spec unsubscribe(Tortoise311.client_id(), topic | topics, [options]) :: {:ok, reference()}
        when topics: [topic],
             topic: Tortoise311.topic_filter(),
             options:
               {:timeout, timeout()}
               | {:identifier, Tortoise311.package_identifier()}
  def unsubscribe(client_id, topics, opts \\ [])

  def unsubscribe(client_id, [topic | _] = topics, opts) when is_binary(topic) do
    caller = {_, ref} = {self(), make_ref()}
    {identifier, opts} = Keyword.pop_first(opts, :identifier, nil)
    unsubscribe = %Package.Unsubscribe{identifier: identifier, topics: topics}
    GenServer.cast(via_name(client_id), {:unsubscribe, caller, unsubscribe, opts})
    {:ok, ref}
  end

  def unsubscribe(client_id, topic, opts) when is_binary(topic) do
    unsubscribe(client_id, [topic], opts)
  end

  @doc """
  Unsubscribe from topics and block until the server acknowledges.

  This is a synchronous version of
  `Tortoise311.Connection.unsubscribe/3`. It will block until the server
  has send the acknowledge message.

  See `Tortoise311.Connection.unsubscribe/3` for configuration options.
  """
  @spec unsubscribe_sync(Tortoise311.client_id(), topic | topics, [options]) ::
          :ok | {:error, :timeout}
        when topics: [topic],
             topic: Tortoise311.topic_filter(),
             options:
               {:timeout, timeout()}
               | {:identifier, Tortoise311.package_identifier()}
  def unsubscribe_sync(client_id, topics, opts \\ [])

  def unsubscribe_sync(client_id, topics, opts) when is_list(topics) do
    timeout = Keyword.get(opts, :timeout, 5000)
    {:ok, ref} = unsubscribe(client_id, topics, opts)

    receive do
      {{Tortoise311, ^client_id}, ^ref, result} -> result
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def unsubscribe_sync(client_id, topic, opts) when is_binary(topic) do
    unsubscribe_sync(client_id, [topic], opts)
  end

  @doc """
  Ping the broker.

  When the round-trip is complete a message with the time taken in
  milliseconds will be send to the process that invoked the ping
  command.

  The connection will automatically ping the broker at the interval
  specified in the connection configuration, so there is no need to
  setup a reoccurring ping. This ping function is exposed for
  debugging purposes. If ping latency over time is desired it is
  better to listen on `:ping_response` using the `Tortoise311.Events`
  PubSub.
  """
  @spec ping(Tortoise311.client_id()) :: {:ok, reference()}
  defdelegate ping(client_id), to: Tortoise311.Connection.Controller

  @doc """
  Ping the server and await the ping latency reply.

  Takes a `client_id` and an optional `timeout`.

  Like `ping/1` but will block the caller process until a response is
  received from the server. The response will contain the ping latency
  in milliseconds.
  """
  @spec ping_sync(Tortoise311.client_id(), timeout()) ::
          {:ok, reference()} | {:error, :timeout}
  defdelegate ping_sync(client_id, timeout \\ Tortoise311.default_timeout()),
    to: Tortoise311.Connection.Controller

  @doc false
  @spec connection(Tortoise311.client_id(), [opts]) ::
          {:ok, {module(), term()}} | {:error, :unknown_connection} | {:error, :timeout}
        when opts: {:timeout, timeout()} | {:active, boolean()}
  def connection(client_id, opts \\ [active: false]) do
    # register a connection subscription in the case we are currently
    # in the connect phase; this solves a possible race condition
    # where the connection is requested while the status is
    # connecting, but will reach the receive block after the message
    # has been dispatched from the pubsub; previously we registered
    # for the connection message in this window.
    {:ok, _} = Events.register(client_id, :connection)
    timeout = Keyword.get(opts, :timeout, Tortoise311.default_timeout())

    case Tortoise311.Registry.meta(via_name(client_id)) do
      {:ok, {_transport, _socket} = connection} ->
        {:ok, connection}

      {:ok, :connecting} ->
        receive do
          {{Tortoise311, ^client_id}, :connection, {transport, socket}} ->
            {:ok, {transport, socket}}
        after
          timeout ->
            Logger.warning("[Tortoise311] Connection - timed out")
            {:error, :timeout}
        end

      :error ->
        {:error, :unknown_connection}
    end
  after
    # if the connection subscription is non-active we should remove it
    # from the registry, so the process will not receive connection
    # messages when the connection is reestablished.
    active? = Keyword.get(opts, :active, false)
    unless active?, do: Events.unregister(client_id, :connection)
  end

  # Callbacks
  @impl GenServer
  def init(
        {transport, %Connect{client_id: client_id} = connect, backoff_opts, subscriptions, opts}
      ) do
    {:ok, %Handler{} = handler} =
      Handler.new(Keyword.fetch!(opts, :handler)) |> Handler.execute(:init)

    state = %State{
      client_id: client_id,
      server: transport,
      connect: connect,
      backoff: Backoff.new(backoff_opts),
      subscriptions: subscriptions,
      opts: opts,
      status: :down,
      handler: handler
    }

    Tortoise311.Registry.put_meta(via_name(client_id), :connecting)
    {:ok, _pid} = Tortoise311.Events.register(client_id, :status)

    {:ok, state, {:continue, :first_connect}}
  end

  @impl GenServer
  def terminate(_reason, state) do
    :ok = Tortoise311.Registry.delete_meta(via_name(state.connect.client_id))
    :ok = Events.dispatch(state.client_id, :status, :terminated)
    :ok
  end

  @impl GenServer
  def handle_continue(:first_connect, state) do
    # Apply a short delay before connecting to limit the max rate of reconnects
    # if the GenServer crashes. The delay is jittered by 50% of its value. The
    # default is 1 second.
    delay_with_jitter = first_connect_delay_with_jitter(state)

    Process.send_after(self(), :connect, delay_with_jitter)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:connect, state) do
    # make sure we will not fall for a keep alive timeout while we reconnect
    # check if the will needs to be updated for each connection
    state = cancel_keep_alive(state) |> maybe_update_last_will()

    with {%Connack{status: :accepted} = connack, socket} <-
           do_connect(state.server, state.connect),
         {:ok, state} = init_connection(socket, state) do
      # we are connected; reset backoff state, etc
      state =
        %State{state | backoff: Backoff.reset(state.backoff)}
        |> update_connection_status(:up)
        |> reset_keep_alive()

      case connack do
        %Connack{session_present: true} ->
          {:noreply, state}

        %Connack{session_present: false} ->
          :ok = Inflight.reset(state.client_id)
          unless Enum.empty?(state.subscriptions), do: send(self(), :subscribe)
          {:noreply, state}
      end
    else
      %Connack{status: {:refused, reason}} ->
        {timeout, state} = Map.get_and_update(state, :backoff, &Backoff.next/1)

        Logger.warning(
          "[Tortoise311] Connection refused: #{inspect(reason)}, #{inspect(summarize_state(state))}"
        )

        Process.send_after(self(), :connect, timeout)
        {:noreply, state}

      {:error, reason} ->
        {timeout, state} = Map.get_and_update(state, :backoff, &Backoff.next/1)

        Logger.warning(
          "[Tortoise311] Connection failed: #{inspect(reason)}, #{inspect(summarize_state(state))}. Retrying in #{timeout} msecs."
        )

        case categorize_error(reason) do
          :connectivity ->
            Process.send_after(self(), :connect, timeout)
            {:noreply, state}

          :other ->
            {:stop, reason, state}
        end
    end
  end

  def handle_info(:subscribe, %State{subscriptions: subscriptions} = state) do
    client_id = state.connect.client_id

    case Enum.empty?(subscriptions) do
      true ->
        # nothing to subscribe to, just continue
        {:noreply, state}

      false ->
        # subscribe to the predefined topics
        case Inflight.track_sync(client_id, {:outgoing, subscriptions}, 5000) do
          {:error, :timeout} ->
            {:noreply, retry_subscribe(state, "Subscription timed out")}

          result ->
            case handle_suback_result(result, state) do
              {:ok, updated_state} ->
                {:noreply, updated_state}

              {:error, reasons} ->
                {:noreply,
                 retry_subscribe(state, "Subscription failed because of #{inspect(reasons)}")}
            end
        end
    end
  end

  def handle_info(:ping, %State{} = state) do
    case Controller.ping_sync(state.connect.client_id, state.connect.keep_alive_timeout) do
      {:ok, round_trip_time} ->
        Events.dispatch(state.connect.client_id, :ping_response, round_trip_time)
        state = reset_keep_alive(state)
        {:noreply, state}

      {:error, :timeout} ->
        {:stop, :ping_timeout, state}
    end
  end

  # dropping connection
  def handle_info({transport, _socket}, state) when transport in [:tcp_closed, :ssl_closed] do
    Logger.error("Socket closed before we handed it to the receiver")
    # communicate that we are down
    :ok = Events.dispatch(state.client_id, :status, :down)
    {:noreply, state}
  end

  # react to connection status change events
  def handle_info(
        {{Tortoise311, client_id}, :status, status},
        %{client_id: client_id, status: current} = state
      ) do
    case status do
      ^current ->
        {:noreply, state}

      :up ->
        Logger.info("[Tortoise311] Connection went up.")
        {:noreply, %State{state | status: status}}

      :down ->
        # Try to reconnect after the same jittered delay as for a first connect
        # This should help avoid excessive numbers of connects when network access is flapping
        delay_with_jitter = first_connect_delay_with_jitter(state)

        Logger.info(
          "[Tortoise311] Connection went down. Reconnecting after #{delay_with_jitter}."
        )

        Process.send_after(self(), :connect, delay_with_jitter)
        {:noreply, %State{state | status: status}}
    end
  end

  @impl GenServer
  def handle_call(:subscriptions, _from, state) do
    {:reply, state.subscriptions, state}
  end

  def handle_call(:disconnect, from, state) do
    :ok = Events.dispatch(state.client_id, :status, :terminating)
    :ok = Inflight.drain(state.client_id)
    :ok = Controller.stop(state.client_id)
    :ok = GenServer.reply(from, :ok)
    {:stop, :shutdown, state}
  end

  @impl GenServer
  def handle_cast({:subscribe, {caller_pid, ref}, subscribe, opts}, state) do
    client_id = state.connect.client_id
    timeout = Keyword.get(opts, :timeout, 5000)

    case Inflight.track_sync(client_id, {:outgoing, subscribe}, timeout) do
      {:error, :timeout} = error ->
        send(caller_pid, {{Tortoise311, client_id}, ref, error})
        {:noreply, state}

      result ->
        case handle_suback_result(result, state) do
          {:ok, updated_state} ->
            send(caller_pid, {{Tortoise311, client_id}, ref, :ok})
            {:noreply, updated_state}

          {:error, reasons} ->
            error = {:unable_to_subscribe, reasons}
            send(caller_pid, {{Tortoise311, client_id}, ref, {:error, reasons}})
            {:stop, error, state}
        end
    end
  end

  def handle_cast({:unsubscribe, {caller_pid, ref}, unsubscribe, opts}, state) do
    client_id = state.connect.client_id
    timeout = Keyword.get(opts, :timeout, 5000)

    case Inflight.track_sync(client_id, {:outgoing, unsubscribe}, timeout) do
      {:error, :timeout} = error ->
        send(caller_pid, {{Tortoise311, client_id}, ref, error})
        {:noreply, state}

      unsubbed ->
        topics = Keyword.drop(state.subscriptions.topics, unsubbed)
        subscriptions = %Package.Subscribe{state.subscriptions | topics: topics}
        send(caller_pid, {{Tortoise311, client_id}, ref, :ok})
        {:noreply, %State{state | subscriptions: subscriptions}}
    end
  end

  # Helpers

  defp first_connect_delay_with_jitter(state) do
    delay = Keyword.get(state.opts, :first_connect_delay, 1000)
    round(delay * (1.0 + (:rand.uniform() - 0.5)))
  end

  defp handle_suback_result(%{:error => []} = results, %State{} = state) do
    subscriptions = Enum.into(results[:ok], state.subscriptions)
    {:ok, %State{state | subscriptions: subscriptions}}
  end

  defp handle_suback_result(%{:error => errors}, %State{}) do
    {:error, errors}
  end

  defp reset_keep_alive(%State{keep_alive: nil} = state) do
    ref = Process.send_after(self(), :ping, state.connect.keep_alive * 1000)
    %State{state | keep_alive: ref}
  end

  defp reset_keep_alive(%State{keep_alive: previous_ref} = state) do
    # Cancel the previous timer, just in case one was already set
    _ = Process.cancel_timer(previous_ref)
    ref = Process.send_after(self(), :ping, state.connect.keep_alive * 1000)
    %State{state | keep_alive: ref}
  end

  defp cancel_keep_alive(%State{keep_alive: nil} = state) do
    state
  end

  defp cancel_keep_alive(%State{keep_alive: keep_alive_ref} = state) do
    _ = Process.cancel_timer(keep_alive_ref)
    %State{state | keep_alive: nil}
  end

  defp maybe_update_last_will(%State{connect: connect, handler: handler} = state) do
    if function_exported?(handler.module, :last_will, 1) do
      {{:ok, last_will}, _updated_handler} = Handler.execute(handler, :last_will)

      if last_will == nil do
        state
      else
        updated_connect = %Connect{connect | will: last_will}
        %State{state | connect: updated_connect}
      end
    else
      state
    end
  end

  # dispatch connection status if the connection status change
  defp update_connection_status(%State{status: same} = state, same) do
    state
  end

  defp update_connection_status(%State{} = state, status) do
    :ok = Events.dispatch(state.connect.client_id, :status, status)
    %State{state | status: status}
  end

  defp do_connect(server, %Connect{} = connect) do
    %Transport{type: transport, host: host, port: port, opts: opts} = server

    with {:ok, socket} <- transport.connect(host, port, opts, 10000),
         :ok = transport.send(socket, Package.encode(connect)),
         {:ok, packet} <- transport.recv(socket, 4, 5000) do
      try do
        case Package.decode(packet) do
          %Connack{status: :accepted} = connack ->
            {connack, socket}

          %Connack{status: {:refused, _reason}} = connack ->
            connack
        end
      catch
        :error, {:badmatch, _unexpected} ->
          violation = %{expected: Connect, got: packet}
          {:error, {:protocol_violation, violation}}
      end
    else
      {:error, :econnrefused} ->
        {:error, {:connection_refused, host, port}}

      {:error, :nxdomain} ->
        {:error, {:nxdomain, host, port}}

      {:error, {:options, {:cacertfile, []}}} ->
        {:error, :no_cacertfile_specified}

      {:error, :closed} ->
        {:error, :server_closed_connection}

      {:error, :timeout} ->
        {:error, :connection_timeout}

      {:error, other} ->
        {:error, other}
    end
  end

  defp init_connection(socket, %State{opts: opts, server: transport, connect: connect} = state) do
    connection = {transport.type, socket}
    :ok = start_connection_supervisor(opts)
    :ok = Receiver.handle_socket(connect.client_id, connection)
    :ok = Tortoise311.Registry.put_meta(via_name(connect.client_id), connection)
    :ok = Events.dispatch(connect.client_id, :connection, connection)

    # set clean session to false for future reconnect attempts
    connect = %Connect{connect | clean_session: false}
    {:ok, %State{state | connect: connect}}
  end

  defp retry_subscribe(state, reason) do
    {timeout, state} = Map.get_and_update(state, :backoff, &Backoff.next/1)

    Logger.warning(
      "[Tortoise311] #{reason}: #{inspect(summarize_state(state))}. Retrying in #{timeout} msecs."
    )

    Process.send_after(self(), :subscribe, timeout)
    state
  end

  defp start_connection_supervisor(opts) do
    case Connection.Supervisor.start_link(opts) do
      {:ok, _pid} ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok
    end
  end

  defp categorize_error({:nxdomain, _host, _port}) do
    :connectivity
  end

  defp categorize_error({:connection_refused, _host, _port}) do
    :connectivity
  end

  defp categorize_error(:server_closed_connection) do
    :connectivity
  end

  defp categorize_error(:connection_timeout) do
    :connectivity
  end

  defp categorize_error(:enetunreach) do
    :connectivity
  end

  defp categorize_error(_other) do
    :other
  end

  defp summarize_state(state) do
    %{
      client_id: state.client_id,
      protocol: state.connect.protocol,
      protocol_version: state.connect.protocol_version,
      keep_alive: state.connect.keep_alive,
      clean_session: state.connect.clean_session,
      host: state.server.host,
      port: state.server.port,
      transport: state.server.type,
      status: state.status,
      subscriptions: state.subscriptions
    }
  end
end
