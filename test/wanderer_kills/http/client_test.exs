defmodule WandererKills.Http.ClientTest do
  use WandererKills.UnifiedTestCase,
    async: false,
    type: :unit,
    mocks: false,
    clear_caches: false

  alias WandererKills.Core.Support.Error
  alias WandererKills.Http.Client

  test "GET retries closed connections and returns the successful response" do
    url = start_http_server([:close, :close, :ok])

    assert {:ok, %{status: 200, body: "ok"}} = Client.get(url)
    assert_receive {:request, :GET}
    assert_receive {:request, :GET}
    assert_receive {:request, :GET}
    refute_receive {:request, _}
  end

  test "POST does not replay a delivered body when the peer closes before responding" do
    url = start_http_server([:close, :close, :close])
    body = ~s({"killmail_id":123456})

    result = Client.post(url, body)

    assert_receive {:request, :POST}
    assert_receive {:delivery, :POST, ^body}
    refute_receive {:request, _}
    refute_receive {:delivery, _, _}
    assert {:error, %Error{type: :request_failed, retryable: false}} = result
  end

  test "GET stops after two retries and preserves a retryable connection-closed error" do
    url = start_http_server([:close, :close, :close, :ok])

    assert {:error, %Error{type: :connection_closed, retryable: true}} = Client.get(url)
    assert_receive {:request, :GET}
    assert_receive {:request, :GET}
    assert_receive {:request, :GET}
    refute_receive {:request, _}
  end

  test "GET preserves transport failure context and retryability without closed-connection retries" do
    url = start_http_server([:invalid_tls, :invalid_tls], "https")

    assert {:error, %Error{type: :transport_error, retryable: true, message: message}} =
             Client.get(url)

    assert message =~ "TransportError"
    assert message =~ ":tls_alert"
    assert_receive {:request, :TLS}
    refute_receive {:request, _}
  end

  # A loopback peer exercises Finch's actual error wrapping, not a mock of Client.
  # Keep a listener alive after the scripted responses so extra retries are observable.
  defp start_http_server(actions, scheme \\ "http") do
    packet = if scheme == "https", do: :raw, else: :http_bin

    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, packet: packet, ip: {127, 0, 0, 1}])

    {:ok, port} = :inet.port(listener)
    owner = self()
    on_exit(fn -> :gen_tcp.close(listener) end)

    start_supervised!({Task, fn -> Enum.each(actions, &serve_request(listener, owner, &1)) end})

    "#{scheme}://127.0.0.1:#{port}/"
  end

  defp serve_request(listener, owner, :invalid_tls) do
    {:ok, socket} = :gen_tcp.accept(listener)
    {:ok, _client_hello} = :gen_tcp.recv(socket, 0, 2000)
    send(owner, {:request, :TLS})
    respond(socket, :ok)
    :gen_tcp.close(socket)
  end

  defp serve_request(listener, owner, action) do
    {:ok, socket} = :gen_tcp.accept(listener)
    {:ok, {:http_request, method, _, _}} = :gen_tcp.recv(socket, 0, 2000)
    content_length = receive_headers(socket)
    :ok = :inet.setopts(socket, packet: :raw)
    body = receive_body(socket, content_length)
    send(owner, {:request, method})
    send(owner, {:delivery, method, body})
    respond(socket, action)
    :gen_tcp.close(socket)
  end

  defp receive_headers(socket, content_length \\ 0) do
    case :gen_tcp.recv(socket, 0, 2000) do
      {:ok, :http_eoh} ->
        content_length

      {:ok, {:http_header, _, :"Content-Length", _, value}} ->
        receive_headers(socket, String.to_integer(value))

      {:ok, {:http_header, _, _, _, _}} ->
        receive_headers(socket, content_length)
    end
  end

  defp receive_body(_socket, 0), do: ""

  defp receive_body(socket, content_length) do
    {:ok, body} = :gen_tcp.recv(socket, content_length, 2000)
    body
  end

  defp respond(_socket, :close), do: :ok

  defp respond(socket, :ok) do
    :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
  end
end
