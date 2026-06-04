import socket
import threading
import unittest

from src.common.communication.tcp import TCPSocket, connect


def _start_echo_server(handler):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(1)
    port = server_sock.getsockname()[1]

    def run():
        try:
            client_sock, _ = server_sock.accept()
            tcp_sock = TCPSocket(client_sock)
            try:
                handler(tcp_sock)
            finally:
                tcp_sock.close()
        finally:
            server_sock.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return port, thread, server_sock.close


class TestTCPSocketRoundTrip(unittest.TestCase):
    def test_send_and_receive_short_message(self):
        received = []

        def handler(sock):
            received.append(sock.recv_bytes())

        port, thread, _ = _start_echo_server(handler)
        client = connect("127.0.0.1", port)
        client.send_bytes(b"hola mundo")
        client.close()
        thread.join(timeout=2)

        self.assertEqual(received, [b"hola mundo"])

    def test_send_and_receive_empty_payload(self):
        received = []

        def handler(sock):
            received.append(sock.recv_bytes())

        port, thread, _ = _start_echo_server(handler)
        client = connect("127.0.0.1", port)
        client.send_bytes(b"")
        client.close()
        thread.join(timeout=2)

        self.assertEqual(received, [b""])

    def test_send_and_receive_multiple_messages_preserves_order(self):
        received = []

        def handler(sock):
            for _ in range(3):
                received.append(sock.recv_bytes())

        port, thread, _ = _start_echo_server(handler)
        client = connect("127.0.0.1", port)
        client.send_bytes(b"uno")
        client.send_bytes(b"dos")
        client.send_bytes(b"tres")
        client.close()
        thread.join(timeout=2)

        self.assertEqual(received, [b"uno", b"dos", b"tres"])

    def test_send_and_receive_large_payload(self):
        # 1 MiB de payload — fuerza al kernel a fragmentar el send/recv en TCP.
        payload = b"x" * (1 << 20)
        received = []

        def handler(sock):
            received.append(sock.recv_bytes())

        port, thread, _ = _start_echo_server(handler)
        client = connect("127.0.0.1", port)
        client.send_bytes(payload)
        client.close()
        thread.join(timeout=5)

        self.assertEqual(len(received), 1)
        self.assertEqual(len(received[0]), len(payload))
        self.assertEqual(received[0], payload)

    def test_binary_payload_roundtrips_unmodified(self):
        payload = bytes(range(256))
        received = []

        def handler(sock):
            received.append(sock.recv_bytes())

        port, thread, _ = _start_echo_server(handler)
        client = connect("127.0.0.1", port)
        client.send_bytes(payload)
        client.close()
        thread.join(timeout=2)

        self.assertEqual(received, [payload])

    def test_full_echo_roundtrip(self):
        def handler(sock):
            data = sock.recv_bytes()
            sock.send_bytes(data)

        port, thread, _ = _start_echo_server(handler)
        client = connect("127.0.0.1", port)
        client.send_bytes(b"ping")
        response = client.recv_bytes()
        client.close()
        thread.join(timeout=2)

        self.assertEqual(response, b"ping")


class _FakeSocket:
    def __init__(self, incoming, recv_chunk=1):
        self._buf = bytearray(incoming)
        self._chunk = recv_chunk
        self.sent = bytearray()

    def recv(self, n):
        if not self._buf:
            return b""
        take = min(self._chunk, n, len(self._buf))
        chunk = bytes(self._buf[:take])
        del self._buf[:take]
        return chunk

    def sendall(self, data):
        self.sent.extend(data)

    def shutdown(self, *_):
        pass

    def close(self):
        pass


class TestTCPSocketShortReads(unittest.TestCase):
    def test_recv_handles_byte_by_byte_chunks(self):
        payload = b"mensaje fragmentado"
        header = len(payload).to_bytes(4, "big")
        fake = _FakeSocket(header + payload, recv_chunk=1)

        sock = TCPSocket(fake)
        self.assertEqual(sock.recv_bytes(), payload)

    def test_recv_handles_header_split_across_chunks(self):
        payload = b"abc"
        header = len(payload).to_bytes(4, "big")
        # chunk de 2 bytes parte el header de 4 bytes en dos lecturas.
        fake = _FakeSocket(header + payload, recv_chunk=2)

        sock = TCPSocket(fake)
        self.assertEqual(sock.recv_bytes(), payload)

    def test_recv_raises_when_connection_closes_mid_header(self):
        fake = _FakeSocket(b"\x00\x00", recv_chunk=1)  # solo 2 de 4 bytes del header

        sock = TCPSocket(fake)
        with self.assertRaises(ConnectionError):
            sock.recv_bytes()

    def test_recv_raises_when_connection_closes_mid_body(self):
        header = (10).to_bytes(4, "big")
        fake = _FakeSocket(header + b"abc", recv_chunk=1)  # header completo, body corto

        sock = TCPSocket(fake)
        with self.assertRaises(ConnectionError):
            sock.recv_bytes()


class TestTCPSocketHeaderFormat(unittest.TestCase):
    def test_send_writes_length_prefix_big_endian(self):
        fake = _FakeSocket(b"")
        sock = TCPSocket(fake)

        sock.send_bytes(b"hola")

        self.assertEqual(bytes(fake.sent[:4]), (4).to_bytes(4, "big"))
        self.assertEqual(bytes(fake.sent[4:]), b"hola")

    def test_send_empty_payload_writes_zero_length_header(self):
        fake = _FakeSocket(b"")
        sock = TCPSocket(fake)

        sock.send_bytes(b"")

        self.assertEqual(bytes(fake.sent), (0).to_bytes(4, "big"))


if __name__ == "__main__":
    unittest.main()
