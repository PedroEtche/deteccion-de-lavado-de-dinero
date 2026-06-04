import socket

_BYTE_ORDER = "big"
_HEADER_SIZE = 4


class TCPSocket:
    def __init__(self, sock):
        self._sock = sock

    def send_bytes(self, payload: bytes):
        header = len(payload).to_bytes(_HEADER_SIZE, byteorder=_BYTE_ORDER)
        self._sock.sendall(header + payload)

    def recv_bytes(self) -> bytes:
        header = self._recv_exact(_HEADER_SIZE)
        msg_size = int.from_bytes(header, byteorder=_BYTE_ORDER)
        return self._recv_exact(msg_size)

    def close(self):
        self._sock.close()

    def shutdown(self, instruction):
        if instruction == "wr":
            self._sock.shutdown(socket.SHUT_WR)
        elif instruction == "rd":
            self._sock.shutdown(socket.SHUT_RD)
        else:
            self._sock.shutdown(socket.SHUT_RDWR)

    def _recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Connection closed unexpectedly")
            buf.extend(chunk)
        return bytes(buf)


def connect(host, port):
    return TCPSocket(socket.create_connection((host, port)))
