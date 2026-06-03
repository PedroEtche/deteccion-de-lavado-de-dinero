import socket

_HEADER_SIZE = 4
_BYTE_ORDER = "big"


class TCPSocket:
    def __init__(self, sock):
        self._sock = sock

    def send_bytes(self, payload):
        header = len(payload).to_bytes(_HEADER_SIZE, byteorder=_BYTE_ORDER)
        self._sock.sendall(header + payload)

    def recv_bytes(self):
        header = self._recv_exact(_HEADER_SIZE)
        length = int.from_bytes(header, byteorder=_BYTE_ORDER)
        return self._recv_exact(length)

    def close(self):
        self._sock.close()

    # Used for sending a signal to the other end of communication ending
    # wr means dont expect more messages
    # rd means i wont be reading anymore
    # rdwr means the comunnication is finished
    def shutdown(self, instruction):
        if instruction == "wr":
            self._sock.shutdown(socket.SHUT_WR)
        elif instruction == "rd":
            self._sock.shutdown(socket.SHUT_RD)
        else:
            self._sock.shutdown(socket.SHUT_RDWR)

    def _recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Connection closed unexpectedly")
            buf.extend(chunk)
        return bytes(buf)


def connect(host, port):
    return TCPSocket(socket.create_connection((host, port)))
