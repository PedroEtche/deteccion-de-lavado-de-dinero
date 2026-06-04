import os
import socket
import tempfile
import threading
import unittest

from src.client.main import run_client
from src.common.communication import (
    STREAM_ACCOUNTS,
    STREAM_TRANSACTIONS,
)
from src.common.communication.protocol import receive_streams
from src.common.communication.tcp import TCPSocket


_TX_HEADER = (
    "Timestamp,From Bank,Account,To Bank,Account,"
    "Amount Received,Receiving Currency,Amount Paid,"
    "Payment Currency,Payment Format,Is Laundering\n"
)

_AC_HEADER = "Bank Name,Bank ID,Account Number,Entity ID,Entity Name\n"


def _start_collecting_server():
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(1)
    port = server_sock.getsockname()[1]
    collected = []

    def run():
        try:
            client_sock, _ = server_sock.accept()
            tcp_sock = TCPSocket(client_sock)
            try:
                for stream, batch in receive_streams(tcp_sock):
                    collected.append((stream, batch))
            finally:
                tcp_sock.close()
        finally:
            server_sock.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return port, thread, collected


def _write_csv(header, rows):
    handle = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8", newline=""
    )
    handle.write(header)
    for row in rows:
        handle.write(row + "\n")
    handle.close()
    return handle.name


class TestClient(unittest.TestCase):
    def test_client_sends_transactions_split_in_batches(self):
        tx_rows = [
            f"2022/09/01 00:0{i},20,A{i},30,B{i},100,USD,100,USD,Wire,0"
            for i in range(10)
        ]
        accounts_path = _write_csv(_AC_HEADER, [])
        transactions_path = _write_csv(_TX_HEADER, tx_rows)
        try:
            port, thread, collected = _start_collecting_server()
            run_client(
                "127.0.0.1", port, accounts_path, transactions_path, batch_size=3
            )
            thread.join(timeout=2)

            tx_batches = [b for s, b in collected if s == STREAM_TRANSACTIONS]
            self.assertEqual([len(b) for b in tx_batches], [3, 3, 3, 1])
        finally:
            os.unlink(accounts_path)
            os.unlink(transactions_path)

    def test_client_sends_accounts_and_transactions_in_order(self):
        accounts_path = _write_csv(
            _AC_HEADER,
            [
                "China Bank #2820,314693,81B86A280,800D8CCF0,Corporation #41344",
                "France Bank #4585,311253,8187FEA80,800B505E0,Corporation #54497",
            ],
        )
        transactions_path = _write_csv(
            _TX_HEADER,
            [
                "2022/09/01 00:00,20,A,30,B,100,USD,100,USD,Wire,0",
            ],
        )
        try:
            port, thread, collected = _start_collecting_server()
            run_client(
                "127.0.0.1", port, accounts_path, transactions_path, batch_size=10
            )
            thread.join(timeout=2)

            streams = [s for s, _ in collected]
            self.assertEqual(streams, [STREAM_ACCOUNTS, STREAM_TRANSACTIONS])
        finally:
            os.unlink(accounts_path)
            os.unlink(transactions_path)

    def test_client_drops_is_laundering_and_disambiguates_account_column(self):
        accounts_path = _write_csv(_AC_HEADER, [])
        transactions_path = _write_csv(
            _TX_HEADER,
            [
                "2022/09/01 00:00,20,A,30,B,100,USD,100,USD,Wire,1",
            ],
        )
        try:
            port, thread, collected = _start_collecting_server()
            run_client(
                "127.0.0.1", port, accounts_path, transactions_path, batch_size=10
            )
            thread.join(timeout=2)

            tx_batches = [b for s, b in collected if s == STREAM_TRANSACTIONS]
            self.assertEqual(len(tx_batches), 1)
            row = tx_batches[0][0]
            self.assertNotIn("Is Laundering", row)
            self.assertEqual(row["Account"], "A")
            self.assertEqual(row["Account.1"], "B")
        finally:
            os.unlink(accounts_path)
            os.unlink(transactions_path)

    def test_client_accounts_batch_preserves_columns(self):
        accounts_path = _write_csv(
            _AC_HEADER,
            [
                "China Bank #2820,314693,81B86A280,800D8CCF0,Corporation #41344",
            ],
        )
        transactions_path = _write_csv(_TX_HEADER, [])
        try:
            port, thread, collected = _start_collecting_server()
            run_client(
                "127.0.0.1", port, accounts_path, transactions_path, batch_size=10
            )
            thread.join(timeout=2)

            ac_batches = [b for s, b in collected if s == STREAM_ACCOUNTS]
            self.assertEqual(len(ac_batches), 1)
            row = ac_batches[0][0]
            self.assertEqual(row["Bank Name"], "China Bank #2820")
            self.assertEqual(row["Account Number"], "81B86A280")
            self.assertEqual(row["Entity Name"], "Corporation #41344")
        finally:
            os.unlink(accounts_path)
            os.unlink(transactions_path)

    def test_client_with_only_headers_sends_no_batches(self):
        accounts_path = _write_csv(_AC_HEADER, [])
        transactions_path = _write_csv(_TX_HEADER, [])
        try:
            port, thread, collected = _start_collecting_server()
            run_client(
                "127.0.0.1", port, accounts_path, transactions_path, batch_size=5
            )
            thread.join(timeout=2)

            self.assertEqual(collected, [])
        finally:
            os.unlink(accounts_path)
            os.unlink(transactions_path)


if __name__ == "__main__":
    unittest.main()
