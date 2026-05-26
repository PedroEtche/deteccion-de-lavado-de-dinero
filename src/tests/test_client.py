import os
import socket
import tempfile
import threading
import unittest

from src.client.main import run_client
from src.common.communication.protocol import receive_batches
from src.common.communication.tcp import TCPSocket


_HEADER = (
    "Timestamp,From Bank,Account,To Bank,Account,"
    "Amount Received,Receiving Currency,Amount Paid,"
    "Payment Currency,Payment Format,Is Laundering\n"
)


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
                for batch in receive_batches(tcp_sock):
                    collected.append(batch)
            finally:
                tcp_sock.close()
        finally:
            server_sock.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return port, thread, collected


def _write_csv(rows):
    handle = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8", newline=""
    )
    handle.write(_HEADER)
    for row in rows:
        handle.write(row + "\n")
    handle.close()
    return handle.name


class TestClient(unittest.TestCase):

    def test_client_sends_all_rows_split_in_batches(self):
        rows = [
            f"2022/09/01 00:0{i},20,A{i},30,B{i},100,USD,100,USD,Wire,0"
            for i in range(10)
        ]
        csv_path = _write_csv(rows)
        try:
            port, thread, collected = _start_collecting_server()
            run_client("127.0.0.1", port, csv_path, batch_size=3)
            thread.join(timeout=2)

            self.assertEqual([len(b) for b in collected], [3, 3, 3, 1])
        finally:
            os.unlink(csv_path)

    def test_client_drops_is_laundering_and_disambiguates_account_column(self):
        rows = ["2022/09/01 00:00,20,A,30,B,100,USD,100,USD,Wire,1"]
        csv_path = _write_csv(rows)
        try:
            port, thread, collected = _start_collecting_server()
            run_client("127.0.0.1", port, csv_path, batch_size=10)
            thread.join(timeout=2)

            self.assertEqual(len(collected), 1)
            row = collected[0][0]
            self.assertNotIn("Is Laundering", row)
            self.assertEqual(row["Account"], "A")
            self.assertEqual(row["Account.1"], "B")
        finally:
            os.unlink(csv_path)

    def test_client_with_only_header_sends_no_batches(self):
        csv_path = _write_csv([])
        try:
            port, thread, collected = _start_collecting_server()
            run_client("127.0.0.1", port, csv_path, batch_size=5)
            thread.join(timeout=2)

            self.assertEqual(collected, [])
        finally:
            os.unlink(csv_path)

    def test_client_against_real_sample_file_if_present(self):
        sample_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "sample.csv"
        )
        if not os.path.exists(sample_path):
            self.skipTest("data/sample.csv not generated; run `make sample` to enable")

        port, thread, collected = _start_collecting_server()
        run_client("127.0.0.1", port, sample_path, batch_size=500)
        thread.join(timeout=10)

        self.assertGreater(len(collected), 0)
        for batch in collected:
            for row in batch:
                self.assertNotIn("Is Laundering", row)
                self.assertIn("Account", row)
                self.assertIn("Account.1", row)


if __name__ == "__main__":
    unittest.main()
