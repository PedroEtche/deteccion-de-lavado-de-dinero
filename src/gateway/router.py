import logging
import threading


class WorkerRouter:
    """Rutea los mensajes de salida del gateway hacia los workers.

    - accounts: broadcast a worker_1..N
    - transactions: round-robin determinista por msg_id (a date y a usd)
    - eof: broadcast a "eof_broadcast" en los 3 exchanges

    El round-robin usa `(msg_id % N) + 1` (mismo esquema que BaseWorker): sin
    contadores ni estado propio, y una reentrega del mismo msg_id cae siempre
    en el mismo worker. El _lock protege los channels de pika (no son
    thread-safe) frente a los multiples threads de cliente que publican a la vez.
    """

    def __init__(
        self,
        transactions_usd_mw,
        transactions_date_mw,
        accounts_mw,
        transactions_usd_workers,
        transactions_date_workers,
        accounts_workers,
    ):
        self.transactions_usd_mw = transactions_usd_mw
        self.transactions_date_mw = transactions_date_mw
        self.accounts_mw = accounts_mw
        self.transactions_usd_workers = transactions_usd_workers
        self.transactions_date_workers = transactions_date_workers
        self.accounts_workers = accounts_workers
        self._lock = threading.Lock()

    def send_accounts(self, serialized_message: bytes):
        """Broadcasts data to accounts workers."""
        with self._lock:
            logging.info(
                "Broadcasting accounts message to %d workers", self.accounts_workers
            )
            for worker_id in range(1, self.accounts_workers + 1):
                self.accounts_mw.send(
                    serialized_message,
                    routing_key=f"worker_{worker_id}",
                )

    def send_transactions(self, serialized_message: bytes, msg_id: int):
        """Sends data via deterministic Round-Robin (msg_id % N) to a worker."""
        date_worker = (msg_id % self.transactions_date_workers) + 1
        usd_worker = (msg_id % self.transactions_usd_workers) + 1
        with self._lock:
            logging.info(
                "Routing transactions message to workers with Round-Robin strategy"
            )
            self.transactions_date_mw.send(
                serialized_message,
                routing_key=f"worker_{date_worker}",
            )
            self.transactions_usd_mw.send(
                serialized_message,
                routing_key=f"worker_{usd_worker}",
            )

    def send_eof(self, eof_message: bytes):
        """Broadcasts EOF to all workers listening to the exchange."""
        routing_key = "eof_broadcast"
        with self._lock:
            self.transactions_usd_mw.send(eof_message, routing_key=routing_key)
            self.transactions_date_mw.send(eof_message, routing_key=routing_key)
            self.accounts_mw.send(eof_message, routing_key=routing_key)
