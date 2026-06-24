from typing import Callable
import logging


class EofCoordinator:
    """
    Counts incoming EOFs for a specific client.
    Once expected_eofs is reached, it triggers the flush callback.
    """

    def __init__(
        self, expected_eofs: int, on_flush: Callable[[str], None], state_manager
    ) -> None:
        if expected_eofs <= 0:
            raise ValueError("expected_eofs must be positive")

        self._expected = expected_eofs
        self._on_flush = on_flush
        self.state_manager = state_manager

        self.eofs_by_client = {}

        self.pending_flushes = []

        client_ids = self.state_manager.get_all_client_ids()

        for client_id in client_ids:
            eof_count = self.state_manager.load_eof_count(client_id)
            logging.info("Recovered EOF count for client %s: %s", client_id, eof_count)
            self.eofs_by_client[client_id] = eof_count

            if eof_count >= self._expected:
                self.pending_flushes.append(client_id)

    def resume_pending_flushes(self) -> None:
        """
        Called by the base worker AFTER RabbitMQ output exchanges are fully initialized.
        """
        for client_id in self.pending_flushes:
            logging.info("Resuming interrupted flush for client %s", client_id)
            self.check_eof_count(client_id)
        self.pending_flushes.clear()

    def handle_eof(self, client_id: str) -> int:
        """
        Called by the worker when it receives an EOF directly from upstream.
        """
        client_eofs = self.eofs_by_client.get(client_id, 0)
        client_eofs += 1

        self.eofs_by_client[client_id] = client_eofs
        self.state_manager.save_snapshot(client_id, client_eofs, {})

        return self.check_eof_count(client_id)

    def check_eof_count(self, client_id: str) -> int:
        """
        Returns the current EOF count for a specific client.
        """
        client_eofs = self.eofs_by_client.get(client_id, 0)
        if client_eofs >= self._expected:
            self._on_flush(client_id)
            self.eofs_by_client.pop(client_id, None)
            self.state_manager.delete_client(client_id)
            
        return client_eofs
        