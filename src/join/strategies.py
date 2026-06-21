import uuid
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional
import dataclasses

from src.common.communication.internal import (
    build_batch_message,
    build_results_for_query,
    Q2ResultRow,
    Q3ResultRow,
    Q4ResultRow,
    Q5ResultRow,
)

class JoinStrategy(ABC):
    """Abstract strategy that owns all join state.

    The Join worker delegates incoming data via :meth:`join_batch`. Stateful
    strategies accumulate state per client; when a batch is ready to be emitted
    mid-stream they invoke the registered join callback. When the Join signals
    it is safe to flush (all EOFs received) it calls :meth:`flush`, and the
    strategy returns everything it had buffered for that client.
    """

    def __init__(self) -> None:
        # callback(client, message_dict) registered by the worker.
        self._on_ready: Optional[Callable[[str, dict], None]] = None

    def register_join_callback(self, callback: Callable[[str, dict], None]) -> None:
        self._on_ready = callback

    def _emit(self, client: str, message: Optional[dict]) -> None:
        """Send a ready message downstream through the registered callback."""
        if self._on_ready is not None and message:
            self._on_ready(client, message)

    def __str__(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def join_batch(self, batch: List[Any], client: str) -> None:
        """Accumulate state for ``client``; emit ready batches via the callback."""
        raise NotImplementedError()

    @abstractmethod
    def flush(self, client: str) -> Optional[dict]:
        """Return a fully built message with everything buffered for ``client``
        (or ``None`` if there is nothing), clearing that client's state."""
        raise NotImplementedError()
    
    @abstractmethod
    def get_client_state(self, client: str) -> Any:
        """Returns the raw internal memory for a specific client. Return None if stateless."""
        raise NotImplementedError()

    @abstractmethod
    def set_client_state(self, client: str, state: Any) -> None:
        """Restores the raw internal memory for a specific client."""
        raise NotImplementedError()


class NoStrategy(JoinStrategy):
    """Streaming pass-through: emits each batch as it arrives, buffers nothing."""

    def join_batch(self, batch: List[Any], client: str) -> None:
        return None

    def flush(self, client: str) -> Optional[dict]:
        return None

    def build_eof_message(self, client, msg_id=None):
        pass

    def get_client_state(self, client: str) -> Any:
        return None  # Stateless! Don't write to disk.

    def set_client_state(self, client: str, state: Any) -> None:
        pass


class QueryResultStrategy(JoinStrategy):
    """Streaming pass-through for query result transactions."""

    def __init__(self, query_number: int) -> None:
        super().__init__()
        self.query_number = query_number

        _ROW_CLASSES = {
            2: Q2ResultRow,
            3: Q3ResultRow,
            4: Q4ResultRow,
        }
        self.row_class = _ROW_CLASSES.get(self.query_number)

    def join_batch(self, batch: List[Any], client: str) -> None:
        if not batch:
            return

        processed_batch = batch
        if self.row_class:
            processed_batch = []
            
            valid_keys = {f.name for f in dataclasses.fields(self.row_class)}
            
            for item in batch:
                if isinstance(item, dict):
                    safe_kwargs = {k: v for k, v in item.items() if k in valid_keys}
                else:
                    safe_kwargs = {k: getattr(item, k) for k in valid_keys if hasattr(item, k)}
                
                processed_batch.append(self.row_class(**safe_kwargs))

        self._emit(
            client,
            build_results_for_query(
                query_number=self.query_number,
                batch=processed_batch,
                eof=False,
                client=client,
            ),
        )

    def flush(self, client: str) -> Optional[dict]:
        return self.build_eof_message(client)

    def build_eof_message(self, client, msg_id=None):
        return build_results_for_query(
            query_number=self.query_number, batch=[], eof=True, client=client
        )

    def get_client_state(self, client: str) -> Any:
        return None  # Stateless! Don't write to disk.

    def set_client_state(self, client: str, state: Any) -> None:
        pass

class AveragesUnionStrategy(JoinStrategy):
    """Stateful: junta los promedios parciales que llegan de los N shards del
    aggregator (cada shard trae los formatos que le tocaron) y en el flush emite
    la union de todos. El Join se configura con routing_strategy=broadcast para
    que esa union llegue por fanout a todos los historical_filter workers.

    Los promedios entran como batches "batch" con items
    {payment_format, average_amount}; se emiten igual (msg_type "batch")."""

    def __init__(self) -> None:
        super().__init__()
        self.averages_by_client: Dict[str, List[Any]] = {}

    def join_batch(self, batch: List[Any], client: str) -> None:
        if batch:
            self.averages_by_client.setdefault(client, []).extend(batch)

    def flush(self, client: str) -> Optional[dict]:
        averages = self.averages_by_client.pop(client, [])
        if not averages:
            return None
        return build_batch_message(
            "batch", batch=averages, client=client, msg_id=str(uuid.uuid4())
        )

    def build_eof_message(self, client, msg_id=None):
        # El EOF lo broadcastea BaseWorker por "eof_broadcast"; no hace falta
        # payload especial aca.
        return None

    def get_client_state(self, client: str) -> Any:
        return self.averages_by_client.get(client, [])

    def set_client_state(self, client: str, state: Any) -> None:
        if state: self.averages_by_client[client] = state

class CountStrategy(JoinStrategy):
    """Stateful: accumulates how many rows arrive per client and emits the total
    on flush. Used to count how many rows passed an upstream filter."""

    def __init__(self) -> None:
        super().__init__()
        self.query_number = 5
        self.count_by_client: Dict[str, int] = {}

    def join_batch(self, batch: List[Any], client: str) -> None:
        self.count_by_client[client] = self.count_by_client.get(client, 0) + len(batch)

    def flush(self, client: str) -> Optional[dict]:
        count = self.count_by_client.pop(client, 0)
        return build_results_for_query(
            query_number=self.query_number, batch=[Q5ResultRow(count=count)], eof=True, client=client
        )

    def build_eof_message(self, client, msg_id=None):
        return build_results_for_query(
            query_number=self.query_number, batch=[], eof=True, client=client
        )

    def get_client_state(self, client: str) -> Any:
        return self.count_by_client.get(client, 0)

    def set_client_state(self, client: str, state: Any) -> None:
        if state is not None: self.count_by_client[client] = state
