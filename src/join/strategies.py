import uuid
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional


from src.common.communication.internal import (
    build_batch_message,
    build_results_for_query,
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

    # Default no-op. Strategies que necesiten enriquecer con datos de accounts
    # (ej. BankMaxAmountStrategy para Q2) hacen override.
    def add_accounts(self, batch: List[Any], client: str) -> None:
        pass

    def build_eof_message(self, client, msg_id=None):
        raise NotImplementedError()


class NoStrategy(JoinStrategy):
    """Streaming pass-through: emits each batch as it arrives, buffers nothing."""

    def join_batch(self, batch: List[Any], client: str) -> None:
        if batch:
            self._emit(client, build_q1_result(batch=batch, eof=False, client=client))

    def flush(self, client: str) -> Optional[dict]:
        return None

    def build_eof_message(self, client, msg_id=None):
        return build_q1_result(batch=[], eof=True, client=client)


class QueryResultStrategy(JoinStrategy):
    """Streaming pass-through for query result transactions."""
    
    def __init__(self, query_number: int) -> None:
        super().__init__()
        self.query_number = query_number

    def join_batch(self, batch: List[Any], client: str) -> None:
        if batch:
            self._emit(client, build_results_for_query(query_number=self.query_number, batch=batch, eof=False, client=client))

    def flush(self, client: str) -> Optional[dict]:
        return None

    def build_eof_message(self, client, msg_id=None):
        return build_results_for_query(query_number=self.query_number, batch=[], eof=True, client=client)


# class UnionStrategy(JoinStrategy):
#     """Buffers every row per client and emits the union on flush."""
#
#     def __init__(self) -> None:
#         super().__init__()
#         self.batch_by_client: Dict[str, List[Any]] = {}
#
#     def join_batch(self, batch: List[Any], client: str) -> None:
#         self.batch_by_client.setdefault(client, []).extend(batch)
#
#     def flush(self, client: str) -> Optional[dict]:
#         rows = self.batch_by_client.pop(client, [])
#         if not rows:
#             return None
#         return build_batch_message(
#             "batch", batch=rows, client=client, msg_id=str(uuid.uuid4())
#         )
#
#     def build_eof_message(self, client, msg_id=None):
#         return build_q1_result(batch=[], eof=True, client=client)
#
#
# class CountStrategy(JoinStrategy):
#     """Counts rows per client and emits the total on flush."""
#
#     def __init__(self) -> None:
#         super().__init__()
#         self.count_by_client: Dict[str, int] = {}
#
#     def join_batch(self, batch: List[Any], client: str) -> None:
#         self.count_by_client[client] = self.count_by_client.get(client, 0) + len(batch)
#
#     def flush(self, client: str) -> Optional[dict]:
#         count = self.count_by_client.pop(client, 0)
#         return build_batch_message(
#             "batch", batch=[count], client=client, msg_id=str(uuid.uuid4())
#         )
#
#     def build_eof_message(self, client, msg_id=None):
#         return build_q1_result(batch=[], eof=True, client=client)
#
#
# class BankMaxAmountStrategy(JoinStrategy):
#     """Tracks the max transaction per bank per client, enriching with bank
#     names received via :meth:`add_accounts`; emits the enriched result on flush."""
#
#     def __init__(self) -> None:
#         super().__init__()
#         self.max_per_bank_by_client: Dict[str, Dict[str, Dict[str, Any]]] = {}
#         # bank_id -> bank_name, por cliente. Poblado desde el accounts_queue.
#         self.bank_names_by_client: Dict[str, Dict[Any, str]] = {}
#
#     def add_accounts(self, batch: List[Any], client: str) -> None:
#         bank_names = self.bank_names_by_client.setdefault(client, {})
#         for account in batch:
#             bank_id = getattr(account, "bank_id", None)
#             bank_name = getattr(account, "bank_name", None)
#             if bank_id is not None and bank_name is not None:
#                 bank_names[bank_id] = bank_name
#
#     def join_batch(self, batch: List[Any], client: str) -> None:
#         max_per_bank = self.max_per_bank_by_client.setdefault(client, {})
#         for tx in batch:
#             bank = tx["from_bank"]
#             amount = tx["amount_paid"] or 0.0
#             current = max_per_bank.get(bank)
#
#             if current is None or amount > current["amount_paid"]:
#                 max_per_bank[bank] = {
#                     "from_bank": tx["from_bank"],
#                     "from_account": tx["from_account"],
#                     "amount_paid": amount,
#                 }
#
#     def flush(self, client: str) -> Optional[dict]:
#         entries = self.max_per_bank_by_client.pop(client, {})
#         bank_names = self.bank_names_by_client.pop(client, {})
#         if not entries:
#             return None
#
#         results = []
#         for entry in entries.values():
#             results.append(
#                 {
#                     "bank_name": bank_names.get(entry["from_bank"], entry["from_bank"]),
#                     "from_account": entry["from_account"],
#                     "amount_paid": entry["amount_paid"],
#                 }
#             )
#         return build_batch_message(
#             "batch", batch=results, client=client, msg_id=str(uuid.uuid4())
#         )
#
#     def build_eof_message(self, client, msg_id=None):
#         return build_q1_result(batch=[], eof=True, client=client)
#
#
# class AccountStrategy(JoinStrategy):
#     """Buffers raw account records per client; emits them on flush."""
#
#     def __init__(self) -> None:
#         super().__init__()
#         self.accounts_by_client: Dict[str, List[Dict[str, str]]] = {}
#
#     def join_batch(self, batch: List[Any], client: str) -> None:
#         self.accounts_by_client.setdefault(client, []).extend(batch)
#
#     def flush(self, client: str) -> Optional[dict]:
#         accounts = self.accounts_by_client.pop(client, [])
#         if not accounts:
#             return None
#         return build_batch_message(
#             "batch", batch=accounts, client=client, msg_id=str(uuid.uuid4())
#         )
#
#     def build_eof_message(self, client, msg_id=None):
#         return build_q1_result(batch=[], eof=True, client=client)
