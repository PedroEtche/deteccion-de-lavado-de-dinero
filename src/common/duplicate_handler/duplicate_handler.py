class DuplicateHandler:
    """
    Detecta duplicados por (client, sender) usando el maximo msg_id visto.

    Cada sender emite un msg_id entero monotonico creciente. Con prefetch=1 y
    una cola por worker, los mensajes de un mismo sender llegan a este receptor
    en orden creciente. Por lo tanto, si llega un msg_id <= al ultimo visto para
    ese (client, sender), es una reentrega: un duplicado.

    El estado es por cliente (afuera) y por sender (adentro) para acompañar el
    ciclo de vida por cliente (se libera en el EOF) y poder snapshotearlo.
    """

    def __init__(self):
        # last_seen[client_id][sender] = ultimo msg_id procesado (int)
        self.last_seen_per_client: dict[str, dict[str, int]] = {}

    def is_duplicate(self, client_id: str, sender: str, msg_id: int) -> bool:
        # Sin sender o sin msg_id no podemos deduplicar: dejamos pasar.
        # (msg_id puede ser 0, que es valido; por eso comparamos contra None.)
        if sender is None or msg_id is None:
            return False
        last_seen = self.last_seen_per_client.get(client_id)
        if not last_seen:
            return False
        last = last_seen.get(sender)
        if last is None:
            return False
        return msg_id <= last

    def mark_seen(self, client_id: str, sender: str, msg_id: int) -> None:
        if sender is None or msg_id is None:
            return
        per_sender = self.last_seen_per_client.setdefault(client_id, {})
        last = per_sender.get(sender)
        if last is None or msg_id > last:
            per_sender[sender] = msg_id

    def clear_client(self, client_id: str) -> None:
        """Frees memory for a specific client."""
        self.last_seen_per_client.pop(client_id, None)

    def get_state(self, client_id: str) -> dict[str, int]:
        """Exports state to be saved in a snapshot."""
        return dict(self.last_seen_per_client.get(client_id, {}))

    def restore_state(self, client_id: str, saved_state: dict[str, int]) -> None:
        """Restores state from a snapshot upon crash recovery."""
        if saved_state:
            self.last_seen_per_client[client_id] = dict(saved_state)
