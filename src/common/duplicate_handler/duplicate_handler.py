class DuplicateHandler:
    """
    Handles duplicate messages by tracking seen message IDs per client.
    """

    def __init__(self):
        self.seen_msg_ids_per_client = {}

    def is_duplicate(self, client_id: str, msg_id: str) -> bool:
        if not msg_id:
            return False
        return msg_id in self.seen_msg_ids_per_client.get(client_id, set())

    def mark_seen(self, client_id: str, msg_id: str) -> None:
        if not msg_id:
            return
        self.seen_msg_ids_per_client.setdefault(client_id, set()).add(msg_id)

    def clear_client(self, client_id: str) -> None:
        """Frees memory for a specific client."""
        self.seen_msg_ids_per_client.pop(client_id, None)

    def get_state(self, client_id: str) -> list[str]:
        """Exports state to be saved in a snapshot."""
        return list(self.seen_msg_ids_per_client.get(client_id, set()))

    def restore_state(self, client_id: str, saved_state: list[str]) -> None:
        """Restores state from a snapshot upon crash recovery."""
        if saved_state:
            self.seen_msg_ids_per_client[client_id] = set(saved_state)