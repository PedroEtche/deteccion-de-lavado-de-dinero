from abc import ABC, abstractmethod
from datetime import date
from typing import Any, List, Optional

class JoinStrategy(ABC):
    """Abstract strategy for grouping batches of messages."""

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        raise NotImplementedError()


class NoStrategy(JoinStrategy):
    """A strategy that returns the input batch unchanged."""

    def __str__(self) -> str:
        return "NoStrategy"

    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        return batch

class CountStrategy(JoinStrategy):
    def __init__(self):
        self.count_by_client = {}

    def __str__(self) -> str:
        return f"CountStrategy"

    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        if client is None:
            raise ValueError("client is required for CountStrategy")
        self.count_by_client[client] = self.count_by_client.get(client, 0) + len(batch)
        return [self.count_by_client[client]]
    
    def get_count_for_client(self, client: str) -> int:
        return self.count_by_client.get(client, 0)
    
class PaymentFormatAverageStrategy(JoinStrategy):
    def __str__(self) -> str:
        return "PaymentFormatAverageStrategy"
    
    def join_batch(self, batch: List[Any], client: Optional[str] = None) -> List[Any]:
        pass
            