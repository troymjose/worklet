import logging
import threading
from typing import Dict
from src.worklet.tracker.base_tracker import Tracker
from src.worklet.kafka.producer.models import KafkaProducerMessage

logger = logging.getLogger(__name__)


# TODO: should we se WeakSet here ?

class InMemoryTracker(Tracker):
    """Thread-safe in-memory tracker for producer messages."""

    def __init__(self) -> None:
        self._tracker: Dict[str, KafkaProducerMessage] = {}
        self._lock = threading.Lock()

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def __len__(self) -> int:
        with self._lock:
            return len(self._tracker)

    def add(self, key: str, value: KafkaProducerMessage) -> None:
        with self._lock:
            self._tracker[key] = value

    def remove(self, key: str) -> None:
        with self._lock:
            del self._tracker[key]  # raises KeyError if missing

    def pop(self, key: str) -> KafkaProducerMessage | None:
        with self._lock:
            return self._tracker.pop(key, None)

    def exists(self, key: str) -> bool:
        with self._lock:
            return key in self._tracker

    def get(self, key: str) -> KafkaProducerMessage | None:
        with self._lock:
            return self._tracker.get(key, None)

    def keys(self) -> set[str]:
        return set(self._tracker.keys())

    def values(self) -> list[KafkaProducerMessage]:
        return list(self._tracker.values())
