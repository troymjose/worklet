from typing import Dict, Type
from src.worklet.tracker.base_tracker import Tracker
from src.worklet.tracker.redis_tracker import RedisTracker
from src.worklet.tracker.in_memory_tracker import InMemoryTracker


class TrackerFactory:
    """ Factory for creating trackers based on the provided type """

    def __init__(self):
        # Dictionary to hold registered trackers
        self._trackers: Dict[str, Type[Tracker]] = {}

    def register(self, *, type: str, tracker: Type[Tracker]) -> None:
        """ Register a new tracker with the factory """
        self._trackers[type.lower()] = tracker

    def get(self, *, type: str) -> Tracker:
        """ Get the tracker class by type """
        # Look up the tracker class by type
        tracker: Type[Tracker] | None = self._trackers.get(type.lower(), None)
        # If not found, raise an error
        if tracker is None:
            raise ValueError(type)
        # Return an instance of the tracker
        return tracker()

    def options(self) -> tuple[str, ...]:
        """ Return a list of available tracker types """
        return tuple(self._trackers.keys())


# Initialize the tracker factory
tracker_factory = TrackerFactory()
# Register the AsyncThreadtracker with the factory
tracker_factory.register(type='in_memory', tracker=InMemoryTracker)
tracker_factory.register(type='redis', tracker=RedisTracker)
