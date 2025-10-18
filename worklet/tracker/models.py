from enum import Enum, auto
from dataclasses import dataclass
from typing import Any, Callable, TypeVar, final

__all__ = ["TrackerType", "TrackerConfig", ]

F = TypeVar("F", bound=Callable[..., Any])


class TrackerType(Enum):
    IN_MEMORY = auto()
    REDIS = auto()


@final
@dataclass(frozen=True, slots=True)
class TrackerConfig:
    type: TrackerType
    redis_url: str
