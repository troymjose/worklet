from dataclasses import dataclass
from typing import Any, Callable, TypeVar

from worklet.executor.models import TaskRetry

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class WorkletModel:
    id: str
    portal: str
    func: F
    retry: TaskRetry
