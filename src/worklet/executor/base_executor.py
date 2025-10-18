import logging
from abc import ABC, abstractmethod
from concurrent.futures import Future
from src.worklet.executor.models import Task

__all__ = ["BaseExecutor", ]

logger = logging.getLogger(__name__)


class BaseExecutor(ABC):
    """
    Abstract base class for all executor implementations.

    Executors must implement `execute` and `shutdown`. Supports context manager
    interface for automatic shutdown.
    """

    __slots__ = ()

    @abstractmethod
    def execute(self, task: Task) -> Future:
        """
        Submit a task for execution.

        Args:
            task (Task): The task to execute.

        Returns:
            Future: A concurrent.futures.Future representing the task result.
        """
        ...

    @abstractmethod
    def shutdown(self) -> None:
        """
        Shut down the executor, optionally waiting for running tasks to finish.
        """
        ...

    def __enter__(self) -> "BaseExecutor":
        """Enable context manager usage."""
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        """Ensure executor shutdown on context exit."""
        if logger.isEnabledFor(logging.INFO):
            logger.info("Executor shutting down...")
        self.shutdown()
        return False
