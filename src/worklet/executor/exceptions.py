from src.worklet.executor.models import Task

__all__ = ["ExecutorShutdownInProgressError",
           "ExecutorConfigurationError",
           "ExecutorTaskError",
           "ExecutorNotFoundError",
           "RetryError", ]


class TaskError(Exception):
    """
    Base exception for all task-related errors in the executor.

    Use this as a catch-all for any task failures.
    """
    __slots__ = ()


class ExecutorError(Exception):
    """
    Base exception for all executor-related errors.

    Use this as a catch-all for executor configuration, registration,
    or runtime errors.
    """
    __slots__ = ()


class ExecutorShutdownInProgressError(TaskError):
    """
    Raised when a task is submitted while the executor is shutting down.

    Attributes:
        task (Task): The task that was attempted during shutdown.

    Example:
        >>> raise ExecutorShutdownInProgressError(task)
    """

    __slots__ = ("task",)

    def __init__(self, task: Task):
        self.task = task
        message = (
            f"TaskError\n"
            f"Exception: ExecutorShutdownInProgressError\n"
            f"Message: Task cannot be submitted. Executor is shutting down.\n\n"
            f"Task Details\n"
            f"----\n"
            f"Id     : {task.id}\n"
            f"Name   : {task.action.func.__name__}\n"
            f"Args   : {task.action.args}\n"
            f"Kwargs : {task.action.kwargs}\n"
        )
        super().__init__(message)


class ExecutorConfigurationError(ExecutorError, TypeError):
    """
    Raised when an invalid executor configuration is provided.

    Example:
        >>> raise ExecutorConfigurationError("Concurrency must be > 0")
    """
    __slots__ = ()


class ExecutorTaskError(ExecutorError, TypeError):
    """
    Raised when an invalid task definition is provided to the executor.

    Example:
        >>> raise ExecutorTaskError("Task.args must be a tuple")
    """
    __slots__ = ()


class ExecutorNotFoundError(ExecutorError):
    """
    Raised when a requested executor type is not registered in the factory.

    Example:
        >>> raise ExecutorNotFoundError("No executor registered with name 'async'")
    """
    __slots__ = ()


class RetryError(Exception):
    """Raised when a task fails after all retry attempts."""
    __slots__ = ("func_name", "last_exception", "attempts")

    def __init__(self, func_name: str, last_exception: Exception, attempts: int):
        self.func_name = func_name
        self.last_exception = last_exception
        self.attempts = attempts
        super().__init__(f"{func_name} failed after {attempts} attempts: {last_exception}")
