import os
import uuid
from dataclasses import dataclass, field
from typing import Callable, Any, Awaitable, Union, final

__all__ = ["ExecutorConfig", "TaskAction", "TaskRetry", "Task", ]

# Type alias for supported callables (sync or async)
ExecutorCallable = Union[Callable[..., Any], Callable[..., Awaitable[Any]]]

# Internal defaults
_DEFAULT_CONCURRENCY: int = min(32, (os.cpu_count() or 1) + 4)
_DEFAULT_GRACEFUL_SHUTDOWN: bool = True
_DEFAULT_SHUTDOWN_TIMEOUT: float = 60.0


@final
@dataclass(frozen=True, slots=True)
class ExecutorConfig:
    """
    Configuration settings for executor behavior and resource management.

    Controls how executors manage concurrency, handle graceful shutdown, and
    enforce timeout policies. This configuration ensures consistent behavior
    across thread-based, process-based, or async-based executors.

    Attributes:
        concurrency (int):
            Maximum number of concurrent workers (threads/processes/tasks).
            Defaults to `min(32, (os.cpu_count() or 1) + 4)`.

        graceful_shutdown (bool):
            Whether to allow ongoing tasks to complete before shutdown.
            If False, running tasks may be abruptly terminated.

        shutdown_timeout_seconds (float):
            Maximum time (in seconds) to wait for graceful shutdown.
            Ignored if `graceful_shutdown` is False.

    Example:
        >>> config = ExecutorConfig(concurrency=8, graceful_shutdown=True, shutdown_timeout_seconds=120)
    """
    concurrency: int = _DEFAULT_CONCURRENCY
    graceful_shutdown: bool = _DEFAULT_GRACEFUL_SHUTDOWN
    shutdown_timeout_seconds: float = _DEFAULT_SHUTDOWN_TIMEOUT


@final
@dataclass(frozen=True, slots=True)
class TaskAction:
    """
    Represents the callable action associated with a task.

    This class encapsulates the function to execute, along with its
    positional and keyword arguments. The function may be synchronous
    or asynchronous.

    Attributes:
        func (Callable[..., Any] | Callable[..., Awaitable[Any]]):
            The function or coroutine function to execute.
        args (tuple[Any, ...]):
            Positional arguments to pass to the function.
        kwargs (dict[str, Any]):
            Keyword arguments to pass to the function.

    Example:
        >>> def add(a, b): return a + b
        >>> action = TaskAction(func=add, args=(2, 3), kwargs={})
        >>> action.func(*action.args, **action.kwargs)
        5
    """
    func: ExecutorCallable
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@final
@dataclass(frozen=True, slots=True)
class TaskRetry:
    """
    Defines retry configuration and exponential backoff policy for a task.

    This class specifies how a task should be retried in the event of a failure,
    including retry limits, delays, exponential backoff parameters, and the
    exception types that trigger a retry.

    Attributes:
        retry (int):
            Maximum number of retry attempts before marking the task as failed.
        retry_delay_seconds (float):
            Initial delay before the first retry attempt (in seconds).
        max_retry_delay_seconds (float):
            Maximum delay between retry attempts (in seconds).
        exceptions (tuple[type[Exception], ...]):
            Exception types that should trigger a retry.
        backoff_factor (float):
            Multiplier applied to the delay after each retry attempt to
            implement exponential backoff.
        jitter_factor_min (float):
            Minimum random jitter multiplier applied to the delay to avoid
            thundering herd effects.
        jitter_factor_max (float):
            Maximum random jitter multiplier applied to the delay.

    Example:
        >>> retry_config = TaskRetry(retry=5, retry_delay_seconds=30, backoff_factor=2.0)
        >>> retry_config.retry
        5
    """
    retry: int = 3
    retry_delay_seconds: float = 60.0
    max_retry_delay_seconds: float = 300.0
    exceptions: tuple[type[Exception], ...] = (Exception,)
    backoff_factor: float = 2.0
    jitter_factor_min: float = 1.0
    jitter_factor_max: float = 1.5


@final
@dataclass(frozen=True, slots=True)
class Task:
    """
    Represents a unit of work to be executed by the Worklet runtime.

    Each Task contains an identifier, the callable action to execute,
    and its retry policy configuration. The Task object is immutable,
    lightweight, and safe for concurrent use across threads or async
    event loops.

    Attributes:
        id (str): Unique identifier for the task. Auto-generated if not provided.
        action (TaskAction): Function and arguments representing the task's work.
        retry (TaskRetry): Retry configuration for the task.

    Example:
        >>> def process_order(order_id): ...
        >>> task = Task(
        ...     action=TaskAction(func=process_order, args=(123,), kwargs={}),
        ...     retry=TaskRetry()
        ... )
        >>> print(task.id)
        'b1a2e3f4-5678-4d90-9a12-abc123def456'
    """
    action: TaskAction
    retry: TaskRetry
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
