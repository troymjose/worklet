from typing import Type, Dict, Tuple, Optional
from worklet.executor.models import ExecutorConfig
from worklet.executor.base_executor import BaseExecutor
from worklet.executor.exceptions import ExecutorNotFoundError
from worklet.executor.async_executor import AsyncThreadExecutor

__all__ = ["executor_factory"]

ExecutorRegistry = Dict[str, Type[BaseExecutor]]


class ExecutorFactory:
    """
    Factory for managing and instantiating executor implementations.

    This factory maintains a registry of available executors (e.g., async, thread,
    process-based) and provides a unified interface to create configured instances.

    Usage Example:
        >>> config = ExecutorConfig(concurrency=8)
        >>> executor = executor_factory.get(name="async", config=config)

    Attributes:
        _executors (ExecutorRegistry):
            Internal registry mapping executor names to their classes.
    """

    __slots__ = ("_executors",)

    def __init__(self) -> None:
        """Initialize an empty executor registry."""
        self._executors: ExecutorRegistry = {}

    def register(self, *, name: str, executor: Type[BaseExecutor]) -> None:
        """
        Register an executor class under a unique name.

        Args:
            name (str): The identifier for the executor (e.g., 'async', 'thread').
            executor (Type[BaseExecutor]): The executor class to register.
        """
        self._executors[name.lower()] = executor

    def get(self, *, name: str, config: Optional[ExecutorConfig] = None) -> BaseExecutor:
        """
        Retrieve and instantiate a registered executor.

        Args:
            name (str): The name of the executor to retrieve.
            config (ExecutorConfig, optional): Optional executor configuration.
                If not provided, a default `ExecutorConfig` is used.

        Returns:
            BaseExecutor: An instantiated executor configured for use.

        Raises:
            ExecutorNotFoundError: If no executor is registered under the given name.
        """
        executor_cls = self._executors.get(name.lower())
        if executor_cls is None:
            raise ExecutorNotFoundError(f"No executor registered with name '{name}'")

        return executor_cls(config=config or ExecutorConfig())

    def options(self) -> Tuple[str, ...]:
        """
        Get all available executor names.

        Returns:
            Tuple[str, ...]: A tuple of registered executor identifiers.
        """
        return tuple(self._executors.keys())

    def __contains__(self, name: str) -> bool:
        """Check if an executor is registered with the given name."""
        return name.lower() in self._executors


# Initialize and register default executors
executor_factory = ExecutorFactory()
executor_factory.register(name='async', executor=AsyncThreadExecutor)
