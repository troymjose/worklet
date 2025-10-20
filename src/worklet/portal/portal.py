from typing import Callable, TypeVar, Any, Optional
from src.worklet.worker import runtime
from src.worklet.utils.func import func_utils
from src.worklet.worklet.worklet import Worklet
from src.worklet.executor.models import TaskRetry
from src.worklet.worklet.models import WorkletModel
from src.worklet.worklet.registry import WorkletRegistry
from src.worklet.portal.models import PortalConfig, PortalModel
from src.worklet.utils.circuit_breaker import CircuitBreakerConfig
from src.worklet.portal.teleporter import Teleporter, KafkaProducerConfig

F = TypeVar("F", bound=Callable[..., Any])


class Portal:
    """
    The entrypoint for Worklet background task orchestration.

    A `Portal` is a lightweight gateway that connects your application
    (e.g., FastAPI, Django, Flask) to the Worklet distributed execution engine.
    It integrates with Kafka to schedule and transport background tasks to workers.

    Developers can define async-like background functions simply by
    decorating them with `@portal.teleportable`. These functions are
    automatically serialized, queued, and executed on a background worker.

    Example
    -------
    ```python
    from worklet.portal import Portal

    portal = Portal(
        name="App1",
        topic="user-events",
        bootstrap_servers="localhost:9092",
    )

    @portal.teleportable(retry=3, retry_delay_seconds=2)
    def send_email(to: str, subject: str):
        print(f"Sending email to {to} with subject {subject}")

    # Usage inside your FastAPI or Django route:
    send_email.teleport(to="alice@example.com", subject="Welcome!")
    ```

    Parameters
    ----------
    name : str
        A unique name for your portal instance (typically your app name).

    topic : str
        Kafka topic name to which tasks will be published.

    bootstrap_servers : str
        Kafka broker address(es), e.g. `"localhost:9092"`.

    config : Optional[PortalConfig]
        Optional configuration for fine-tuning producer, retry, and circuit breaker behavior.
    """

    __slots__ = (
        "_name",
        "_topic",
        "_bootstrap_servers",
        "_config",
        "_consumer_group",
        "_worklet_registry",
        "_teleporter",
    )

    def __init__(
            self,
            name: str,
            topic: str,
            bootstrap_servers: str,
            config: PortalConfig | None = None,
    ) -> None:
        """
        Initialize a new Portal instance and prepare Kafka connectivity.

        Raises
        ------
        ValueError
            If `name`, `topic`, or `bootstrap_servers` is empty.
        """
        if not name or not topic or not bootstrap_servers:
            raise ValueError("Portal requires non-empty name, topic, and bootstrap_servers.")

        self._name = name
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._config: PortalConfig = PortalConfig() if config is None else config
        self._worklet_registry = WorkletRegistry()
        self._consumer_group = self._initialize_consumer_group()
        self._teleporter: Teleporter = self._initialize_teleporter()

    # ---------------------------------------------------------------------
    # Internal Initialization
    # ---------------------------------------------------------------------

    def _initialize_consumer_group(self) -> str:
        """Constructs a default consumer group name if not provided in config."""
        env: str = "" if self._config.env is None else f".{self._config.env}"
        default_consumer_group: str = f"worklet.{self._name}-{self._topic}{env}"
        return self._config.kafka_consumer_group or default_consumer_group

    def _initialize_teleporter(self) -> Teleporter:
        """
        Creates and configures the internal Teleporter for Kafka message delivery.
        Includes producer setup and circuit breaker configuration.
        """
        kafka_producer_config: KafkaProducerConfig = KafkaProducerConfig(
            client_id=self._name,
            bootstrap_servers=self._bootstrap_servers,
            topic=self._topic,
            on_delivery=self._config.on_delivery,
            extra_confluent_kafka_config=self._config.kafka_producer_config,
        )

        circuit_breaker_config: CircuitBreakerConfig = CircuitBreakerConfig(
            failure_threshold=self._config.circuit_breaker_failure_threshold,
            recovery_timeout_seconds=self._config.circuit_breaker_recovery_timeout_seconds,
            success_threshold=self._config.circuit_breaker_success_threshold,
        )

        return Teleporter(
            kafka_producer_config=kafka_producer_config,
            max_queue_size=self._config.max_queue_size,
            circuit_breaker_config=circuit_breaker_config,
        )

    # ---------------------------------------------------------------------
    # Developer-Friendly Representations
    # ---------------------------------------------------------------------

    def __str__(self) -> str:
        """Return a developer-friendly string representation of the portal."""
        return (
            f"Portal(name={self._name}, topic={self._topic}, "
            f"bootstrap_servers={self._bootstrap_servers}, "
            f"consumer_group={self._consumer_group})"
        )

    # ---------------------------------------------------------------------
    # Task Registration & Decorator
    # ---------------------------------------------------------------------

    def teleportable(
            self,
            name: Optional[str] = None,
            retry: int = 1,
            retry_delay_seconds: float = 1,
            max_retry_delay_seconds: float = 300.0,
            retry_exceptions: tuple[type[Exception], ...] = (Exception,),
            retry_backoff_factor: float = 2,
            retry_jitter_factor_min: float = 1,
            retry_jitter_factor_max: float = 1.5,
    ) -> Callable[[F], Worklet[F]]:
        """
        Decorator for marking a function as teleportable (background-executable).

        When used, the decorated function is automatically registered
        as a Worklet task and can be executed asynchronously using `.teleport()`.

        Example
        -------
        ```python
        @portal.teleportable(retry=3, retry_delay_seconds=1)
        def generate_invoice(order_id: str):
            # Heavy computation or external API call
            process_invoice(order_id)

        # Later inside your route:
        generate_invoice.teleport(order_id="12345")
        ```

        Parameters
        ----------
        name : Optional[str]
            Custom task ID. If None, uses the function's fully-qualified name.

        retry : int
            Number of retry attempts before marking the task as failed.

        retry_delay_seconds : float
            Initial delay (in seconds) before first retry.

        max_retry_delay_seconds : float
            Maximum backoff delay (in seconds) between retries.

        retry_exceptions : tuple[type[Exception], ...]
            Exception types that trigger retry. Default is all Exceptions.

        retry_backoff_factor : float
            Exponential backoff multiplier for retry intervals.

        retry_jitter_factor_min : float
            Minimum random jitter multiplier for retry delay.

        retry_jitter_factor_max : float
            Maximum random jitter multiplier for retry delay.

        Returns
        -------
        Callable[[F], Worklet[F]]
            The decorator function which wraps the callable into a Worklet.

        Raises
        ------
        ValueError
            If `retry` < 1.

        TypeError
            If applied to a non-callable.
        """
        if retry < 1:
            raise ValueError("Retry count must be >= 1.")

        def decorator(func: F) -> Worklet[F] | None:
            if not callable(func):
                raise TypeError("teleportable decorator can only be applied to callables.")

            # Non-worker (client) mode → return Worklet proxy for async scheduling
            if not runtime.IS_WORKER_RUNNING:
                return Worklet(teleporter=self._teleporter, func=func)

            # Worker (server) mode → register the function for execution
            if runtime.PORTAL != self._name:
                return None

            # Register portal metadata once
            if self._worklet_registry.portal is None:
                portal_model = PortalModel(
                    name=self._name,
                    topic=self._topic,
                    bootstrap_servers=self._bootstrap_servers,
                    consumer_group=self._consumer_group,
                )
                self._worklet_registry.set_portal(portal_model)

            # Create and register the Worklet model for this function
            task_id: str = name or func_utils.get_canonical_func_name(func=func)
            task_retry: TaskRetry = TaskRetry(
                retry=retry,
                retry_delay_seconds=retry_delay_seconds,
                max_retry_delay_seconds=max_retry_delay_seconds,
                exceptions=retry_exceptions,
                backoff_factor=retry_backoff_factor,
                jitter_factor_min=retry_jitter_factor_min,
                jitter_factor_max=retry_jitter_factor_max,
            )

            worklet: WorkletModel = WorkletModel(
                id=task_id,
                portal=self._name,
                func=func,
                retry=task_retry,
            )

            self._worklet_registry.register(worklet=worklet)
            return None

        return decorator
