import time
import logging
import threading
from enum import Enum, auto
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """ States of the Circuit Breaker """
    # Circuit Breaker is closed, allowing requests
    CLOSED = auto()
    # Circuit Breaker is open, blocking requests
    OPEN = auto()
    # Circuit Breaker is half-open, allowing limited requests to test recovery
    HALF_OPEN = auto()


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """ Configuration for the Circuit Breaker """
    # Failure threshold: number of failures to trigger OPEN state
    failure_threshold: int = 10
    # Recovery timeout: cooldown period before transitioning to HALF_OPEN
    recovery_timeout_seconds: float = 30.0
    # Success threshold: number of consecutive successes to transition from HALF_OPEN to CLOSED
    success_threshold: int = 3


class CircuitBreakerOpen(Exception):
    """ Custom exception raised when the circuit breaker is open """

    def __init__(self, state: CircuitBreakerState, wait: float):
        self.state = state
        self.wait = wait
        super().__init__(f"Circuit Breaker is {state.name}. Retry after {wait:.1f}s.")


@dataclass()
class CircuitBreakerRuntime:
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    state: CircuitBreakerState = CircuitBreakerState.CLOSED


class CircuitBreaker:
    """ Implements the Circuit Breaker pattern to manage fault tolerance in distributed systems """

    def __init__(self, config: CircuitBreakerConfig = CircuitBreakerConfig()):
        """ Initializes the CircuitBreaker with the given configuration """
        # Initialize the configuration for the circuit breaker
        self._config: CircuitBreakerConfig = config
        # Thread lock for synchronizing state changes
        self._lock = threading.RLock()
        # Initialize runtime state
        self._runtime = CircuitBreakerRuntime()
        # Right now, when you switch to HALF_OPEN, all incoming requests will be allowed —
        # but ideally only one (or a small number) should go through to test recovery.
        # Only one request is allowed during HALF-OPEN, while all others are denied until that one finishes.
        # That’s exactly what a proper circuit breaker should do.
        self._half_open_function_call_in_progress = False

    def __call__(self, func):
        """Allows the CircuitBreaker instance to be used as a decorator."""

        def wrapper(*args, **kwargs):
            self.try_recovery()
            try:
                result = func(*args, **kwargs)
                self.record_success()
                return result
            except Exception as e:
                self.record_failure()
                raise e
            finally:
                # Always allow future HALF-OPEN test after completion
                self._half_open_function_call_in_progress = False

        return wrapper

    def try_recovery(self):
        """ Check if the circuit breaker can transition to HALF-OPEN state """
        with self._lock:
            if self._runtime.state == CircuitBreakerState.OPEN:
                time_duration_seconds_since_last_failure = time.monotonic() - self._runtime.last_failure_time
                if time_duration_seconds_since_last_failure < self._config.recovery_timeout_seconds:
                    raise CircuitBreakerOpen(state=CircuitBreakerState.OPEN,
                                             wait=self._config.recovery_timeout_seconds - time_duration_seconds_since_last_failure)
                self._runtime.state = CircuitBreakerState.HALF_OPEN
                self._half_open_function_call_in_progress = True
                logger.info("Circuit breaker transitioning to HALF-OPEN state.")
            if self._runtime.state == CircuitBreakerState.HALF_OPEN:
                # In HALF-OPEN state, dont allow requests until one passes
                if self._half_open_function_call_in_progress:
                    raise CircuitBreakerOpen(state=CircuitBreakerState.HALF_OPEN, wait=1.0)

    def record_failure(self):
        with self._lock:
            self._half_open_function_call_in_progress = False
            self._runtime.last_failure_time = time.monotonic()
            if self._runtime.state == CircuitBreakerState.CLOSED:
                self._runtime.failure_count += 1
                if self._runtime.failure_count >= self._config.failure_threshold:
                    self._runtime.state = CircuitBreakerState.OPEN
                    logger.warning(f"Failure threshold reached. Circuit OPEN. Failures: {self._runtime.failure_count}")
            elif self._runtime.state == CircuitBreakerState.HALF_OPEN:
                self._runtime.state = CircuitBreakerState.OPEN
                logger.warning("HALF-OPEN test failed. Circuit OPEN.")

    def record_success(self):
        with self._lock:
            self._half_open_function_call_in_progress = False

            if self._runtime.state == CircuitBreakerState.HALF_OPEN:
                self._runtime.success_count += 1
                if self._runtime.success_count >= self._config.success_threshold:
                    self._runtime.state = CircuitBreakerState.CLOSED
                    self._reset()
                    logger.info("Success threshold reached in HALF-OPEN. Circuit CLOSED.")
            elif self._runtime.state == CircuitBreakerState.CLOSED:
                # If a successful call happens in CLOSED state, reset the failure count
                self._reset()

    def _reset(self):
        self._runtime.failure_count = 0
        self._runtime.success_count = 0
