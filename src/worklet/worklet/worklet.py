import logging
from functools import update_wrapper
from typing import TypeVar, Callable, Any, Generic
from src.worklet.worker import runtime
from src.worklet.utils.func import func_utils
from src.worklet.portal.teleporter import Teleporter
from src.worklet.kafka.producer.models import KafkaProducerMessage, KafkaProducerMessageFunction

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class WorkletMeta(type):
    def __call__(cls, teleporter: Teleporter, func: Callable) -> F:
        if not func:
            raise ValueError("Worklet must wrap a callable function.")
        # Ensure the function is callable
        if not callable(func):
            raise TypeError("Worklet must wrap a callable.")
        # Create an instance of the Worklet class with the provided Kafka producer and function
        instance = super().__call__(teleporter, func)
        # Update the wrapper to maintain the original function's metadata
        update_wrapper(instance, func)
        return instance


class Worklet(Generic[F], metaclass=WorkletMeta):
    """ A Worklet is a callable that can be executed asynchronously via Kafka """

    def __init__(self, teleporter: Teleporter, func: Callable) -> None:
        """ Initialize the Worklet with a Kafka producer and a function."""
        self._teleporter: Teleporter = teleporter
        self._func: F = func

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def teleport(self, *args, **kwargs) -> None:
        """ Send this worklet to Kafka for remote/background execution """

        if runtime.IS_WORKER_RUNNING:
            return

        logger.debug(
            f"Teleporting worklet {func_utils.get_canonical_func_name(func=self._func)} with args={args} kwargs={kwargs}")

        self._teleporter.teleport(message=KafkaProducerMessage(
            func=KafkaProducerMessageFunction(id=func_utils.get_canonical_func_name(func=self._func), args=args,
                                              kwargs=kwargs)))
