import uuid
import orjson
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, Callable, TypeVar, final

__all__ = ["KafkaProducerConfig", "KafkaProducerMessage", "KafkaProducerMessageFunction", ]

F = TypeVar("F", bound=Callable[..., Any])

@final
@dataclass(frozen=True, slots=True)
class KafkaProducerConfig:
    """ Represents a function by its id, args, and kwargs. """
    client_id: str
    bootstrap_servers: str
    topic: str
    on_delivery: Optional[F] = None
    extra_confluent_kafka_config: Dict[str, Any] = field(default_factory=dict)

@final
@dataclass(frozen=True, slots=True)
class KafkaProducerMessageFunction:
    """ Represents a function by its id, args, and kwargs. """
    id: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

@final
@dataclass(frozen=True, slots=True)
class KafkaProducerMessage:
    """ Kafka Producer Message Model with serialization to bytes functionality using orjson. """
    id: str = field(default_factory=lambda: str(uuid.uuid4()), init=False)
    func: KafkaProducerMessageFunction

    def to_bytes(self) -> bytes:
        """
        Serializes the dataclass instance to bytes using orjson with explicit options
        and robust error handling.
        """
        try:
            # Use OPT_SERIALIZE_DATACLASS for explicit native serialization.
            return orjson.dumps(self, option=orjson.OPT_SERIALIZE_DATACLASS)
        except orjson.JSONEncodeError as e:
            # Add logging for production environments
            print(f"Serialization error: {e}")
            raise
