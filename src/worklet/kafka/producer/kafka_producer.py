import logging
from confluent_kafka import Producer
from typing import TypeVar, Callable, Any
from src.worklet.kafka.producer.models import KafkaProducerMessage, KafkaProducerConfig

__all__ = ["KafkaProducer", ]

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class KafkaProducer:
    """
    High-performance Kafka producer wrapper using confluent_kafka.

    This class provides a lightweight abstraction for producing messages
    to Kafka with idempotent delivery, configurable callbacks, and
    non-blocking operation.
    """

    # Slots are used to limit the attributes of the class to those defined, which can improve performance.
    __slots__ = ('_topic', '_on_delivery', '_producer')

    def __init__(self, config: KafkaProducerConfig) -> None:

        # Initialize the topic to which messages will be published
        self._topic: str = config.topic
        # Set the on_delivery callback function, defaulting to a no-op if not provided
        self._on_delivery: F = config.on_delivery

        if not isinstance(config.extra_confluent_kafka_config, dict):
            raise TypeError("config must be a dictionary")
        # Copy the dict to protect from unintended shared state
        producer_init_config = dict(config.extra_confluent_kafka_config) if config.extra_confluent_kafka_config else {}
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Kafka producer custom configurations: {producer_init_config}")
        producer_init_config.update({"client.id": config.client_id, "bootstrap.servers": config.bootstrap_servers,
                                     "enable.idempotence": True, })
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Kafka producer overridden configurations: {producer_init_config}")
        # Create the Kafka producer instance with the specified configuration
        self._producer: Producer = Producer(producer_init_config)

    def publish(self, data: KafkaProducerMessage, on_delivery: F | None = None) -> None:

        try:
            # Convert ProducerMessageModel to bytes
            value: bytes = data.to_bytes()
            # Produce the message to the specified topic with the value
            self._producer.produce(topic=self._topic,
                                   key=data.id.encode("utf-8"),
                                   value=value,
                                   on_delivery=self._on_delivery if on_delivery is None else on_delivery)
            # triggers callbacks without blocking
            self._producer.poll(0)
        except BufferError as e:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(f"Local producer queue is full: {e}")
        except Exception as e:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(f"Error while producing message: {e}")

    def flush(self, timeout: float = 5.0) -> None:
        self._producer.flush(timeout)
