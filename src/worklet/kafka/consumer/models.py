from dataclasses import dataclass
from confluent_kafka import Message
from typing import Mapping, Any, Dict, final

__all__ = ["KafkaConsumerConfig", "ConsumerMessage", ]

@final
@dataclass(frozen=True, slots=True)
class KafkaConsumerConfig:
    """
    Immutable configuration object for initializing a Kafka consumer.

    This dataclass encapsulates all configuration parameters required to
    construct and manage a `KafkaConsumer` instance. It provides a strongly-typed,
    immutable, and memory-efficient structure for storing consumer settings.

    Attributes:
        topic (str):
            Name of the Kafka topic to subscribe to.

        bootstrap_servers (str):
            Comma-separated list of Kafka broker addresses, e.g.
            "localhost:9092,broker2:9092".

        consumer_group (str, optional):
            Consumer group identifier used for offset management and
            load-balanced consumption. Defaults to `"worklet-group"`.

        poll_timeout_seconds (float, optional):
            Timeout (in seconds) for each poll operation.
            Controls how long the consumer waits for new messages before returning `None`.
            Defaults to `1.0`.

        extra_confluent_kafka_config (Dict[str, Any] | None, optional):
            Additional advanced configuration parameters supported by
            the `confluent_kafka.Consumer` client (e.g. SSL, SASL, and tuning options).
            These values override defaults when initializing the consumer.
            Defaults to `None`.

    Example:
        >>> config = KafkaConsumerConfig(
        ...     topic="worklet-tasks",
        ...     bootstrap_servers="localhost:9092",
        ...     consumer_group="worker-group-1",
        ...     poll_timeout_seconds=0.5,
        ...     extra_confluent_kafka_config={"enable.auto.commit": False}
        ... )
    """
    topic: str
    bootstrap_servers: str
    consumer_group: str = "worklet-group"
    poll_timeout_seconds: float = 1.0
    extra_confluent_kafka_config: Dict[str, Any] | None = None

@final
@dataclass(frozen=True, slots=True)
class ConsumerMessage:
    """
    Immutable Kafka Consumer Message wrapper.

    Attributes:
        msg (Message): Raw Kafka message object from Confluent Kafka client.
        data (dict): Deserialized payload (typically JSON-decoded).
    """
    msg: Message
    data: Mapping[str, Any]
