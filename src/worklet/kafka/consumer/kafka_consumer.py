import orjson
import logging
from confluent_kafka import Consumer, KafkaError, Message
from src.worklet.kafka.consumer.models import ConsumerMessage, KafkaConsumerConfig

__all__ = ["KafkaConsumer", ]

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """
    Production-grade Kafka consumer wrapper.

    Args:
        topic (str): Kafka topic to subscribe to.
        bootstrap_servers (str): Kafka bootstrap server list.
        consumer_group (str): Consumer group ID.
        poll_timeout_seconds (int): Poll timeout in seconds.
        config (dict | None): Additional Kafka consumer configuration.
    """

    # Slots are used to limit the attributes of the class to those defined, which can improve performance.
    __slots__ = ("_consumer",
                 "_poll_timeout_seconds",
                 "_running",
                 "_paused_partitions",
                 "_paused")

    def __init__(self, config: KafkaConsumerConfig):
        """ Initializes the KafkaConsumer with topic, bootstrap servers, consumer group, poll timeout, and config."""

        # Copy the dict to protect from unintended shared state
        consumer_init_config: dict = dict(
            config.extra_confluent_kafka_config) if config.extra_confluent_kafka_config else {}
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Kafka consumer custom configurations: {consumer_init_config}")

        consumer_init_config.update({"bootstrap.servers": config.bootstrap_servers,
                                     "group.id": config.consumer_group,
                                     "auto.offset.reset": "earliest",
                                     "enable.auto.commit": False, })
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Kafka consumer overridden configurations: {consumer_init_config}")

        self._consumer = Consumer(consumer_init_config)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Kafka consumer initialized with configurations: {consumer_init_config}")

        self._is_connected(config=config)

        self._consumer.subscribe([config.topic])
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Kafka consumer subscribed to topic: {config.topic}")

        self._poll_timeout_seconds = config.poll_timeout_seconds
        self._running, self._paused, self._paused_partitions = True, False, set()

    def poll(self) -> ConsumerMessage | None:
        """ Polls for a message from Kafka. Returns None if no message is available or if paused. """

        if not self._running:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Kafka consumer closing in progress, skipping poll")
            return None

        if self._paused:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Kafka consumer is paused, skipping poll")
            return None

        msg: Message = self._consumer.poll(self._poll_timeout_seconds)

        if not msg:
            return None

        if msg.error():
            # Handle specific Kafka exceptions
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event - not a real error
                if logger.isEnabledFor(logging.ERROR):
                    logger.error(
                        f"Kafka consumer error: {msg.topic()} [{msg.partition()}] reached end offset {msg.offset()}")
            elif msg.error().code() == KafkaError._UNKNOWN_PARTITION:
                if logger.isEnabledFor(logging.ERROR):
                    logger.error(f"Kafka consumer error: Unknown partition. Rebalancing might be in progress.")
            elif msg.error().code() == KafkaError._TRANSPORT:
                if logger.isEnabledFor(logging.ERROR):
                    logger.error(f"Kafka consumer error: Transport error: {msg.error()}")
            elif msg.error().code() == KafkaError._ALL_BROKERS_DOWN:
                if logger.isEnabledFor(logging.ERROR):
                    logger.error(f"Kafka consumer error: All brokers are down: {msg.error()}")
            elif msg.error():
                if logger.isEnabledFor(logging.ERROR):
                    logger.error(f"Kafka error: {msg.error()}")
            return None

        # parse JSON payload (orjson is fast)
        raw = msg.value()
        if raw is None:
            # no payload — return wrapper with empty dict
            return None
        else:
            try:
                # orjson returns bytes->object; less overhead than json.loads
                data = orjson.loads(raw)
                data['func']['args']=tuple(data['func']['args'])
            except Exception as exc:
                # avoid noisy logs in high-throughput flows; log once at WARN level with context
                if logger.isEnabledFor(logging.WARNING):
                    logger.warning("Failed to parse message at topic=%s partition=%s offset=%s error=%s",
                                   msg.topic(), msg.partition(), msg.offset(), exc)
                # drop / skip malformed message
                return None

        return ConsumerMessage(msg=msg, data=data)

    def close(self):
        """ Closes the Kafka consumer gracefully. """

        self._running = False
        if logger.isEnabledFor(logging.INFO):
            logger.info("Kafka consumer started closing ...")

        try:
            self._consumer.close()
            if logger.isEnabledFor(logging.INFO):
                logger.info("Kafka consumer successfully closed")
        except Exception as exc:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(f"Kafka consumer failed to close. Exception: {exc}")

    def pause(self):
        """ Pauses the consumer by pausing all assigned partitions. """

        try:
            partitions = self._consumer.assignment()
            if partitions:
                self._paused_partitions.update(partitions)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Kafka consumer pausing partitions: {partitions}")
                self._consumer.pause(partitions)
                self._paused = True
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Kafka consumer paused. Partitions: {partitions}")
        except Exception as exc:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(f"Kafka consumer failed to pause partitions. Exception: {exc}")

    def is_paused(self) -> bool:
        """ Returns True if the consumer is currently paused. """

        return self._paused

    def resume(self):
        """ Resumes the consumer by resuming all previously paused partitions. """

        try:
            if self._paused_partitions:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Kafka consumer resuming partitions: {self._paused_partitions}")
                self._consumer.resume(list(self._paused_partitions))
                self._paused_partitions.clear()
                self._paused = False
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Kafka consumer resumed. Partitions: {self._paused_partitions}")
        except Exception as exc:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(f"Kafka consumer failed to resume partitions. Exception: {exc}")

    def commit(self, offsets):
        """ Commits the specified offsets to Kafka. """

        try:
            self._consumer.commit(offsets=offsets, asynchronous=False)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Kafka consumer offsets committed successfully. Offsets: {offsets}")
        except Exception as exc:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(f"Kafka consumer failed to commit offsets. Exception: {exc}")

    def _is_connected(self, config: KafkaConsumerConfig) -> None:
        """
        Validate Kafka connectivity and topic existence before starting the worker.
        Ensures the broker is reachable and the configured topic exists.
        Raises a RuntimeError with clear error messages if checks fail.
        """
        try:
            logger.info(f"🔌 Validating Kafka connection to bootstrap servers: {config.bootstrap_servers}")
            md = self._consumer.list_topics(timeout=5)  # Access the underlying confluent_kafka.Consumer
        except Exception as exc:
            raise RuntimeError(
                f"❌ Unable to connect to Kafka broker at {config.bootstrap_servers}. "
                f"Please ensure Kafka is running and reachable. Details: {exc}"
            ) from exc

        # Check if the topic exists in cluster metadata
        topic = config.topic
        if topic not in md.topics:
            raise RuntimeError(
                f"❌ Kafka topic '{topic}' not found on the broker. "
                f"Ensure it exists and your worker has access permissions."
            )

        logger.info(f"✅ Kafka connection validated. Topic '{topic}' is available.")
