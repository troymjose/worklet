from typing import final
from dataclasses import dataclass, field


@final
@dataclass(frozen=True, slots=True)
class WorkerQueueConfig:
    """ Configuration options for the worker queue """
    max_size: int = 10
    pause_threshold: float = 0.9  # Pause when 90% full
    resume_threshold: float = 0.5  # Resume when 50% full


@final
@dataclass(frozen=True, slots=True)
class KafkaConsumerCommitConfig:
    """ Configuration options for committing Kafka consumer offsets """
    retry: int = 3
    retry_delay_seconds: int = 1
    max_batch_size: int = 100
    max_batch_interval_seconds: int = 5


@final
@dataclass(frozen=True, slots=True)
class WorkerKafkaConfig:
    """ Configuration options for the Kafka consumer """
    extra_confluent_kafka_config: dict[str, str] = field(default_factory=lambda: {
        # fetch batching settings:
        "fetch.min.bytes": 1_024,  # wait until at least 1KB is available to reduce small fetches
        "fetch.wait.max.ms": 100,  # wait up to 100ms for more data (tradeoff latency vs throughput)
        "max.partition.fetch.bytes": 1_048_576,  # 1MB per partition
        # local queue settings:
        "queued.min.messages": 10000,
        # rely on the OS DNS etc for bootstrap refresh:
        "socket.keepalive.enable": True,
    })
    poll_timeout_seconds: float = 1.0
    commit: KafkaConsumerCommitConfig = KafkaConsumerCommitConfig()


@final
@dataclass(frozen=True, slots=True)
class ErrorModel:
    message: str
    code: int
    details: str

    def __str__(self) -> str:
        return f"""
Error
-----
Code    = {self.code}
Message = {self.message}
Details = {self.details}
"""
