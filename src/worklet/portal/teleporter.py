import queue
import asyncio
import logging
import threading
from src.worklet.tracker.factory import tracker_factory
from src.worklet.executor.factory import executor_factory
from src.worklet.tracker.in_memory_tracker import InMemoryTracker
from src.worklet.kafka.producer.kafka_producer import KafkaProducer
from src.worklet.executor.models import Task, TaskAction, TaskRetry, ExecutorConfig
from src.worklet.kafka.producer.models import KafkaProducerConfig, KafkaProducerMessage
from src.worklet.utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpen, CircuitBreakerConfig

logger = logging.getLogger(__name__)


class Teleporter:
    """
    Teleporter handles message publishing with retry, async executor, and circuit breaker.
    Decouples KafkaProducer from high-level logic.
    Handles its own background worker thread for processing the retry queue.
    """

    __slots__ = (
        "_producer", "_circuit_breaker", "_queue", "_shutdown_event", "_worker_thread", "_tracker", "_executor")

    def __init__(self,
                 kafka_producer_config: KafkaProducerConfig,
                 max_queue_size,
                 circuit_breaker_config: CircuitBreakerConfig = CircuitBreakerConfig()):

        self._producer = KafkaProducer(config=kafka_producer_config)
        self._circuit_breaker = CircuitBreaker(config=circuit_breaker_config)
        self._queue: queue.Queue[str] = queue.Queue(maxsize=max_queue_size)

        self._shutdown_event = threading.Event()
        self._tracker: InMemoryTracker = tracker_factory.get(type='in_memory')
        # Restore any pending messages from tracker before starting worker
        self._restore_pending_tasks()
        # Start background worker thread
        self._worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self._worker_thread.start()
        self._executor = executor_factory.get(name='async', config=ExecutorConfig())

    # -------------------------------------------------------------------------
    # 🧩 Internal recovery logic
    # -------------------------------------------------------------------------
    def _restore_pending_tasks(self) -> None:
        """
        Restore pending tasks from the tracker backend into the processing queue.

        This ensures that tasks persisted in the tracker (e.g., after a crash
        or restart) are recovered and re-sent automatically.
        """
        try:
            pending_tasks = self._tracker.keys()
            for task_id in pending_tasks:
                try:
                    self._queue.put_nowait(task_id)
                except queue.Full:
                    logger.warning(f"Queue full during recovery. {task_id} deferred.")
                    break
            if pending_tasks:
                logger.info(f"Restored {len(pending_tasks)} pending tasks from tracker backend")
            else:
                logger.info("No pending tasks found in tracker backend.")
        except Exception as e:
            logger.exception(f"Failed to restore tasks from tracker: {e}")

    def teleport(self, message: KafkaProducerMessage):
        """Push message into the retry queue for background delivery."""
        if self._shutdown_event.is_set():
            raise RuntimeError("Teleporter is shutting down, cannot accept new messages.")
        self._tracker.add(message.id, message)
        self._queue.put(message.id)

    def _process_queue(self):
        while not self._shutdown_event.is_set():
            try:
                message_id = self._queue.get(timeout=1)
            except queue.Empty:
                message = None
                continue

            message = self._tracker.get(key=message_id)

            if not message:
                continue

            retry: TaskRetry = TaskRetry(retry=0)
            action: TaskAction = TaskAction(func=self._send_once, args=(message,), kwargs={})
            task: Task = Task(id=message.id, action=action, retry=retry)

            # Submit actual send attempt as executor task
            self._executor.execute(task=task)

    async def _send_once(self, message: KafkaProducerMessage):
        """Single send attempt with circuit breaker handling + retries."""
        try:
            self._circuit_breaker(lambda: self._producer.publish(message))()
        except CircuitBreakerOpen as exc:
            logger.warning(f"[Breaker Open] Delaying message {message.id}")
            await asyncio.sleep(exc.wait)
            self._queue.put(message.id)  # requeue
        except Exception as e:
            logger.error(f"[Teleport Failed] {message.id} → {e}")
            await asyncio.sleep(1)
            self._queue.put(message.id)  # requeue
        else:
            logger.debug(f"[Teleport Success] {message.id}")
            self._tracker.remove(message.id)

    def stop(self):
        """Gracefully stop the Teleporter and ensure all pending tasks are handled."""
        logger.info("Stopping Teleporter...")

        # 1️⃣ Signal the worker to stop
        self._shutdown_event.set()

        # 2️⃣ Wait for executor tasks to complete first
        # (ensures no tasks are lost)
        try:
            self._executor.shutdown(wait=True)
        except Exception as e:
            logger.warning(f"Executor shutdown issue: {e}")

        # 3️⃣ Now wait for the worker thread to finish gracefully
        self._worker_thread.join(timeout=5)

        logger.info(f"Teleporter stopped.")
