import math
import time
import queue
import random
import signal
import logging
import threading
from queue import Queue
from typing import Literal
from src.worklet.worker import runtime
from src.worklet.worker.errors import errors
from confluent_kafka import TopicPartition
from src.worklet.portal.models import PortalModel
from src.worklet.worklet.models import WorkletModel
from src.worklet.worklet.registry import WorkletRegistry
from src.worklet.executor.factory import executor_factory
from src.worklet.utils.autodiscovery import AutoDiscovery
from src.worklet.kafka.consumer.kafka_consumer import KafkaConsumer
from src.worklet.executor.models import ExecutorConfig, Task, TaskAction
from src.worklet.worker.models import WorkerQueueConfig, WorkerKafkaConfig
from src.worklet.kafka.consumer.models import ConsumerMessage, KafkaConsumerConfig

logger = logging.getLogger(__name__)


class Worker:
    """ Universal worker that polls tasks from Kafka and executes them using the specified executor with backpressure handling and graceful shutdown support """

    def __init__(self,
                 portal: str,
                 worklets: str = 'worklets',
                 queue_config: WorkerQueueConfig = WorkerQueueConfig(),
                 executor: Literal['async'] = 'async',
                 executor_config: ExecutorConfig = ExecutorConfig(),
                 kafka_consumer_config: WorkerKafkaConfig = WorkerKafkaConfig()):

        self._portal: str = portal
        self._worklets: str = worklets
        self._queue_config: WorkerQueueConfig = queue_config
        self._executor_name: str = executor
        self._executor_config: ExecutorConfig = executor_config
        self._kafka_config: WorkerKafkaConfig = kafka_consumer_config

        runtime.IS_WORKER_RUNNING = True
        runtime.PORTAL = portal

        # Event to signal shutdown
        self._stop_event = threading.Event()

    @staticmethod
    def _autodiscover_worklets(*, worklets_folder_name):
        """ Auto-discover and register worklets from installed packages """
        AutoDiscovery(folder_name=worklets_folder_name).discover()

    @staticmethod
    def _ensure_registries() -> None:
        """ Ensure that the mandatory registries are populated """

        # Raise error if no worklets are registered
        if len(WorkletRegistry()) == 0:
            raise RuntimeError(errors.worklets_not_found)

        # # Raise error if no portals are registered
        # if len(PortalRegistry()) == 0:
        #     raise RuntimeError(errors.portals_not_found)

    def _setup_executor(self):
        """ Setup the executor based on the provided name and configuration """
        # Validate executor name
        if self._executor_name not in executor_factory.options():
            # Raise error if executor is not valid
            raise ValueError(errors.invalid_executor)
        try:
            self._executor = executor_factory.get(name=self._executor_name, config=self._executor_config)
        except Exception as exc:
            raise RuntimeError(f'{errors.executor_init_error}. Exception: {exc}')

    def _setup_kafka_consumer(self):
        """
        Setup the Kafka consumer based on the provided portal data.
        Ensures that the consumer can connect to the Kafka cluster before proceeding.
        """

        try:
            portal_registry_data: PortalModel = WorkletRegistry().portal
        except Exception:
            raise RuntimeError(errors.portals_not_found)

        config = KafkaConsumerConfig(
            topic=portal_registry_data.topic,
            bootstrap_servers=portal_registry_data.bootstrap_servers,
            consumer_group=portal_registry_data.consumer_group,
            poll_timeout_seconds=self._kafka_config.poll_timeout_seconds,
            extra_confluent_kafka_config=self._kafka_config.extra_confluent_kafka_config
        )
        # Initialize consumer
        self._consumer = KafkaConsumer(config=config)

    def _process_queue(self):
        while not self._stop_event.is_set():
            try:
                # Block until item is available
                kafka_consumer_poll_message: ConsumerMessage | None = self._queue.get(timeout=1)

            except queue.Empty:
                continue
            except Exception as _:
                continue  # Optional: log queue timeout or processing error
            else:
                logger.debug("Processing queue message")
                # Only set queue as done. Don't perform commit here, as the task was not processed
                self._queue.task_done()

                if kafka_consumer_poll_message is None:
                    continue

            # TODO: CONVERT kafka_consumer_poll_message to ExecutorTask safely. First fetch the function from worklet registry using the function path
            # Then create the ExecutorTask using the fetched function and the args and kwargs from kafka_consumer_poll_message.data

            try:
                worklet: WorkletModel = WorkletRegistry().get(
                    id=kafka_consumer_poll_message.data.get('func', {}).get('id', ''))
            except:
                continue
            # TODO: Only alow the current portal methods
            try:
                action: TaskAction = TaskAction(func=worklet.func,
                                                args=kafka_consumer_poll_message.data['func'].get('args', tuple()),
                                                kwargs=kafka_consumer_poll_message.data['func'].get('kwargs', {}))
                task: Task = Task(id=kafka_consumer_poll_message.data['id'], action=action, retry=worklet.retry)
            except Exception as e:
                logger.exception("Failed to deserialize data: %s", e)
                task = None

            if task is None:
                # Exit the worker if the contract between the producer and consumer is broken
                logger.exception("Failed to deserialize data from queue: %s", e)
                self.stop()
                break

            # Submit to executor and only mark task_done when it actually finishes
            try:
                future = self._executor.execute(task=task)  # MUST return a Future
            except Exception:
                logger.exception("Executor submission failed")
                # # Only set queue as done. Don't perform commit here, as the task was not processed
                # self._queue.task_done()
                # Continue to the next item in the queue, Dont run tbe below code
                continue

            def _done_callback(_future):
                try:
                    if _future.cancelled():
                        logging.warning("Future was cancelled: %s", _future)
                    elif _future.exception():
                        try:
                            # Re-raising the exception here allows logging the full traceback
                            _future.result()
                        except Exception as e:
                            logging.exception("Future %s raised an exception: %s", _future, e)
                    else:
                        try:
                            result = _future.result()
                            logging.info("Future %s completed with result: %s", _future, result)
                            # TODO: Should we commit everytime? because else error task will keep piling up
                            # Only commit if the task was successful
                            self._commit_queue.put(kafka_consumer_poll_message.msg)
                        except Exception as e:
                            # This handles the unlikely case where accessing the result of a completed future itself raises an exception
                            logging.exception("Error retrieving result from completed future %s: %s", _future, e)
                except Exception as e:
                    logging.exception("Error in done callback for future %s: %s", _future, e)
                finally:
                    # TODO: REVIEW IF THIS IS NEEDED HERE ----->>>> self._queue.task_done()
                    # self._queue.task_done()
                    pass

            future.add_done_callback(_done_callback)

    def _setup_queue_processor(self, queue_config: WorkerQueueConfig):
        """
        Create a thread-safe queue with max size for backpressure
        Calculate pause and resume thresholds based on the provided configuration
        Create and start a background thread to process items from the queue
        """
        # Create a thread-safe queue with max size for backpressure
        self._queue = Queue(maxsize=queue_config.max_size)  # Sync queue with backpressure
        # Calculate pause threshold
        self._pause_threshold = int(queue_config.max_size * queue_config.pause_threshold)
        # Calculate resume threshold
        self._resume_threshold = int(queue_config.max_size * queue_config.resume_threshold)

        # Compute thresholds safely, even for tiny queues
        self._pause_threshold = max(1, math.ceil(queue_config.max_size * queue_config.pause_threshold))

        self._resume_threshold = max(0, min(self._pause_threshold - 1,
                                            math.floor(queue_config.max_size * queue_config.resume_threshold)))

        logger.debug(
            f"Queue thresholds → pause: {self._pause_threshold}, resume: {self._resume_threshold}, max: {queue_config.max_size}"
        )

        # Create the background thread for polling messages from Kafka. This thread will run the _process_queue method.
        self._background_thread = threading.Thread(target=self._process_queue,
                                                   daemon=True,
                                                   name='worklet-worker-background-thread')
        # Start the background thread
        self._background_thread.start()

    def _handle_signal(self, signum, frame):
        logger.info(f"Signal {signum} received")
        self.stop()

    def _register_signal_handlers(self):
        """ Register signal handlers for graceful shutdown on SIGTERM and SIGINT """
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _control_consumer_rate(self):
        """ Pause or resume the Kafka consumer based on the queue size for backpressure handling """

        # Get current queue size
        qsize: int = self._queue.qsize()

        # Pause conditions
        pause_conditions: list = [not self._consumer.is_paused(), qsize >= self._pause_threshold]
        # Resume conditions
        resume_conditions: list = [self._consumer.is_paused(), qsize <= self._resume_threshold]

        if all(pause_conditions):
            self._consumer.pause()
            logger.debug(f"{' 🚨 Kafka Consumer'.ljust(20)} - Paused. Queue size: {qsize}")
        elif all(resume_conditions):
            self._consumer.resume()
            logger.debug(f"{' ✅ Kafka Consumer'.ljust(20)} - Resumed. Queue size: {qsize}")

    def _process_commit_queue_individualy(self, commit_error_retries: int = 3,
                                          commit_error_retry_delay_seconds: int = 1):
        """
        Process the commit queue and commit messages to Kafka with retry logic.
        Continuously attempt to get messages from the commit queue without blocking which keeps main loop responsive.
        If a message is retrieved, attempt to commit it using the Kafka consumer with retries with exponential backoff + jitter on failure.
        If the commit is successful, mark the task as done in the commit queue.
        If the commit fails, log the exception but still mark the task as done to avoid blocking.
        If the commit queue is empty, exit the loop.
        This ensures that all messages that have been processed and are ready for commit are handled in a non-blocking manner, allowing the main worker loop to continue processing new messages.
        The use of task_done() is crucial for proper queue management, especially if queue.join() is used elsewhere to wait for all tasks to be completed.
        This method should be called periodically in the main worker loop to ensure timely commits.
        This approach helps maintain the integrity of message processing and ensures that offsets are committed only after successful processing.
        This method should be called in the main thread to ensure thread safety with the Kafka consumer.
        """
        while True:

            # Non-blocking get from the commit queue
            try:
                message = self._commit_queue.get_nowait()
            except queue.Empty:
                break
            except Exception as _:
                break

            # Retries with exponential backoff + jitter
            for attempt in range(1, commit_error_retries + 1):
                try:
                    # Commit the message to Kafka
                    self._consumer.commit(message=message)
                    break
                except Exception as exc:
                    logger.info(f"Kafka Commit Failed for message: {message}. Attempt: {attempt}. Exception:{exc}")
                    if attempt < commit_error_retries:
                        # Doubles the delay on each retry
                        # This avoids hammering Kafka with retries and gives time for transient issues (e.g., broker failover, network hiccups) to resolve.
                        # Adds a small random "jitter" (between 0 and 0.1 seconds).
                        # Prevents the thundering herd problem: if multiple consumers fail at the same time, they would otherwise retry in sync (e.g., all retry at 2s, then 4s, then 8s).
                        # Jitter spreads out the retries slightly, reducing load spikes on Kafka.
                        delay = commit_error_retry_delay_seconds * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                        # Wait before retrying
                        logger.info(f"Retrying Kafka commit in {delay:.2f} seconds...")
                        time.sleep(delay)
                    else:
                        logger.info(f"Kafka Commit Failed for message: {message}. Attempt: {attempt}. Exception:{exc}")

            # Signal to the queue that a task has been completed.
            # Its primary purpose is to work in conjunction with the queue.join() method to track the progress of tasks
            self._commit_queue.task_done()

    def _process_commit_queue(self):
        """
        Process the commit queue
        Commit messages to Kafka in batches with both size-based and time-based flushing
        Retry logic with exponential backoff + jitter on commit failures
        """
        # Gather messages to commit
        messages_to_commit: list = []
        # Get start time for batch interval
        start_time = time.time()
        # Fetch messages from the commit queue until batch size or interval is reached
        while True:
            try:
                # Non-blocking get from the commit queue
                msg = self._commit_queue.get_nowait()
                # Add message to the batch
                messages_to_commit.append(msg)
                # Mark task as done in the commit queue
                self._commit_queue.task_done()
            except queue.Empty:
                break
            except Exception as exc:
                logger.exception("Unexpected error fetching from commit queue: %s", exc)
                break

            # Stop if batch is full
            if len(messages_to_commit) >= self._kafka_config.commit.max_batch_size:
                break

            # Stop if batch interval exceeded
            if (time.time() - start_time) >= self._kafka_config.commit.max_batch_interval_seconds:
                break

        # Check if there are messages to commit
        if not messages_to_commit:
            return  # nothing to commit

        # # Deduplicate by partition: only commit latest offset
        # offsets_to_commit = {}
        # for msg in messages_to_commit:
        #     tp = (msg.topic, msg.partition)
        #     offsets_to_commit[tp] = max(msg.offset(), offsets_to_commit.get(tp, -1))

        # Deduplicate by partition: only commit latest offset
        offsets_to_commit = {}
        for msg in messages_to_commit:
            # Use TopicPartition object as the key for clarity
            tp = TopicPartition(msg.topic(), msg.partition())
            offsets_to_commit[tp] = max(msg.offset(), offsets_to_commit.get(tp, -1))

        # Convert the dictionary to the required list of TopicPartition objects
        # and add 1 to the offset since Kafka commits the *next* offset to be consumed
        commit_list = [
            TopicPartition(tp.topic, tp.partition, offset + 1)
            for tp, offset in offsets_to_commit.items()
        ]
        # Retry with exponential backoff + jitter
        for attempt in range(1, self._kafka_config.commit.retry + 1):
            try:
                self._consumer.commit(offsets=commit_list)
                logger.debug("✅ Committed offsets: %s", offsets_to_commit)
                break
            except Exception as exc:
                logger.warning(
                    f"⚠️ Kafka commit batch failed. Attempt: {attempt}. Error: {exc}"
                )
                if attempt < self._kafka_config.commit.retry:
                    delay = (self._kafka_config.commit.retry_delay_seconds * (
                            2 ** (attempt - 1)) + random.uniform(0, 0.1))
                    logger.info(f"Retrying Kafka commit in {delay:.2f}s...")
                    time.sleep(delay)
                else:
                    logger.error(
                        "❌ Commit failed after retries. Offsets lost: %s",
                        offsets_to_commit,
                    )

    def start(self):

        # Auto-discover and register worklets
        self._autodiscover_worklets(worklets_folder_name=self._worklets)

        # Ensure registries are populated
        self._ensure_registries()

        # Setup executor
        self._setup_executor()

        # Setup Kafka consumer
        self._setup_kafka_consumer()

        # Setup queue processor for backpressure handling
        self._setup_queue_processor(queue_config=self._queue_config)

        # Store completed messages awaiting commit
        # Kafka consumers are not thread-safe.
        # A _commit_queue is introduced to hand over completed messages from the executor thread to the polling thread for safe commit.
        self._commit_queue = Queue()

        # Register signal handlers for graceful shutdown
        self._register_signal_handlers()

        """ Start the worker to poll messages from Kafka and process them """
        logging.info("Worklet worker started...")
        try:
            while not self._stop_event.is_set():
                try:
                    # Control consumer rate based on queue size for backpressure handling
                    self._control_consumer_rate()
                    # Poll Kafka for new messages
                    data = self._consumer.poll()
                    if data:
                        logger.debug(f"Received data: {data}")
                        # This will block if the queue is full, providing backpressure to Kafka consumer
                        self._queue.put(data, block=True)
                    # Flush commits every loop iteration
                    self._process_commit_queue()
                except Exception:
                    logger.exception("Kafka polling error")

        except KeyboardInterrupt:
            logging.info("🛑 Worker shutting down...")

        finally:
            self.stop()

    def stop(self):
        logging.info("🔁 Waiting for queue to drain...")
        self._stop_event.set()
        self._queue.join()
        logging.info("🧹 Cleaning up...")
        try:
            self._process_commit_queue()
        except Exception:
            logger.exception("Error flushing commits")
        try:
            self._consumer.close()
        except Exception:
            logger.exception("Error closing consumer")

        try:
            self._executor.shutdown()
        except:
            pass

        if self._background_thread.is_alive():
            self._background_thread.join()
        logging.info("✅ Shutdown complete.")
