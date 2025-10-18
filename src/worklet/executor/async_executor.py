import os
import random
import time
import asyncio
import logging
import threading
from weakref import WeakSet
from concurrent.futures import Future
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from src.worklet.executor.base_executor import BaseExecutor
from src.worklet.executor.models import ExecutorConfig, Task
from src.worklet.executor.exceptions import ExecutorShutdownInProgressError, RetryError

__all__ = ["AsyncThreadExecutor", ]

logger = logging.getLogger(__name__)


class AsyncThreadExecutor(BaseExecutor):
    """
    An asynchronous executor optimized for coroutine tasks with optional support for
    synchronous callables.

    This executor is primarily designed to run **async functions (coroutines)** efficiently
    in a dedicated asyncio event loop running in a background thread. Concurrency is controlled
    via an asyncio semaphore, allowing multiple coroutines to run concurrently without
    blocking the event loop.

    Synchronous (blocking) functions can also be submitted. These are executed in a
    thread pool to prevent blocking the event loop, but note that once a synchronous task
    has started in the thread pool, it **cannot be cancelled or stopped** until it completes.

    Key Features:
        - Optimized for async-first workloads.
        - Synchronous tasks are run safely in a thread pool.
        - Graceful or forced shutdown of the executor.
        - Tracks running asyncio tasks for proper cleanup.
        - Provides Futures for all task submissions, enabling easy integration with
          both async and sync code.

    Methods:
        execute(task: Task) -> Future:
            Submit a task for execution. Returns a Future representing the task result.
        shutdown() -> None:
            Gracefully shuts down the executor, optionally waiting for running tasks
            to complete. Already-started synchronous tasks in the thread pool cannot
            be interrupted.

    Usage Example:
        >>> from src.worklet import ExecutorConfig
        >>> from src.worklet import Task
        >>> config = ExecutorConfig(concurrency=8, graceful_shutdown=True, shutdown_timeout_seconds=30)
        >>> executor = AsyncThreadExecutor(config=config)
        >>> async def my_coroutine(x, y):
        ...     return x + y
        >>> task = Task(action=my_coroutine, args=(1, 2), kwargs={})
        >>> future = executor.execute(task)
        >>> result = await asyncio.wrap_future(future)

    Notes:
        - Async tasks are preferred and run directly in the event loop for maximum efficiency.
        - Synchronous tasks are executed in a thread pool to avoid blocking the loop.
        - Once a synchronous task has started, it cannot be cancelled, even during shutdown.
        - Shutdown behavior:
            - `graceful_shutdown=True` waits for running tasks to finish within a timeout.
            - `graceful_shutdown=False` cancels all pending tasks but running threadpool tasks continue.
        - Tasks submitted after shutdown initiation will immediately raise
          `ExecutorShutdownInProgressError`.
    """

    __slots__ = ("_config",
                 "_shutdown_event",
                 "_loop",
                 "_semaphore",
                 "_thread_pool_executor",
                 "_background_thread",
                 "_async_tasks",
                 "_async_tasks_lock",)

    def __init__(self, config: ExecutorConfig = ExecutorConfig()) -> None:
        """ Initialize the AsyncThreadExecutor with the given configuration """

        # Configuration for the executor
        self._config: ExecutorConfig = config
        # Event to signal shutdown
        self._shutdown_event = threading.Event()
        # Create a new event loop for the background thread
        self._loop = asyncio.new_event_loop()
        # Semaphore to limit the number of concurrent tasks in the event loop
        self._semaphore = asyncio.Semaphore(config.concurrency)
        # Thread pool executor to run blocking tasks in separate threads
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) + 4))
        # Create the background thread that will run the event loop. Also sets up the event loop in that thread.
        self._background_thread: threading.Thread = self._create_background_thread_with_event_loop()
        # Start the background thread
        self._background_thread.start()
        # Track all running asyncio tasks for proper shutdown
        self._async_tasks: WeakSet[asyncio.Task] = WeakSet()
        # Lock to protect access to the_async_tasks set
        self._async_tasks_lock = threading.Lock()

    def _set_and_start_loop(self) -> None:
        """ Set the event loop for the background thread and start it and run forever """

        # Explicitly set an event loop as the current event loop for the current OS thread
        asyncio.set_event_loop(self._loop)
        # Start the event loop and keep it running indefinitely
        self._loop.run_forever()

    def _create_background_thread_with_event_loop(self) -> threading.Thread:
        """
        Create thread and sets the thread’s target function to _set_and_start_loop
        _set_and_start_loop is a method that starts and keeps an asyncio event loop running indefinitely.
        Marks the thread as a daemon thread, meaning it will automatically exit when the main program exits.
        """

        # Create and return a background thread to run the event loop
        return threading.Thread(target=self._set_and_start_loop,
                                daemon=True,
                                name=f'worklet-async-thread-executor-background-thread-{id(self)}')

    @staticmethod
    def compute_delay(task: Task, attempt: int) -> float:
        """
        Compute the retry delay for a task based on its retry configuration.

        This method calculates the next delay duration before retrying a failed task.
        It uses exponential backoff with optional jitter (randomization) to avoid
        synchronized retry storms when multiple workers fail concurrently.

        The delay grows exponentially with each attempt, is capped at a maximum
        value (`max_retry_delay_seconds`), and then randomized by applying a
        jitter multiplier between `jitter_factor_min` and `jitter_factor_max`.

        Args:
            task (Task):
                The task object containing retry configuration parameters.
            attempt (int):
                The current retry attempt count (starting from 1 for the first retry).

        Returns:
            float:
                The computed delay (in seconds) to wait before the next retry.

        Example:
            >>> delay = RetryPolicy.compute_delay(task, attempt=2)
            >>> print(f"Next retry in {delay:.2f}s")

        Notes:
            - This method does **not** perform any sleeping or scheduling; it only
              returns the computed delay value.
            - Uses exponential backoff with jitter, a best-practice approach for
              distributed systems to minimize load spikes during mass retries.
        """
        # Base exponential backoff calculation:
        calculated_backoff = (
                task.retry.retry_delay_seconds * (task.retry.backoff_factor ** (attempt - 1))
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Calculated delay before jitter: {calculated_backoff:.2f}s")

        # Cap the backoff delay at the configured maximum:
        capped_backoff = min(calculated_backoff, task.retry.max_retry_delay_seconds)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Delay after applying max limit: {capped_backoff:.2f}s")

        # Apply a random jitter factor to avoid retry collisions across workers:
        jitter = random.uniform(task.retry.jitter_factor_min, task.retry.jitter_factor_max)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Jitter factor: {jitter:.2f}")

        # Final delay is the capped backoff adjusted by the jitter multiplier:
        delay = capped_backoff * jitter
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Final delay after applying jitter: {delay:.2f}s")

        return delay

    @staticmethod
    async def async_retry(task: Task):
        """
        Execute a task with automatic asynchronous retries and exponential backoff.

        This coroutine attempts to execute the task's callable function multiple times
        (as configured in the task’s retry policy). If the task fails with a retryable
        exception, it will delay using exponential backoff with jitter before retrying.

        The retry loop is fully asynchronous and non-blocking — suitable for high-
        concurrency workloads where tasks may fail transiently due to network or I/O
        issues.

        Args:
            task (Task):
                The task instance containing the callable and retry configuration.

        Raises:
            RetryError:
                If the task fails permanently after all retry attempts.
            asyncio.CancelledError:
                If the coroutine is externally cancelled before completion.
            Exception:
                Any unexpected error not covered by the retry policy.

        Returns:
            Any:
                The result of the task’s successful execution.

        Notes:
            - Cancellation requests (`CancelledError`) are propagated immediately.
            - Logs each retry attempt and final failure for traceability.
            - Implements exponential backoff with jitter — standard best practice for
              distributed systems resilience.
        """
        try:
            total_attempts = max(1, task.retry.retry + 1)
            for attempt in range(1, total_attempts + 1):
                try:
                    # Attempt to execute the task (supports async functions)
                    return await task.action.func(*task.action.args, **task.action.kwargs)

                except asyncio.CancelledError:
                    # Always propagate cancellations immediately (best practice)
                    raise

                except task.retry.exceptions as exc:
                    # Retry only for configured exception types
                    if attempt < task.retry.retry:
                        # Compute dynamic backoff delay
                        delay = AsyncThreadExecutor.compute_delay(task=task, attempt=attempt)

                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"Task {task.id} failed on attempt {attempt}/{task.retry.retry}: {exc}. "
                                f"Retrying in {delay:.2f}s."
                            )

                        # Non-blocking sleep before retrying
                        await asyncio.sleep(delay)

                    else:
                        # Exhausted all retries → log and raise final RetryError
                        if logger.isEnabledFor(logging.ERROR):
                            logger.exception(f"Task {task.id} permanently failed after {task.retry.retry} attempts.")
                        raise RetryError(task.id, exc, task.retry.retry) from exc

        except Exception as e:
            # Final safety net for unexpected errors
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(f"Task {task.id} failed after retries: {e}")
            raise

    @staticmethod
    def sync_retry(task: Task):
        """
        Executes a synchronous (blocking) task function with automatic retry support.

        This method runs the given task function multiple times if it raises a
        retry-eligible exception, applying exponential backoff and random jitter
        between retries. The retry configuration is defined in the `Task.retry`
        field, which controls the number of attempts, delay intervals, backoff
        multiplier, and maximum retry limit.

        The function ensures:
          • Safe propagation of unrecoverable exceptions as `RetryError`.
          • Configurable retry behavior with delay and jitter.
          • Structured logging for observability and diagnostics.

        Args:
            task (Task):
                The Task object containing:
                - `action`: a `TaskAction` defining the callable and its arguments.
                - `retry`: a `TaskRetry` object defining retry parameters (count,
                  delay, exceptions, etc.).
                - `id`: a unique identifier for traceability.

        Raises:
            RetryError:
                Raised when the task permanently fails after exhausting all retries.
            Exception:
                Propagates any unexpected exception that occurs outside the retry
                control loop (e.g., coding errors, system-level failures).

        Returns:
            Any:
                The return value of the successfully executed task function.

        Notes:
            • This method is designed for synchronous/blocking functions.
            • For async coroutines, use `AsyncThreadExecutor.async_retry()`.
            • Logging levels:
                - DEBUG → retry progress and delay details
                - ERROR/EXCEPTION → terminal failures with full stack trace
        """
        try:
            # Iterate through all retry attempts, starting from 1 up to the configured limit.
            total_attempts = max(1, task.retry.retry + 1)
            for attempt in range(1, total_attempts + 1):
                try:
                    # Attempt to execute the task function.
                    return task.action.func(*task.action.args, **task.action.kwargs)

                except task.retry.exceptions as exc:
                    # If the exception type matches the retry policy.
                    if attempt < task.retry.retry:
                        # Compute the next delay with exponential backoff and jitter.
                        delay = AsyncThreadExecutor.compute_delay(attempt=attempt, task=task)

                        # Log retry details (only if DEBUG logging is enabled for performance).
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(
                                f"{task.id} failed on attempt {attempt}/{task.retry.retry}: {exc}. "
                                f"Retrying in {delay:.2f}s"
                            )

                        # Wait before the next retry attempt.
                        time.sleep(delay)
                    else:
                        # Log the terminal failure after all retry attempts are exhausted.
                        if logger.isEnabledFor(logging.ERROR):
                            logger.exception(
                                f"{task.id} permanently failed after {task.retry.retry} attempts"
                            )
                        # Wrap the last exception into a RetryError for consistent error handling.
                        raise RetryError(task.id, exc, task.retry.retry) from exc

        except Exception as e:
            # Catch and log any unhandled exception that occurs outside the retry loop.
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(f"Task {task.id} failed after retries: {e}")
            raise

    async def _execute_async(self, task: Task) -> Future:
        """
        Execute a task asynchronously within the executor's event loop.

        This internal method handles the actual execution of tasks submitted via
        `execute()`. It is designed primarily for **async coroutines**, while also
        safely executing synchronous functions in a thread pool to avoid blocking the
        event loop.

        Execution Details:
            - Async functions (coroutines) are awaited directly within the event loop.
            - Synchronous functions are executed in the thread pool using
              `run_in_executor` to prevent blocking.
            - Concurrency for async tasks is controlled using an asyncio.Semaphore
              based on the executor's configuration.
            - Each task execution is tracked in `_async_tasks` to support proper
              shutdown behavior.

        Logging:
            - Logs task start, completion, or failure with elapsed time.
            - Logs whether the task completed successfully or raised an exception.

        Args:
            task (Task): The task to execute. Must include the callable (`func`)
                         and its arguments (`args` and `kwargs`).

        Returns:
            Any: The result of the task execution, either from the coroutine
                 directly or from the synchronous function executed in the thread pool.

        Raises:
            Exception: Any exception raised by the task function is propagated
                       and logged. The Future returned by `execute()` will
                       contain this exception.

        Notes:
            - Async tasks are preferred; synchronous tasks should be used sparingly.
            - The task is registered in `_async_tasks` to enable graceful shutdown.
            - After execution (successful or failed), the task is removed from
              `_async_tasks`.
            - This method is **not intended to be called directly** from outside
              the executor; use `execute(task)` instead.
        """

        # Manage concurrency and limit resource usage in asynchronous code
        async with self._semaphore:
            # Get the current asyncio Task object
            task_obj = asyncio.current_task()
            # Register the task in our tracking set for proper shutdown using the lock
            with self._async_tasks_lock:
                # Add the current task to the set of running tasks
                self._async_tasks.add(task_obj)
            # Flag to indicate if an exception occurred during function execution
            exception_in_execution: bool = False
            # Get start time
            start = time.perf_counter()
            # Try to execute the function
            try:
                # Log start message
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"{' ⚡️ Started'.ljust(13)} - {task}")
                # Check if the function is a coroutine function
                if asyncio.iscoroutinefunction(task.action.func):
                    # If it is a coroutine function, run it directly
                    return await AsyncThreadExecutor.async_retry(task=task)
                else:
                    # If it is a normal function, run it in the thread pool executor
                    return await self._loop.run_in_executor(self._thread_pool_executor,
                                                            partial(AsyncThreadExecutor.sync_retry, task=task))
            except:
                # Set the exception flag to True
                exception_in_execution: bool = True
                # Re-raise the exception to propagate it
                raise
            finally:
                # Calculate elapsed time
                elapsed = time.perf_counter() - start
                # Determine the completion message based on whether an exception occurred
                message: str = ' ❌ Error' if exception_in_execution else ' ✅ Completed'
                # Log done message with elapsed time
                if logger.isEnabledFor(logging.INFO):
                    logger.info(f"{message.ljust(12)} - {task} ({elapsed:.6f} seconds)")
                # Unregister the task from our tracking set using the lock
                with self._async_tasks_lock:
                    # Remove the current task from the set of running tasks
                    self._async_tasks.discard(task_obj)

    def execute(self, task: Task) -> Future:
        """
        Submit a task for asynchronous execution and return a Future representing its result.

        This is the primary public method used to schedule tasks for execution in the
        `AsyncThreadExecutor`. It supports both async coroutines and synchronous functions:

            - **Async coroutines** are executed directly in the executor's event loop.
            - **Synchronous functions** are run in a dedicated thread pool to avoid
              blocking the event loop.

        Shutdown Behavior:
            - If the executor is in the process of shutting down, any new task submission
              immediately returns a Future containing an `ExecutorShutdownInProgressError`.
            - Tasks that have already started execution in the thread pool **cannot be stopped**
              once they begin. This is a known limitation of thread pool executors.

        Args:
            task (Task): The task to execute, containing the callable (`func`) and
                         its positional (`args`) and keyword (`kwargs`) arguments.

        Returns:
            Future: A concurrent.futures.Future representing the execution of the task.
                    - For async coroutines, the Future resolves when the coroutine completes.
                    - For synchronous tasks, the Future resolves when the function finishes in the thread pool.
                    - If an exception occurs, it is stored in the Future.

        Notes:
            - This method is **thread-safe** and can be called from multiple threads.
            - Prefer submitting async coroutines; synchronous functions are supported
              only to prevent blocking, not for high-performance parallelism.
            - The returned Future can be used to attach callbacks, wait for completion,
              or retrieve results via `result()` or `exception()`.
        """

        # If shutdown is in progress, return a Future with an exception immediately stating that shutdown is in progress
        if self._shutdown_event.is_set():
            # Create a Future object
            future_exc: Future = Future()
            # Set the exception
            future_exc.set_exception(ExecutorShutdownInProgressError(task=task))
            # Return the Future with the exception
            return future_exc
        try:
            # Schedule the coroutine _execute_async to run in the event loop from another thread
            future: Future = asyncio.run_coroutine_threadsafe(self._execute_async(task=task), self._loop)
            # Return the Future object representing the execution of the task
            return future
        except Exception as exc:
            # If scheduling the coroutine fails, return a Future that is already completed with the exception
            future_exc: Future = Future()
            # Set the exception
            future_exc.set_exception(exc)
            # Return the completed Future with the exception
            return future_exc

    def _shutdown_thread_pool_executor(self):
        """
        Gracefully shuts down the internal thread pool executor used for synchronous tasks.

        This method stops accepting new tasks in the thread pool and attempts to cancel
        any pending tasks that have not yet started. **Tasks that have already started
        execution in the thread pool cannot be interrupted** and will continue running
        until completion.

        Notes:
            - Uses `shutdown(wait=False, cancel_futures=True)` to avoid blocking the main
              thread during shutdown initiation.
            - Logs success or any exceptions encountered during shutdown.
            - This method is invoked automatically during the executor's overall shutdown
              process.
            - For synchronous functions submitted via the executor, the inability to
              stop already running tasks is a known limitation of Python's ThreadPoolExecutor.

        Usage:
            This is an internal method and typically should not be called directly;
            it is managed by the `shutdown()` method of AsyncThreadExecutor.
        """

        # If the executor is None, early return
        if self._thread_pool_executor is None:
            return
        try:
            # Shutdown the thread pool executor with no wait
            self._thread_pool_executor.shutdown(wait=False, cancel_futures=True)
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    f"{' 🚨 Shutdown'.ljust(12)} - Successfully completed initiating shutdown for thread pool executor")

        except Exception as exc:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(
                    f"{' 🚨 Shutdown'.ljust(12)} - Error in thread pool executor shutdown. Exception -> {type(exc).__name__} - {exc}")

    async def _shutdown_all_asyncio_tasks(self):
        """
        Gracefully shuts down all asyncio tasks managed by this executor.

        This method handles proper cleanup of asynchronous tasks when shutting down
        the AsyncThreadExecutor. It differentiates between tasks that are still pending
        (not yet started or scheduled) and tasks that are actively running:

        - Pending tasks are cancelled immediately.
        - Running tasks are awaited for completion if `graceful_shutdown` is True.
          They are **not forcibly cancelled**, ensuring safe termination without
          interrupting critical operations.
        - If `graceful_shutdown` is False, all tasks (pending or running) are cancelled.

        The method respects the `shutdown_timeout_seconds` configuration:
        - If tasks do not complete within the timeout, a warning is logged.
        - Ensures proper concurrency control using the internal `_async_tasks` tracking set.
        """

        # Identify all tasks in this loop.
        # Ignore the asyncio.Task currently running in the current OS thread.
        all_tasks = [task for task in asyncio.all_tasks(self._loop) if task is not asyncio.current_task()]

        # Snapshot the currently running tasks using the lock
        with self._async_tasks_lock:
            # Get the current set of running tasks
            running: list = [task for task in self._async_tasks if not task.done()]

        # Get the pending tasks by excluding running from all tasks
        pending: list = [task for task in all_tasks if task not in running]

        # Log the counts of pending and running tasks
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                f"{' 🚨 Shutdown'.ljust(12)} - Found {len(pending)} pending and {len(running)} running asyncio tasks")

        # If graceful shutdown is enabled, cancel only pending and wait for running to finish
        if self._config.graceful_shutdown:

            # Cancel only the pending tasks
            for task in pending:
                # Cancel the task
                task.cancel()

            if pending:
                if logger.isEnabledFor(logging.INFO):
                    logger.info(
                        f"{' 🚨 Shutdown'.ljust(12)} - Successfully cancelled {len(pending)} pending asyncio tasks")

            # Wait for running to finish (do not cancel them)
            if running:
                try:
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(
                            f"{' 🚨 Shutdown'.ljust(12)} - Started waiting for running asyncio tasks to finish. Waiting up to {self._config.shutdown_timeout_seconds} seconds...")
                    await asyncio.wait_for(asyncio.gather(*running, return_exceptions=True),
                                           timeout=self._config.shutdown_timeout_seconds)
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(
                            f"{' 🚨 Shutdown'.ljust(12)} - Completed waiting ({self._config.shutdown_timeout_seconds}) seconds for running asyncio tasks to finish")
                except asyncio.TimeoutError:
                    if logger.isEnabledFor(logging.ERROR):
                        logger.exception(
                            f"{' 🚨 Shutdown'.ljust(12)} - Timeout Error - Timed out waiting ({self._config.shutdown_timeout_seconds}) seconds for running asyncio tasks to finish")
        # If not graceful, cancel everything
        else:
            # Get all tasks that are not completed
            tasks_to_cancel = [task for task in all_tasks if not task.done()]
            # Loop through tasks_to_cancel list and cancel them
            for task in tasks_to_cancel:
                # Cancel the task
                task.cancel()
            if pending or running:
                if logger.isEnabledFor(logging.INFO):
                    logger.info(
                        f"{' 🚨 Shutdown'.ljust(12)} - Successfully Cancelled {len(pending)} pending and {len(running)} running asyncio tasks")

            # If there are any tasks to cancel
            if tasks_to_cancel:
                try:
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(
                            f"{' 🚨 Shutdown'.ljust(12)} - Started waiting for running asyncio tasks to finish. Waiting up to {self._config.shutdown_timeout_seconds} seconds...")
                    # Wait for all tasks to be cancelled with a timeout
                    await asyncio.wait_for(asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                                           timeout=self._config.shutdown_timeout_seconds)
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(
                            f"{' 🚨 Shutdown'.ljust(12)} - Completed waiting {self._config.shutdown_timeout_seconds} seconds for running asyncio tasks to finish")
                except asyncio.TimeoutError:
                    if logger.isEnabledFor(logging.ERROR):
                        logger.exception(
                            f"{' 🚨 Shutdown'.ljust(12)} - Timeout Error - Timed out waiting ({self._config.shutdown_timeout_seconds}) seconds for running asyncio tasks to finish")

    def _shutdown_asyncio_executions(self):
        """
        Orchestrates the shutdown of all asyncio tasks in the executor's event loop.

        This method schedules the `_shutdown_all_asyncio_tasks` coroutine to run in
        the executor's dedicated event loop from a separate thread. It ensures that
        all managed asynchronous tasks are properly cleaned up according to the
        configured shutdown policy.

        Behavior:
            - If `graceful_shutdown` is True:
                * Pending tasks are cancelled immediately.
                * Running tasks are awaited up to `shutdown_timeout_seconds`.
            - If `graceful_shutdown` is False:
                * All tasks (pending or running) are cancelled immediately.
                * Awaiting for running tasks is still done with the configured timeout.

        Notes:
            - Tasks that are already running in the thread pool (non-async functions)
              cannot be interrupted once started.
            - This method blocks the calling thread until shutdown completes or the
              timeout is reached.
            - Exceptions during shutdown are logged but do not propagate.
        """

        # If the event loop is running
        if self._loop.is_running():
            # Schedule the coroutine to shut down all asyncio tasks in the event loop from another thread
            fut = asyncio.run_coroutine_threadsafe(self._shutdown_all_asyncio_tasks(), self._loop)
            try:
                # Wait for the coroutine to complete with a timeout. Add extra buffer time to the timeout to ensure it completes.
                fut.result(timeout=self._config.shutdown_timeout_seconds + 10)
            except Exception as e:
                if logger.isEnabledFor(logging.ERROR):
                    logger.exception(f"{' 🚨 Shutdown'.ljust(12)} - Error in shutdown of asyncio tasks {e}")

    def _stop_asyncio_event_loop(self):
        """
        Stops the executor's dedicated asyncio event loop.

        This method signals the event loop running in the background thread to
        stop executing. It ensures that no new callbacks or coroutines are processed
        after the shutdown sequence begins.

        Behavior:
            - The event loop is stopped using `call_soon_threadsafe(loop.stop)` to
              safely signal the background thread.
            - Already running callbacks or coroutines will complete before the loop
              fully stops.
            - Exceptions during the stop operation are logged but do not propagate.
        """

        # Stop the event loop
        try:
            # loop.stop() just tells the event loop to break out of run_forever() after completing any currently running callbacks in the loop.
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception as exc:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(
                    f"{' 🚨 Shutdown'.ljust(12)} - Error in stopping the event loop. Failed to schedule loop.stop(). Exception: -> {type(exc).__name__} - {exc}")
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"{' 🚨 Shutdown'.ljust(12)} - Successfully stopped asyncio event loop")

    def _shutdown_background_thread(self):
        """
        Gracefully shuts down the background thread running the executor's event loop.

        This method ensures that the dedicated background thread, which hosts the
        asyncio event loop, is properly terminated as part of the executor shutdown
        sequence.

        Behavior:
            - Checks if the background thread is alive before attempting to join it.
            - Uses a timeout based on the executor's `shutdown_timeout_seconds` to
              prevent indefinite blocking.
            - Exceptions during the join operation are caught and logged, ensuring
              that shutdown proceeds without crashing the application.

        Notes:
            - This does not stop or cancel tasks within the event loop; those should
              be handled separately via `_shutdown_asyncio_executions`.
            - The thread is marked as a daemon, so it would automatically exit when
              the main program terminates, but explicit shutdown is preferred to
              release resources and avoid dangling threads.
        """

        try:
            # Check if the background thread is alive
            if self._background_thread.is_alive():
                # Wait for the background thread to finish with a timeout
                self._background_thread.join(timeout=self._config.shutdown_timeout_seconds)
        except Exception as exc:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(
                    f"{' 🚨 Shutdown'.ljust(12)} - Error in background thread shutdown. Exception: -> {type(exc).__name__} - {exc}")

        if logger.isEnabledFor(logging.INFO):
            logger.info(f"{' 🚨 Shutdown'.ljust(12)} - Successfully completed shutting down background thread")

    def _shutdown_asyncio_event_loop(self):
        """
        Gracefully shuts down and closes the executor's asyncio event loop.

        This method ensures that the event loop is properly terminated after all
        tasks and asynchronous generators have been handled. It is the final step
        in cleaning up the executor's asyncio resources.

        Behavior:
            1. Attempts to shut down any asynchronous generators in the event loop
               using `loop.shutdown_asyncgens()`.
            2. Closes the event loop to release all associated resources.
            3. Catches and logs any exceptions during the shutdown process to ensure
               that the executor cleanup sequence continues without crashing.

        Notes:
            - This should be called only after all asyncio tasks have been
              completed, cancelled, or properly handled via `_shutdown_all_asyncio_tasks`.
            - Closing the event loop is irreversible; the loop cannot be restarted
              after this method is executed.
            - Exceptions are logged but not propagated, prioritizing safe shutdown
              over strict error enforcement.
        """

        try:
            # Shutdown any running asynchronous generators (if used)
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        except Exception:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(f"{' 🚨 Shutdown'.ljust(12)} - Failed to shutdown async generators")

        # Close the event loop to release resources
        try:
            self._loop.close()
        except Exception as exc:
            if logger.isEnabledFor(logging.ERROR):
                logger.exception(
                    f"{' 🚨 Shutdown'.ljust(12)} - Error in asyncio event loop shutdown. Exception -> {type(exc).__name__} - {exc}")

        if logger.isEnabledFor(logging.INFO):
            logger.info(f"{' 🚨 Shutdown'.ljust(12)} - Successfully completed shutting down asyncio event loop")

    def shutdown(self):
        """
        Gracefully shuts down the AsyncThreadExecutor and cleans up all resources.

        This method ensures that the executor stops accepting new tasks,
        terminates ongoing executions as much as possible, and releases all
        underlying resources including threads and the asyncio event loop.

        Shutdown Sequence and Rationale:
            1. **Set shutdown flag (`_shutdown_event`)**:
                - Prevents any new tasks from being submitted while shutdown is in progress.
            2. **Shutdown ThreadPoolExecutor (`_shutdown_thread_pool_executor`)**:
                - Stops accepting new tasks in the thread pool.
                - Already running tasks in threads **cannot be forcibly stopped** and will continue to completion.
                - Threads are terminated gracefully when finished.
            3. **Shutdown asyncio tasks (`_shutdown_asyncio_executions`)**:
                - Cancels pending tasks and optionally waits for running tasks based on `graceful_shutdown`.
                - Ensures that all tasks managed by the event loop are properly cleaned up before stopping the loop.
            4. **Stop the asyncio event loop (`_stop_asyncio_event_loop`)**:
                - Tells the loop to exit from `run_forever()`.
                - Required to safely terminate the background thread running the loop.
            5. **Shutdown the background thread (`_shutdown_background_thread`)**:
                - Waits for the background thread that runs the event loop to exit.
                - Ensures that all loop callbacks and scheduled tasks have been processed.
            6. **Shutdown the asyncio event loop (`_shutdown_asyncio_event_loop`)**:
                - Shuts down asynchronous generators and closes the loop.
                - Releases all remaining loop resources.

        Key Notes:
            - **Order is critical**:
                1. Prevent new submissions first.
                2. Cancel or complete all pending and running asyncio tasks.
                3. Stop the loop and background thread last.
                This ensures no task is lost or abruptly interrupted without proper handling.
            - **ThreadPool tasks**:
                Tasks already running in threads cannot be stopped; they will continue until completion.
            - **Timeouts**:
                Graceful shutdown respects `shutdown_timeout_seconds` for waiting tasks.
        """

        # Check if shutdown is already in progress
        if self._shutdown_event.is_set():
            return

        # Log info message
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"{' 🚨 Shutdown'.ljust(12)} - Successfully started shutdown of AsyncThreadExecutor")

        # Set the shutdown event to indicate that shutdown is in progress
        self._shutdown_event.set()

        # Shutdown all thread pool tasks
        self._shutdown_thread_pool_executor()

        # Shutdown all asyncio tasks
        self._shutdown_asyncio_executions()

        # Stop the event loop
        self._stop_asyncio_event_loop()

        # Shutdown the background thread
        self._shutdown_background_thread()

        # Shutdown the event loop
        self._shutdown_asyncio_event_loop()

        # Log info message
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"{' 🚨 Shutdown'.ljust(12)} - Successfully completed shutdown of AsyncThreadExecutor")
