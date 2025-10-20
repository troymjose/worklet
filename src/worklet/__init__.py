from src.worklet.portal.portal import Portal
from src.worklet.portal.models import PortalConfig

__version__ = "0.1.4"
__all__ = ["Portal", "PortalConfig", ]

import logging

# This ensures your library doesn’t throw logging-related warnings if the host app hasn't configured logging.
# Optional: Default to warning level so logs don’t spam unless configured
logging.getLogger(__name__).addHandler(logging.NullHandler())

# TODO
# VIP: Use logger.isEnabledFor(level) everywhere to improve performance
# if logger.isEnabledFor(logging.DEBUG):
# logger.debug("Task %s scheduled with args=%s", task.id, task.args)
# .
# 🧊 When You Don’t Need It
# Avoid wrapping low-frequency logs, like:
# Startup logs
# Shutdown logs
# Configuration summaries
# Rare exception traces
# Error or critical logs
# if logger.isEnabledFor(logging.INFO):
# logger.info("Executor shutting down...")
# THIS SHOULD BE USED IN EVERY LOGGING Level, not just DEBUG

# TODO:
# VIP: __all__ = ["ExecutorTask", "ExecutorConfig", ], Use this in all files. Very Important

# TODO
# 1. Even though we handle backpressure getting kafka tasks, its not handled at executer level.
# For example if we start a worker with --executor async --concurrency 4 --max-queued-tasks 1
# Expected behaviour is that 1 task will only run, but 4 tasks will be running in parallel.

# TODO
# 2. Great — let’s add Prometheus-style metrics to your Worker class so you can monitor:
# 🔢 Number of tasks received
# ✅ Number of tasks completed
# ❌ Number of failed tasks
# ⏱️ Queue size (real-time)
# 📊 Task execution latency (optional)

# TODO
# 3. Graceful Worker Exit on Signals
# Instead of just KeyboardInterrupt, use the signal module to catch SIGTERM, etc.
# python
# Copy
# Edit
# import signal
# signal.signal(signal.SIGTERM, lambda s, f: self.stop())

# TODO
# 4.5. CLI Improvement (argparse or typer)
# Instead of custom sys.argv parsing:

# TODO
# HOOKS
# try:
#     if self.before_call:
#         await self.before_call(*args, **kwargs)
#
#     if self._is_async:
#         result = await self._func(*args, **kwargs)  # type: ignore
#     else:
#         result = self._func(*args, **kwargs)  # type: ignore
#
#     if self.after_call:
#         await self.after_call(result, *args, **kwargs)
#
#     return result
#
# except Exception as e:
#     logger.exception(f"Error executing worklet {self.__name__}")
#     if self.on_error:
#         await self.on_error(e)
#     raise


# TODO
# Make sure we have .join or Flush capability in portal so that we can wait for all tasks to finish before exiting.
# This should be a blocking call that waits for all tasks to finish processing.
# FastAPI or Flask can use this in the shutdown event to ensure all tasks are processed before exiting.

# TODO
# Add Celery-style auto-shutdown hooks


# TODO
# IF Kafka conn is lost, worker should pause and wait for reconnection. Everything should be smooth

# TODO: ADD Context to possible classes, Base executor, Worker, KafkaConsumer etc
# ---- Context manager ----
# def __enter__(self):
# return self
# def __exit__(self, exc_type, exc, tb):
# self.shutdown()
# return False


# TODO: Timeout and retry per tasks. It should be configurable per task basis.

# TODO: IN future instead of using inmemory queue for retry, use persistent queue like Redis or SQLite. So that when restart happens, we dont loose tasks which failed due to broker issues.


# TODO: KEEP an helper function to return a log prefix
# function should expect "sub" or "name" and return " 🚨 Shutdown".ljust(12) where "Shutdown" is the name passed
# LOG_PREFIX = " 🚨 Shutdown".ljust(12)
# logger.info(f"{LOG_PREFIX} - Completed waiting...")


# TODO: VERY IMPORTANT: Ensure only one KafkaProducer instance per Portal. Because self._kafka_producer is passed into Worklet instances.
# TODO: VERY IMPORTANT: Ensure proper shutdown of KafkaProducer on application exit
# TODO: Should handle Gracefull Shutdown of KafkaProducer
# TODO: Should ensure only one KafkaProducer instance per Portal
# TODO: Should handle re-initialization of KafkaProducer on failure in Client machine
