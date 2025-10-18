from dataclasses import dataclass
from worklet.worker.models import ErrorModel


@dataclass(frozen=True)
class Errors:
    portals_not_found: ErrorModel = ErrorModel(
        message="Portals not found. Please ensure that at least one portal is created.",
        code=1000,
        details="Portal is required to register worklets. Worklets are teleportable only through a portal."
    )
    worklets_not_found: ErrorModel = ErrorModel(
        message="Worklet not found. Please ensure that at least one method is decorated using @<my_portal>.teleportable decorator.",
        code=1001,
        details="No worklets have been registered with the portal. Only decorated methods can be teleported."
    )
    invalid_executor: ErrorModel = ErrorModel(
        message="Invalid executor specified. Please use a supported executor.",
        code=1002,
        details="The specified executor is not supported. Supported executors are: 'async'"
    )
    executor_init_error: ErrorModel = ErrorModel(
        message="Executor Error. Executor initialization failed.",
        code=1003,
        details="The executor could not be initialized with the provided parameters."
    )
    shutdown_in_progress: ErrorModel = ErrorModel(
        message="Executor is shutting down",
        code=1003,
        details="Task cannot be submitted. Executor is shutting down."
    )
    task_failed: ErrorModel = ErrorModel(
        message="Task execution failed",
        code=1004,
        details="An error occurred during task execution."
    )
    deserialization_failed: ErrorModel = ErrorModel(
        message="Failed to deserialize data",
        code=1005,
        details="The data could not be deserialized into the expected format."
    )
    queue_timeout: ErrorModel = ErrorModel(
        message="Queue operation timed out",
        code=1006,
        details="The operation on the queue timed out."
    )
    processing_error: ErrorModel = ErrorModel(
        message="Error processing queue item",
        code=1007,
        details="An error occurred while processing an item from the queue."
    )


errors = Errors()
