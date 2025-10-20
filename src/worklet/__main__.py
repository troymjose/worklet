import argparse
from worklet.executor.models import ExecutorConfig
from worklet.worker.models import WorkerQueueConfig
from worklet.worker.worker import Worker


def main():
    parser = argparse.ArgumentParser(description="Worklet - Let Work Teleport")
    parser.add_argument("--portal",
                        help="Name of the portal to connect to",
                        )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Number of concurrent tasks [default: 1]",
    )
    parser.add_argument(
        "--max-queued-tasks",
        type=int,
        default=10,
        help="Maximum number of queued tasks before applying backpressure [default: 10]",
    )
    parser.add_argument(
        "--task-pause-threshold",
        type=float,
        default=0.9,
        help="Pause task intake when queue reaches this fraction of max queued tasks [default: 0.9]",
    )
    parser.add_argument(
        "--task-resume-threshold",
        type=float,
        default=0.5,
        help="Resume task intake when queue drops below this fraction of max queued tasks [default: 0.5]",
    )

    args = parser.parse_args()

    queue_config: WorkerQueueConfig = WorkerQueueConfig(max_size=args.max_queued_tasks,
                                                        pause_threshold=args.task_pause_threshold,
                                                        resume_threshold=args.task_resume_threshold)
    executor_config: ExecutorConfig = ExecutorConfig(concurrency=args.concurrency)
    worker = Worker(portal=args.portal, queue_config=queue_config, executor_config=executor_config, )
    worker.start()


if __name__ == "__main__":
    main()
