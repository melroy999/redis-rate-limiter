from celery import shared_task

from src.config import app
from src.rate_limiter.registry import get_limiter


@shared_task(bind=True, name="rate_limiter.attempt_consume")
def attempt_consume(self, limiter_id: str):
    """
    Attempt to consume a task from the task queue.
    :param self: The instance of the runner.
    :param limiter_id: The id of the rate limiter instance to use.
    """
    limiter = get_limiter(limiter_id)

    # Lock the execution to avoid the thundering herd problem.
    with limiter.execution_lock() as acquired:
        if not acquired:
            # Someone is already executing an attempt; hence skip.
            return

        # Perform a consume.
        result = limiter.consume()

        # Execute the task if the green light is given.
        if result["success"] and result["task"]:
            task = result["task"]

            # Send to the generic worker.
            app.send_task(
                "rate_limiter.generic_worker",
                args=[limiter_id, task["func_path"], task["payload"], task.get("id")]
            )

            # ONLY pulse if there are still items waiting in the buffer
            # This prevents the dispatcher from running forever.
            if result["remaining_tasks"] > 0:
                self.apply_async(args=[limiter_id], countdown=0)

        elif result["remaining_tasks"] == 0:
            # Stop: No remaining tasks. Next drain will be triggered by a new task being added.
            pass

        elif result["active_concurrency"] >= limiter.max_concurrency:
            # Stop: The next drain will be triggered by worker completion.
            pass

        elif result["remaining_tokens"] <= 0:
            # Wait for the rate window to reset.
            self.apply_async(args=[limiter_id], countdown=result.get("reset_in", 1))
