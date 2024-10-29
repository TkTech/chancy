import dataclasses
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

from chancy.job import QueuedJob


class MaxRetriesExceededError(Exception):
    """Raised when maximum retry attempts have been exhausted"""


class Retry(Exception):
    """
    Raised to explicitly indicate that a job should be retried.
    """

    def __init__(
        self,
        max_attempts: Optional[int] = None,
        backoff: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        backoff_limit: Optional[int] = None,
        backoff_jitter: Optional[tuple[int, int]] = None,
    ):
        super().__init__()
        self.max_attempts = max_attempts
        self.backoff = backoff
        self.backoff_factor = backoff_factor
        self.backoff_limit = backoff_limit
        self.backoff_jitter = backoff_jitter

    def calculate_next_run(self, current_attempts: int) -> datetime:
        """
        Calculate the next retry time using exponential backoff with jitter.

        :param current_attempts: The number of attempts that have been made
        :return: The datetime when the job should be retried next (UTC)
        """
        # Base delay starts with the backoff value or 1 second
        delay = self.backoff or 1

        # Apply exponential backoff
        if self.backoff_factor:
            delay *= self.backoff_factor ** (current_attempts - 1)

        # Cap at backoff limit if specified
        if self.backoff_limit:
            delay = min(delay, self.backoff_limit)

        # Add jitter if specified
        if self.backoff_jitter:
            min_jitter, max_jitter = self.backoff_jitter
            delay += random.uniform(min_jitter, max_jitter)

        return datetime.now(timezone.utc) + timedelta(seconds=delay)


def handle_retry(
    job_instance: "QueuedJob", retry_exception: Retry
) -> "QueuedJob":
    """
    Handles a Retry exception, following the rules defined in the exception.
    Returns a new job instance with updated retry information.

    :param job_instance: The job instance to update
    :param retry_exception: The Retry exception raised by the job function
                            that triggered the retry.
    """
    # Increase attempts count
    new_attempts = job_instance.attempts + 1

    max_attempts = (
        retry_exception.max_attempts
        if retry_exception.max_attempts is not None
        else job_instance.max_attempts
    )

    if new_attempts >= max_attempts:
        raise MaxRetriesExceededError(
            f"Max retry attempts ({max_attempts}) exceeded"
        )

    next_run = retry_exception.calculate_next_run(new_attempts)

    # Create new job instance with updated retry information
    return dataclasses.replace(
        job_instance,
        attempts=new_attempts,
        max_attempts=max_attempts,
        state=job_instance.State.RETRYING,
        scheduled_at=next_run,
        meta={
            **job_instance.meta,
            "retry_count": new_attempts,
            "last_retry": datetime.now(timezone.utc).isoformat(),
            "next_retry": next_run.isoformat(),
        },
    )
