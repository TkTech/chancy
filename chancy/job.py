from dataclasses import dataclass
from typing import Optional


def importable_name(obj):
    """
    Get the importable name for an object.

    .. note::

        This will not work for objects that are not defined in a module,
        such as lambdas, REPL functions, functions-in-functions, etc...

    :param obj: The object to get the importable name for.
    :return: str
    """
    return f"{obj.__module__}.{obj.__qualname__}"


@dataclass(frozen=True, kw_only=True)
class Job:
    """
    Defines a job to be executed by Chancy.
    """

    # The following fields exist only inside the `payload` field in the
    # database.

    #: The import path of the job to execute.
    name: str
    #: The keyword arguments to pass to the job.
    kwargs: Optional[dict] = None
    #: The maximum runtime of this job in seconds.
    timeout: Optional[int] = None
    #: The maximum memory usage of this job in bytes.
    memory_limit: Optional[int] = None
    #: The priority of the job.
    priority: int = 0

    # The following fields exist as columns in the database.

    #: The ID of the job, if it has been created in the database.
    id: Optional[int] = None
    #: The number of times this job has been attempted.
    attempts: Optional[int] = None
    #: The maximum number of times this job should be attempted.
    max_attempts: int = 1
    #: The state of the job.
    state: Optional[str] = None
    #: The time the job was created.
    created_at: Optional[str] = None
    #: The time the job was started.
    started_at: Optional[str] = None
    #: The time the job was completed.
    completed_at: Optional[str] = None

    @classmethod
    def deserialize(cls, data: dict):
        return cls(
            name=data["payload"]["name"],
            kwargs=data["payload"]["kwargs"],
            timeout=data["payload"]["timeout"],
            memory_limit=data["payload"]["memory_limit"],
            id=data["id"],
            attempts=data["attempts"],
            max_attempts=data["max_attempts"],
            state=data["state"],
            created_at=data["created_at"],
            started_at=data["started_at"],
            completed_at=data["completed_at"],
        )

    def serialize(self) -> dict:
        return {
            "id": self.id,
            "payload": {
                "name": self.name,
                "kwargs": self.kwargs,
                "timeout": self.timeout,
                "memory_limit": self.memory_limit,
            },
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "state": self.state,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_func(cls, func, **kwargs):
        return cls(
            name=importable_name(func),
            **kwargs,
        )
