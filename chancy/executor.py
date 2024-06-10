import abc
import dataclasses
import enum
from datetime import datetime
from typing import Any, Optional

from chancy.utils import importable_name


@dataclasses.dataclass
class Limit:
    """
    A limit that can be applied to a job.
    """

    class Type(enum.Enum):
        MEMORY = "memory"
        TIME = "time"

    type_: "Type"
    value: int

    @classmethod
    def deserialize(cls, data: dict) -> "Limit":
        return cls(type_=cls.Type(data["t"]), value=data["v"])

    def serialize(self) -> dict:
        return {"t": self.type_.value, "v": self.value}


@dataclasses.dataclass(frozen=True, kw_only=True)
class Job:
    """
    A job is an immutable, stateless unit of work that can be pushed onto a
    Chancy queue and executed elsewhere.
    """

    func: str
    kwargs: dict[str, Any] | None = dataclasses.field(default_factory=dict)
    priority: int = 0
    max_attempts: int = 1
    scheduled_at: Optional[datetime] = None
    limits: list[Limit] = dataclasses.field(default_factory=list)

    @classmethod
    def from_func(cls, func, **kwargs):
        return cls(func=importable_name(func), **kwargs)

    def with_priority(self, priority: int) -> "Job":
        return dataclasses.replace(self, priority=priority)

    def with_max_attempts(self, max_attempts: int) -> "Job":
        return dataclasses.replace(self, max_attempts=max_attempts)

    def with_scheduled_at(self, scheduled_at: datetime) -> "Job":
        return dataclasses.replace(self, scheduled_at=scheduled_at)

    def with_limits(self, limits: list[Limit]) -> "Job":
        return dataclasses.replace(self, limits=limits)


@dataclasses.dataclass(frozen=True, kw_only=True)
class JobInstance(Job):
    """
    A job instance is a job that has been pushed onto a queue and now has
    stateful information associated with it, such as the number of attempts
    so far.
    """

    class State:
        PENDING = "pending"
        RUNNING = "running"
        FAILED = "failed"
        RETRYING = "retrying"
        SUCCEEDED = "succeeded"

    id: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    attempts: int = 0
    state: State = State.PENDING


class Executor(abc.ABC):
    """
    The executor is responsible for managing the execution of jobs in a job
    pool.
    """

    def __init__(self, queue):
        self.queue = queue

    @abc.abstractmethod
    async def push(self, job: JobInstance):
        """
        Push a job onto the job pool.
        """

    @abc.abstractmethod
    def __len__(self):
        """
        Get the number of pending and running jobs in the pool.
        """
