import asyncio
import dataclasses
import enum
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from chancy.utils import importable_name

if TYPE_CHECKING:
    from chancy.app import Chancy
    from chancy.queue import Queue


class Reference:
    """
    References a Job in the queue.

    For queues that support it, they'll return a Reference as a result of
    a :meth:`QueuePlugin.push()` call. This entry can be used to track the
    job in the queue and refer to it later.
    """

    def __init__(self, queue: "Queue", identifier: str):
        self.queue = queue
        self.identifier = identifier

    async def get(self, chancy: "Chancy") -> "JobInstance":
        """
        Get the job instance referenced by this reference.
        """
        return await self.queue.get_job(chancy, self)

    async def wait(
        self, chancy: "Chancy", *, interval: int = 1
    ) -> "JobInstance":
        """
        Wait for the job instance referenced by this reference to complete.

        .. note::

            "Completed" in this case means the job has either succeeded or
            failed.

        :param chancy: The Chancy application.
        :param interval: The interval at which to poll the job for completion.
        """
        while True:
            job = await self.get(chancy)
            if job.state in (
                JobInstance.State.FAILED,
                JobInstance.State.SUCCEEDED,
            ):
                return job
            await asyncio.sleep(interval)

    def __repr__(self):
        return f"<Reference({self.queue.name!r}, {self.identifier!r})>"


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

    #: An importable name for the function that should be executed when this
    #: job is run. Ex: my_module.my_function
    func: str
    #: The queue to which this job should be pushed.
    queue: str = "default"
    #: The keyword arguments to pass to the job function when it is executed.
    kwargs: dict[str, Any] | None = dataclasses.field(default_factory=dict)
    #: The priority of this job. Jobs with higher priority values will be
    #: executed before jobs with lower priority values.
    priority: int = 0
    #: The maximum number of times this job can be attempted before it is
    #: considered failed.
    max_attempts: int = 1
    #: The time at which this job should be scheduled to run.
    scheduled_at: datetime = dataclasses.field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    #: A list of resource limits that should be applied to this job.
    limits: list[Limit] = dataclasses.field(default_factory=list)
    #: An optional, globally unique identifier for this job. If provided,
    #: only 1 copy of a job with this key will be allowed to run or be
    #: scheduled at a time.
    unique_key: str | None = None

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

    def with_kwargs(self, kwargs: dict[str, Any]) -> "Job":
        return dataclasses.replace(self, kwargs=kwargs)

    def with_unique_key(self, unique_key: str) -> "Job":
        return dataclasses.replace(self, unique_key=unique_key)

    def with_queue(self, queue: str) -> "Job":
        return dataclasses.replace(self, queue=queue)

    def pack(self) -> dict:
        """
        Pack the job into a dictionary that can be serialized and used to
        recreate the job later.
        """
        return {
            "f": self.func,
            "k": self.kwargs,
            "p": self.priority,
            "a": self.max_attempts,
            "s": self.scheduled_at.timestamp(),
            "l": [limit.serialize() for limit in self.limits],
            "u": self.unique_key,
            "q": self.queue,
        }

    @classmethod
    def unpack(cls, data: dict) -> "Job":
        """
        Unpack a serialized job into a Job instance.
        """
        return cls(
            func=data["f"],
            kwargs=data["k"],
            priority=data["p"],
            max_attempts=data["a"],
            scheduled_at=datetime.fromtimestamp(data["s"], tz=timezone.utc),
            limits=[Limit.deserialize(limit) for limit in data["l"]],
            unique_key=data["u"],
            queue=data["q"],
        )


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

    id: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    attempts: int = 0
    state: State = State.PENDING

    @classmethod
    def unpack(cls, data: dict) -> "JobInstance":
        return cls(
            id=data["id"],
            func=data["payload"]["func"],
            kwargs=data["payload"]["kwargs"],
            priority=data["priority"],
            scheduled_at=data["scheduled_at"],
            started_at=data["started_at"],
            completed_at=data["completed_at"],
            attempts=data["attempts"],
            max_attempts=data["max_attempts"],
            state=data["state"],
            unique_key=data["unique_key"],
            queue=data["queue"],
            limits=[
                Limit.deserialize(limit) for limit in data["payload"]["limits"]
            ],
        )
