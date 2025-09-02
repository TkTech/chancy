import dataclasses
import enum
from datetime import datetime, timezone
from typing import (
    Any,
    Optional,
    TypedDict,
    TypeVar,
    ParamSpec,
    Callable,
    Protocol,
    Union,
)
from uuid import UUID

from chancy.utils import importable_name


class ErrorT(TypedDict):
    #: The attempt number of the job when the error occurred.
    attempt: int
    #: The error message, typically the traceback of an exception.
    traceback: str


class Reference:
    """
    References a Job in the queue.

    This object can be used to retrieve the job instance later, or wait for it
    to complete. It is returned by the :meth:`~chancy.app.Chancy.push`,
    :meth:`~chancy.app.Chancy.push_many`, and
    :meth:`~chancy.app.Chancy.push_many_ex` functions.

    Waiting for a job to finish:

    .. code-block:: python

         async with Chancy("postgresql://localhost/postgres") as chancy:
            ref = await chancy.push(Job.from_func(my_function))
            job = await chancy.wait_for_job(ref)
            print(job.state)  # "succeeded"

    Retrieving a job instance:

    .. code-block:: python

        async with Chancy("postgresql://localhost/postgres") as chancy:
            ref = await chancy.push(Job.from_func(my_function))
            job = await chancy.get_job(ref)
            print(job.state)  # "pending"
    """

    __slots__ = ("identifier",)

    def __init__(self, identifier: str | UUID):
        if isinstance(identifier, UUID):
            self.identifier = identifier
        else:
            self.identifier = UUID(identifier)

    def __repr__(self):
        return f"<Reference({self.identifier!r})>"

    def __eq__(self, other: Union[str, UUID, "Reference"]) -> bool:
        if isinstance(other, Reference):
            return self.identifier == other.identifier
        elif isinstance(other, str):
            return str(self.identifier) == other
        elif isinstance(other, UUID):
            return self.identifier == other
        else:
            return super().__eq__(other)

    def __hash__(self) -> int:
        return hash(self.identifier)


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
    #: Arbitrary metadata associated with this job instance. Plugins can use
    #: this to store additional information during the execution of a job.
    meta: dict[str, Any] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_func(cls, func, **kwargs):
        """
        Create a job from a function, attempting to determine the function's
        importable name automatically.

        .. code-block:: python

            def hello_world():
                pass

            job = Job.from_func(hello_world)
        """
        return cls(func=importable_name(func), **kwargs)

    def with_priority(self, priority: int) -> "Job":
        return dataclasses.replace(self, priority=priority)

    def with_max_attempts(self, max_attempts: int) -> "Job":
        return dataclasses.replace(self, max_attempts=max_attempts)

    def with_scheduled_at(self, scheduled_at: datetime) -> "Job":
        return dataclasses.replace(self, scheduled_at=scheduled_at)

    def with_limits(self, limits: list[Limit]) -> "Job":
        return dataclasses.replace(self, limits=limits)

    def with_kwargs(self, **kwargs) -> "Job":
        return dataclasses.replace(self, kwargs=kwargs)

    def with_unique_key(self, unique_key: str) -> "Job":
        return dataclasses.replace(self, unique_key=unique_key)

    def with_queue(self, queue: str) -> "Job":
        return dataclasses.replace(self, queue=queue)

    def with_meta(self, meta: dict[str, Any]) -> "Job":
        return dataclasses.replace(self, meta=meta)

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
            "m": self.meta,
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
            meta=data["m"],
        )


@dataclasses.dataclass(frozen=True, kw_only=True)
class QueuedJob(Job):
    """
    A job instance is a job that has been pushed onto a queue and now has
    stateful information associated with it, such as the number of attempts
    so far.
    """

    class State(enum.Enum):
        PENDING = "pending"
        RUNNING = "running"
        FAILED = "failed"
        RETRYING = "retrying"
        SUCCEEDED = "succeeded"

    #: The unique identifier for this job instance.
    id: str
    #: The time at which this job was created.
    created_at: datetime
    #: The time at which this job was started, if it has been started.
    started_at: Optional[datetime] = None
    #: The time at which this job was completed, if it has been completed.
    completed_at: Optional[datetime] = None
    #: The number of times this job has been attempted.
    attempts: int = 0
    #: The current state of this job instance.
    state: State = State.PENDING
    #: A list of errors that occurred during the execution of this job.
    errors: list[ErrorT] = dataclasses.field(default_factory=list)

    @classmethod
    def unpack(cls, data: dict) -> "QueuedJob":
        return cls(
            id=str(data["id"]),
            func=data["func"],
            kwargs=data["kwargs"],
            priority=data["priority"],
            created_at=data["created_at"],
            scheduled_at=data["scheduled_at"],
            started_at=data["started_at"],
            completed_at=data["completed_at"],
            attempts=data["attempts"],
            max_attempts=data["max_attempts"],
            state=QueuedJob.State(data["state"]),
            unique_key=data["unique_key"],
            queue=data["queue"],
            errors=data["errors"],
            limits=[Limit.deserialize(limit) for limit in data["limits"]],
            meta=data["meta"],
        )


P = ParamSpec("P")
R = TypeVar("R")


class IsAJob(Protocol[P, R]):
    __call__: Callable[P, R]
    job: Job


def job(
    **options,
) -> Callable[[Callable[P, R]], IsAJob[P, R]]:
    """
    A decorator that wraps a function and turns it into a job.

    The wrapped function can still be called as normal, but will have an extra
    ``job`` attribute that contains the job instance.

    .. code-block:: python

        >>> @job()
        ... def hello_world():
        ...    return "Hello, world!"
        >>> hello_world.job
        Job(func=<function my_job at 0x1033041e0>, kwargs={}, ...)
        >>> hello_world()
        'Hello, world!'

    Your decorated function can be pushed like any other job, or you can
    use the `job` property to modify its properties before pushing it.

    .. code-block:: python

        await chancy.push(hello_world)
        await chancy.push(hello_world.job.with_queue("low_priority"))
    """

    def decorator(func: Callable[P, R]) -> IsAJob[P, R]:
        func.job = Job.from_func(func, **options)
        return func

    return decorator
