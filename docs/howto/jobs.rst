Make Jobs
=========

Jobs are the core of Chancy. They are the functions that are run by your
workers.

Creating a Job
--------------

Use the :func:`~chancy.job.job` decorator to create a job:

.. code-block:: python

   from chancy import job

   @job()
   def greet():
       print(f"Hello world!")

You can still call this function normally:

.. code-block:: python

   >>> greet()
   Hello world!

You can also specify the defaults for a job:

.. code-block:: python

    from chancy import job

    @job(queue="default", priority=1, max_attempts=3, kwargs={"name": "World"})
    def greet(*, name: str):
        print(f"Hello, {name}!")

Jobs are immutable once created - use the `with_` methods on a Job to create
a new job with modified properties:

.. code-block:: python

   @job(queue="default", priority=1, max_attempts=3, kwargs={"name": "World"})
   def greet(*, name: str):
       print(f"Hello, {name}!")

    async with Chancy("postgresql://localhost/postgres") as chancy:
       await chancy.push(greet.job.with_kwargs(name="Alice"))


Queue a Job
-----------

Once you've created a job, push it to the queue:

.. code-block:: python

   async with Chancy("postgresql://localhost/postgres") as chancy:
       await chancy.push(greet)

Queue multiple jobs at once:

.. code-block:: python

   await chancy.push_many([job1, job2, job3])

Push returns a :class:`~chancy.job.Reference` object that can be used to
retrieve the job instance later, or wait for it to complete:

.. code-block:: python

   reference = await chancy.push(greet)
   finished_job = await chancy.wait_for_job(reference)
   assert finished_job.state == finished_job.State.SUCCEEDED

Priority
--------

Priority determines the order of execution. The higher the priority, the
sooner the job will be executed:

.. code-block:: python

   higher_priority_job = greet.job.with_priority(10)
   lower_priority_job = greet.job.with_priority(-10)

Retry Attempts
--------------

Specify how many times a job should be retried if it fails:

.. code-block:: python

   greet.job.with_max_attempts(3)

Scheduled Execution
-------------------

Schedule a job to run some time in the future:

.. code-block:: python

   from datetime import datetime, timedelta, timezone

   future_job = greet.job.with_scheduled_at(
       datetime.now(timezone.utc) + timedelta(hours=1)
   )

.. note::

    Scheduled jobs are guaranteed to run *at* or *after* the scheduled time,
    but not *exactly* at that time.

.. tip::

    If you need recurring jobs, take a look at the
    :class:`~chancy.plugins.cron.Cron` plugin.

Resource Limits
---------------

Set memory and time limits for job execution:

.. code-block:: python

   from chancy import Limit, QueuedJob, job

   @job(limits=[
       Limit(Limit.Type.MEMORY, 1024 * 1024 * 1024),
       Limit(Limit.Type.TIME, 60),
   ])
   def greet(*, name: str):
       print(f"Hello, {name}!")

Not all executors will support all types of limits. For example only
the default :class:`~chancy.executors.process.ProcessExecutor` supports
memory limits. Time limits in the threaded and sub-interpreter executors are
cooperative: jobs must accept a :class:`chancy.job.QueuedJob` context and call
:meth:`~chancy.job.QueuedJob.checkpoint` periodically.

.. code-block:: python

   @job(limits=[Limit(Limit.Type.TIME, 60)])
   def process_items(*, context: QueuedJob):
       for item in get_items():
           context.checkpoint()
           process(item)

Unique Jobs
-----------

Prevent duplicate job execution by assigning a unique key:

.. code-block:: python

    from chancy import job

    @job()
    def greet(*, name: str):
        print(f"Hello, {name}!")

    async with Chancy("postgresql://localhost/postgres") as chancy:
        await chancy.push(greet.job.with_unique_key("greet_alice").with_kwargs(name="Alice"))


.. note::

  Unique jobs ensure only one job with the same ``unique_key`` is
  queued or running at a time, but any number can be completed or
  failed.

Validating Arguments
--------------------

Annotate a parameter with a type a validator supports to push an instance of
that type, validate it when the job is pushed and when it runs, and receive
an instance of that type rather than plain JSON. Other parameters are passed
through untouched.

Chancy doesn't depend on any validation library: give it
:class:`~chancy.validation.Validator` objects with its ``validators``
argument. A validator implements three methods:

- ``supports(annotation)``: whether it handles parameters with this
  annotation. The first validator that supports it is used.
- ``dump(annotation, value)``: turns the pushed object into the JSON stored
  in the queue, raising if the value isn't such an object.
- ``load(annotation, value)``: turns the stored JSON back into the object
  passed to the function, validating it.

For example, with pydantic:

.. code-block:: python

   import sys
   from typing import TYPE_CHECKING, Any

   from chancy import Chancy, Job
   from chancy.validation import JSON, Validator

   if TYPE_CHECKING:
       from pydantic import BaseModel

   class PydanticValidator(Validator):
       def supports(self, annotation: object) -> bool:
           # Lazy import, see the sub-interpreter note below.
           if "pydantic" not in sys.modules:
               return False
           from pydantic import BaseModel

           return isinstance(annotation, type) and issubclass(annotation, BaseModel)

       def load(self, annotation: "type[BaseModel]", value: JSON) -> "BaseModel":
           return annotation.model_validate(value)

       def dump(self, annotation: "type[BaseModel]", value: Any) -> JSON:
           if not isinstance(value, annotation):
               raise TypeError(
                   f"expected {annotation.__name__}, got {type(value).__name__}"
               )
           return value.model_dump(mode="json")

   class SendEmail(BaseModel):
       to: str
       subject: str

   async def send_email(email: SendEmail):
       print(f"Sending {email.subject!r} to {email.to}")

   async with Chancy(
       "postgresql://localhost/postgres",
       validators=[PydanticValidator()],
   ) as chancy:
       await chancy.push(
           Job.from_func(send_email).with_kwargs(
               email=SendEmail(to="alice@example.com", subject="Hi")
           )
       )

A kwarg a validator rejects, such as plain JSON pushed for ``SendEmail``,
raises a :class:`~chancy.validation.JobValidationError` before anything is
inserted. When the job runs, an invalid kwarg fails the job with the same
error, and it is retried like any failing job.

A job whose kwargs are already serialized, a :class:`~chancy.job.QueuedJob`
or a :class:`~chancy.job.SerializedJob` such as those the CLI pushes, is
pushed as it is and only validated when it runs.

For libraries describing data with a schema object rather than a type, such
as voluptuous, attach the schema with :data:`typing.Annotated` and look for
it in the annotation's metadata:

.. code-block:: python

   from typing import Annotated, Any

   from voluptuous import Required, Schema

   from chancy.validation import JSON, Validator

   class VoluptuousValidator(Validator):
       @staticmethod
       def schema_of(annotation: object) -> Schema | None:
           metadata = getattr(annotation, "__metadata__", ())
           return next((m for m in metadata if isinstance(m, Schema)), None)

       def supports(self, annotation: object) -> bool:
           return self.schema_of(annotation) is not None

       def validate(self, annotation: object, value: object) -> JSON:
           schema = self.schema_of(annotation)
           assert schema is not None  # supports() was checked first
           data: JSON = schema(value)
           return data

       def load(self, annotation: object, value: JSON) -> object:
           return self.validate(annotation, value)

       def dump(self, annotation: object, value: Any) -> JSON:
           return self.validate(annotation, value)

   SendEmail = Schema({Required("to"): str, Required("subject"): str})

   async def send_email(email: Annotated[dict[str, str], SendEmail]): ...

.. note::

  - Validators apply to every job with a supported annotation, existing ones
    included, and must be configured on every Chancy instance that pushes or
    runs jobs.
  - If the function can't be found where the job is pushed, the job is
    pushed with a :class:`~chancy.validation.ValidationSkippedWarning` and
    only validated when it runs.
  - The :class:`~chancy.plugins.cron.Cron`,
    :class:`~chancy.plugins.trigger.Trigger` and
    :class:`~chancy.plugins.workflow.WorkflowPlugin` plugins validate jobs
    when they are saved, and validate them again when they run.
  - With the process and sub-interpreter executors, validators are pickled
    to where the job runs. With the sub-interpreter executor, import a
    library that can't load there, such as pydantic, lazily: otherwise every
    job of the queue fails.
