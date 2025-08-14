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

   from chancy import Limit, job

   @job(limits=[
       Limit(Limit.Type.MEMORY, 1024 * 1024 * 1024),
       Limit(Limit.Type.TIME, 60),
   ])
   def greet(*, name: str):
       print(f"Hello, {name}!")

Not all executors will support all types of limits. For example only
the default :class:`~chancy.executors.process.ProcessExecutor` supports
memory limits.

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

Concurrency
-----------------------

Control the number of jobs that can run simultaneously using concurrency:

.. code-block:: python

    from chancy import job

    @job()
    def process_user_data(*, user_id: str, action: str):
        print(f"Processing {action} for user {user_id}")

    async with Chancy("postgresql://localhost/postgres") as chancy:
        # Limit to 1 concurrent job per user_id
        job_with_limit = process_user_data.job.with_concurrency(
            max_concurrent=1,
            key="user_id"
        )
        await chancy.push(job_with_limit.with_kwargs(user_id="123", action="upload"))

The ``key`` parameter determines how jobs are grouped for concurrency limits:

**Field-based keys**: Use a parameter name to group by that field's value:

.. code-block:: python

    # Limit by user_id - max 1 job per user
    job.with_concurrency(1, "user_id")

**Callable keys**: Use a function to compute complex grouping keys:

.. code-block:: python

    # Limit by user + action combination
    job.with_concurrency(
        max_concurrent=2,
        key=lambda user_id, action, **kw: f"{user_id}:{action}"
    )

**Function-level limits**: Omit the key to limit all jobs of this type:

.. code-block:: python

    # Limit total concurrent jobs of this type to 5
    job.with_concurrency(5)

.. note::

    Concurrency constraints are enforced globally across all workers in your
    cluster. Jobs exceeding the limit will wait in the queue until a slot
    becomes available.
