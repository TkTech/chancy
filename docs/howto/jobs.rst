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

Control the number of jobs with the same concurrency key that can run
simultaneously across all workers and queues using with_concurrency():

.. code-block:: python

    from chancy import job, ConcurrencyRule

    @job()
    def process_user_data(*, user_id: str, action: str):
        print(f"Processing {action} for user {user_id}")

    async with Chancy("postgresql://localhost/postgres") as chancy:
        # Limit to 1 concurrent job per user_id
        job_with_limit = process_user_data.job.with_concurrency(
            ConcurrencyRule(
                max=1,
                key="user_id"
            )
        )
        await chancy.push(job_with_limit.with_kwargs(user_id="123", action="upload"))

The ``key`` parameter determines how jobs are grouped for concurrency limits:

**Field-based keys**: Use a parameter name to group by that field's value:

.. code-block:: python

    # Limit by user_id - max 1 job per user
    job.with_concurrency(ConcurrencyRule(max=1, key="user_id"))

**Callable keys**: Use a function to compute complex grouping keys:

.. code-block:: python

    # Limit by user + action combination
    job.with_concurrency(
        ConcurrencyRule(
            max=2,
            key=lambda user_id, action, **kw: f"{user_id}:{action}"
        )
    )

**Function-level limits**: Omit the key to limit all jobs of this type:

.. code-block:: python

    # Limit total concurrent jobs of this type to 5
    job.with_concurrency(ConcurrencyRule(max=5))

.. note::

    Concurrency constraints are enforced globally across all workers in your
    cluster. Jobs exceeding the limit will wait in the queue until a slot
    becomes available.

More about concurrency
~~~~~~~~~~~~~~~~~~~~~~

**Concurrency is checked at fetch time:** Chancy enforces concurrency limits
when workers fetch jobs by counting running jobs directly rather than
maintaining counters or leases. This pragmatic design keeps things simple
and robust:

- **Atomic:** The check and claim happen in a single query, eliminating race
  conditions between workers.
- **Easy recovery:** If a worker crashes, the recovery plugin marks the job as
  pending again and the slot is automatically freed. No counters to decrement,
  no leases to release.
- **Minimal overhead:** When few jobs use concurrency limits, performance
  impact is negligible. Jobs without a concurrency key pass straight through,
  and the database efficiently skips empty intermediate results.

Any drawbacks of this approach (such as scan window limitations) can be
mitigated by leveraging Chancy's queue architecture to separate workloads.

**How jobs are fetched:** Workers scan pending jobs by priority within a
configurable window. Jobs that cannot run due to concurrency limits are
skipped. If many jobs in the scan window are blocked, eligible jobs outside
the window won't be considered until the next fetch cycle.

**When to use a dedicated queue:** If you have concurrency keys with strict
low limits but high volume, consider placing them in a dedicated queue. For
example, if you limit per-user processing to 1 concurrent job but have
thousands of users submitting work simultaneously, these jobs can fill the
scan window and delay other work.

This separation ensures:

- Unconstrained jobs aren't starved by concurrency-blocked jobs
- Each queue can be tuned separately

**Tuning the scan window:** The scan limit determines how many pending jobs
the worker examines when looking for work. It is calculated as::

    scan_limit = min(batch_size * scan_factor, scan_limit_upper_bound)

Where:

- **batch_size**: Number of jobs the worker wants to fetch (based on queue concurrency)
- **scan_factor**: Multiplier applied to batch_size (default: 20)
- **scan_limit_upper_bound**: Maximum scan limit regardless of batch_size (default: 1000)

For example, with defaults and a worker fetching 10 jobs::

    scan_limit = min(10 * 20, 1000) = 200 jobs scanned

You can tune these parameters per-queue:

.. code-block:: python

    Queue(
        "user-processing",
        scan_factor=50,              # default: 20
        scan_limit_upper_bound=5000, # default: 1000
    )

Higher values reduce the chance of starvation but increase query cost.
