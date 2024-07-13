Jobs
====

Every unit of work in Chancy is called a :class:`~chancy.job.Job`. Jobs are
created by clients and submitted to :doc:`queues`, where they are picked up
by workers and executed.

Chancy guarantees that a job will be executed **at least once**, but it may be
executed more than once in the event of a worker crash or other failure
that requires the job to be retried. You should always write your jobs to
be idempotent, that is, they should be safe to run multiple times without
causing any problems.

Creating Jobs
-------------

Create a new Job instance with the function you want to run:

.. code-block:: python
   :caption: job.py

   from chancy import Job

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   hello_world = Job.from_func(my_dummy_task, kwargs={"name": "world"})


A Job is just a dataclass that holds the import path for a function, its
arguments, and anything else needed to run the function. You can also create a
Job from a string import path:

.. code-block:: python
   :caption: job.py

   from chancy import Job

   hello_world = Job(func="my_module.my_dummy_task", kwargs={"name": "world"})


A job is immutable once created, but you can create a new job based on an
existing job with different arguments by using the `with_` methods:

.. code-block:: python
   :caption: job.py

   from chancy import Job

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   hello_world = Job.from_func(my_dummy_task, kwargs={"name": "world"})
   hello_bob = hello_world.with_kwargs({"name": "bob"})


Once you've got a job, you can submit it to a queue:

.. code-block:: python
   :caption: job.py

   import asyncio
   from chancy import Job, Queue, Chancy

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   hello_world = Job.from_func(my_dummy_task, kwargs={"name": "world"})

   async def main():
       async with Chancy(
           dsn="postgresql://localhost/postgres",
           plugins=[
               Queue(name="default", concurrency=10),
           ],
       ) as chancy:
           await chancy.migrate()
           await chancy.push("default", hello_world)


You can push as many jobs as you want to a queue with a single push:

.. code-block:: python
   :caption: job.py

   await chancy.push("default", hello_world, hello_bob, hello_world)


The default, postgres-backed Queue will efficiently push these jobs together
in a single transaction.

Priority
--------

Jobs can have a priority, which is used to determine the order in which they
are executed. By default, jobs have a priority of 0, but you can set it to any
integer value. Lower values are executed first, and higher values are executed
later. If two jobs have the same priority, they are executed in the order they
were received.

.. code-block:: python
   :caption: job.py

   import asyncio
   from chancy import Job, Queue, Chancy

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   hello_world = Job.from_func(my_dummy_task, kwargs={"name": "world"})

   async def main():
       async with Chancy(
           dsn="postgresql://localhost/postgres",
           plugins=[
               Queue(name="default", concurrency=10),
           ],
       ) as chancy:
           await chancy.migrate()
           await chancy.push("default", hello_world)
           await chancy.push("default", hello_world.with_priority(10))
           await chancy.push("default", hello_world.with_priority(-10))


Retries
-------

Jobs can be retried a certain number of times if they fail. By default, jobs are
retried 0 times, but you can set the number of retries when creating the job.

.. code-block:: python
   :caption: job.py

   import asyncio
   from chancy import Job, Queue, Chancy

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")
       raise ValueError("Oops!")

   hello_world = Job.from_func(
      my_dummy_task,
      kwargs={"name": "world"},
      max_attempts=3
   )

If any unhandled exception occurs when running this job, the worker running it
dies, or some other unforeseen event happens, the job will be retried up to 3
times. If the job still fails after the last retry, it is marked as failed and
can be inspected later.

Future Work
-----------

Jobs can be scheduled to run at a specific time in the future

.. code-block:: python
   :caption: job.py

   import asyncio
   from datetime import datetime, timezone, timedelta
   from chancy import Job, Queue, Chancy

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   hello_world = Job.from_func(my_dummy_task, kwargs={"name": "world"})

   async def main():
       async with Chancy(
           dsn="postgresql://localhost/postgres",
           plugins=[
               Queue(name="default", concurrency=10),
           ],
       ) as chancy:
           await chancy.migrate()
           await chancy.push(
               "default",
               hello_world.with_scheduled_at(
                   datetime.now(timezone.utc) + timedelta(days=1)
               )
           )

This job will be stored in the queue and will not be picked up by a worker until
the scheduled time has passed. There's no guarantee that the job will be picked
up at **exactly** the scheduled time, but it will be picked up as soon as
possible after that time.

Resource Limits
---------------

Some job :class:`~chancy.executor.Executor` backends, like the default
:class:`~chancy.executors.process.ProcessExecutor`, can use host features
to limit the amount of resources a job can use. For example, you can limit
the amount of memory a job can use, or the time it can run for.

.. code-block:: python
   :caption: job.py

   import asyncio
   from chancy import Job, Queue, Chancy, Limit

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   hello_world = Job.from_func(
      my_dummy_task,
      kwargs={"name": "world"},
      limits=[
          Limit(Limit.Type.MEMORY, 1024 * 1024 * 1024),
          Limit(Limit.Type.TIME, 60),
      ]
   )

Each instance of this job would be allowed to use up to 1GB of memory and run
for up to 60 seconds. When these limits are set, the executor will enforce
them when running the job, and if the job exceeds the limits a standard
`MemoryError` or `TimeoutError` will be raised.


.. warning::

   It's very important to note that these limits should only be considered
   advisory, and not a security boundary. An executor that supports these
   limits will do its best to enforce them, but untrusted code can always
   find a way to disable them.


Globally unique jobs
--------------------

It's possible to give a job a globally unique identifier, which can be used to
prevent the same job from being pushed to the queue more than once. For
example, an expensive "Generate Report" job could be given a unique ID based
on the parameters of the report, and if the same report is requested again
before the first one is finished, the second request will just be silently
ignored.

.. code-block:: python
   :caption: job.py

   import asyncio
   from chancy import Job

   user_id = 1234
   hello_world = Job(
      func="my_reports.generate_report",
      kwargs={"user_id": user_id},
      unique_key=f"hello_world_{user_id}"
   )

.. note::

   Globally unique jobs should be treated as truly "global", that is they will
   be unique *across all queues*. You can always use the queue's name as part
   of your unique key to scope it to a specific queue.