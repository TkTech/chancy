Jobs
====

In Chancy, a Job represents a unit of work to be executed asynchronously.
Understanding how to create, configure, and manage jobs is crucial for
effectively using Chancy.

Creating a Job
--------------

There are two main ways to create a job:

1. From a Python function:

   .. code-block:: python

      from chancy import Job

      def greet(name: str):
          print(f"Hello, {name}!")

      job = Job.from_func(greet, kwargs={"name": "Alice"})

2. From a string import path:

   .. code-block:: python

      job = Job(func="mymodule.greet", kwargs={"name": "Bob"})


Jobs are immutable once created - use the `with_` methods on a Job to create
a new job with modified properties.


Priority
^^^^^^^^

Priority determines the order of execution. Lower values run first:

.. code-block:: python

   high_priority_job = job.with_priority(-10)
   low_priority_job = job.with_priority(10)

Retry Attempts
^^^^^^^^^^^^^^

Specify how many times a job should be retried if it fails:

.. code-block:: python

   Job.from_func(greet, kwargs={"name": "Charlie"}, max_attempts=3)

Scheduled Execution
^^^^^^^^^^^^^^^^^^^

Schedule a job to run some time in the future:

.. code-block:: python

   from datetime import datetime, timedelta, timezone

   future_job = Job.from_func(greet, kwargs={"name": "David"})
   future_job = future_job.with_scheduled_at(
       datetime.now(timezone.utc) + timedelta(hours=1)
   )

.. note::

    Scheduled jobs are guaranteed to run *at* or *after* the scheduled time,
    but not *exactly* at that time.

If you need recurring jobs, take a look at the
:class:`chancy.plugins.cron.Cron` plugin.

Resource Limits
^^^^^^^^^^^^^^^

Set memory and time limits for job execution:

.. code-block:: python

   from chancy import Limit

   limited_job = Job.from_func(
       greet,
       kwargs={"name": "Eve"},
       limits=[
           Limit(Limit.Type.MEMORY, 1024 * 1024 * 1024),  # 1GB
           Limit(Limit.Type.TIME, 60),  # 60 seconds
       ]
   )

Queueing a Job
--------------

Once you've created a job, queue it for execution:

.. code-block:: python

   async with Chancy(dsn="postgresql://localhost/postgres") as chancy:
       await chancy.push("default", job)

Queue multiple jobs at once:

.. code-block:: python

   await chancy.push_many("default", job1, job2, job3)

Unique Jobs
-----------

Prevent duplicate job execution by assigning a unique key:

.. code-block:: python

   user_id = 1234
   unique_job = Job(
      func="generate_report",
      kwargs={"user_id": user_id},
      unique_key=f"report_{user_id}"
   )

.. note::

  Unique jobs ensure only one job with the same ``unique_key`` is
  queued or running at a time, but any number can be completed or
  failed.

Example
-------

Here's an example that puts it all together:

.. code-block:: python

   import asyncio
   from datetime import datetime, timedelta, timezone
   from chancy import Chancy, Job, Limit

   async def process_order(order_id: int):
       # Simulating order processing
       print(f"Processing order {order_id}")
       # ... actual processing logic here ...
       print(f"Order {order_id} processed successfully")

   async def main():
       async with Chancy(dsn="postgresql://localhost/postgres") as chancy:
           # Create a job
           job = Job.from_func(
               process_order,
               kwargs={"order_id": 12345},
               max_attempts=3,
               limits=[
                   Limit(Limit.Type.MEMORY, 512 * 1024 * 1024),  # 512MB
                   Limit(Limit.Type.TIME, 300),  # 5 minutes
               ]
           )

           # Schedule it for 1 hour from now
           scheduled_job = job.with_scheduled_at(
               datetime.now(timezone.utc) + timedelta(hours=1)
           )

           # Queue the job
           reference = await chancy.push("default", scheduled_job)

           # Wait for job completion (in a real scenario, you might
           # not wait synchronously)
           completed_job = await reference.wait()
           print(f"Final job status: {completed_job.state}")

   if __name__ == "__main__":
       asyncio.run(main())

Next Steps
----------
- Explore :doc:`queues` to see how jobs are organized and distributed
- Dive into :doc:`workers` to understand how jobs are processed
- Check out :doc:`executors` to learn about different ways of running jobs
- Discover :doc:`plugins` to extend Chancy's functionality