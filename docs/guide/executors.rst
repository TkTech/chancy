Executors
=========

In Chancy, an Executor is responsible for running a :class:`~chancy.job.Job`
that has been received from a :class:`~chancy.queue.Queue`. Executors handle
the actual execution of the job's task function, including any necessary
setup and teardown.

Key Concepts
------------

- Each Queue uses an executor to run jobs
- Executors determine how jobs are run (e.g., in a separate process)
- Executors manage job limits, such as timeouts and memory restrictions
- Custom executors can be created for specific needs

Built-in Executor
-----------------

Chancy comes with a built-in executor that covers most use cases:

- :class:`chancy.executors.process.ProcessExecutor` - Runs jobs in separate
  process pools. This is the default executor, providing good isolation and
  flexibility, albeit with some overhead.
- :class:`chancy.executors.asyncex.AsyncExecutor` - Runs jobs in the same
  process & event loop as the worker. Useful for I/O-bound tasks that don't
  require long CPU-bound processing, like making HTTP requests.

Using Executors
---------------

By default, queues use the :class:`~chancy.executors.process.ProcessExecutor`.
The executor is specified when creating a Queue:

.. code-block:: python
   :caption: worker.py

   import asyncio
   from chancy import Chancy, Queue, Worker

   async def main():
       async with Chancy(dsn="postgresql://localhost/postgres") as chancy:
           await chancy.migrate()
           await chancy.declare(
               Queue(
                  name="default",
                  concurrency=10,
                  executor='chancy.executors.process.ProcessExecutor',
                  executor_options={"maximum_jobs_per_worker": 1000}
               )
           )
           await Worker(chancy).start()

   if __name__ == "__main__":
       asyncio.run(main())

.. note::

   We specify the executor class as a string and its options as a dictionary
   because they'll be serialized and stored in the database.


Next Steps
----------
- Learn about :doc:`jobs` to understand the core unit of work in Chancy
- Explore :doc:`queues` to see how jobs are organized and distributed
- Dive into :doc:`workers` to understand how jobs are processed
- Discover :doc:`plugins` to extend Chancy's functionality