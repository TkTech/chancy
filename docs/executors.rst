Executors
=========

Once a Job has been received off of a queue, it needs to be executed. This is
where Executors come in. Executors are responsible for running the job's task
function and handling any necessary setup and teardown.

Chancy comes with a few built-in executors that should cover most use cases:

- :class:`chancy.executors.process.ProcessExecutor` - Runs the job in a separate
  process pool. This is the default executor and is the most flexible, but also
  has the highest overhead.


The :class:`~chancy.executors.process.ProcessExecutor` is the default, since it
provides decent default isolation and can handle most use cases without
needing to think about it.

Changing Executors
------------------

Each Queue you define can have its own executor, and you can even write your
own custom executors if you need to.

To use a different executor, simply pass it to the Queue constructor:

.. code-block:: python
   :caption: worker.py

   import asyncio
   from chancy import Chancy, Queue, Worker
   from chancy.executors.process import ProcessExecutor

   async def main():
       async with Chancy(
           dsn="postgresql://localhost/postgres",
           plugins=[
               Queue(name="default", concurrency=10, executor=ProcessExecutor),
           ],
       ) as chancy:
           await chancy.migrate()
           await Worker(chancy).start()

If you need to customize the executor's options, you can use a lambda to pass
arguments to the executor's constructor:

.. code-block:: python

   Queue(
       name="default",
       concurrency=10,
       executor=lambda queue: ProcessExecutor(queue, maximum_jobs_per_worker=1000),
   ),