Custom Executors
================

An :class:`~chancy.executor.Executor` is typically responsible for taking a job
retrieved from a queue that represents a Python function and executing that
function. However, custom executors can do anything you want with a job. You
could run a binary inside an Amazon Firecracker VM, send the job as gcode
to a 3D printer, whatever.

To create a custom executor, subclass :class:`~chancy.executor.Executor` and
implement the :meth:`~chancy.executor.Executor.push` method. This method should
take a job and process it. Lets make a simple executor that imports a function
from a module and just calls it in the worker process without any concurrency:


.. code-block:: python
   :caption: executor.py

   import datetime
   import dataclasses
   from chancy.executor import Executor, JobInstance

   class MyExecutor(Executor):
       async def push(self, job: JobInstance):
           mod_name, func_name = job.func.rsplit(".", 1)
           mod = __import__(mod_name, fromlist=[func_name])
           func = getattr(mod, func_name)

           kwargs = job.kwargs or {}
           try:
              func(**kwargs)
           except Exception:
               await self.queue.push_job_update(
                   dataclasses.replace(
                       job,
                       state=(
                           JobInstance.State.FAILED
                           if job.attempts + 1 >= job.max_attempts
                           else JobInstance.State.RETRYING
                       ),
                       attempts=job.attempts + 1,
                   )
               )
           else:
               await self.queue.push_job_update(
                   dataclasses.replace(
                       job,
                       state=JobInstance.State.SUCCEEDED,
                       completed_at=datetime.datetime.now(
                           tz=datetime.timezone.utc
                       ),
                   )
               )

Then, pass an instance of your custom executor to the
:class:`~chancy.queue.Queue` constructor:

.. code-block:: python
   :caption: worker.py

   import asyncio
   from chancy import Chancy, Worker, Queue
   from my_module import MyExecutor

   async def main():
       async with Chancy(
           dsn="postgresql://localhost/postgres",
           plugins=[
               Queue(name="default", concurrency=10, executor=MyExecutor),
           ],
       ) as chancy:
           await chancy.migrate()
           await chancy.push(
               "default",
               Job(
                  func="my_module.my_dummy_task",
                  kwargs={"name": "world"}
               )
           )
           await Worker(chancy).start()


Now, any job that is pushed to the "default" queue will be executed by
MyExecutor.