Jobs
====

Every unit of work in Chancy is called a Job. Jobs are created by clients and
submitted to a queue, where they are picked up by workers and executed.

Creating Jobs
-------------

Unlike most other Python task queues, Chancy doesn't encourage you to wrap
your functions in a special decorator or class. Instead, you define your
Job as a normal Python function and pass it to the Job constructor:

.. code-block:: python
   :caption: job.py

   import asyncio
   from chancy.executor import Job
   from chancy.queue import Queue
   from chancy.app import Chancy

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   async def main():
       async with Chancy(
           dsn="postgresql://localhost/postgres",
           queues=[
               Queue(name="default", concurrency=10),
           ],
       ) as chancy:
           await chancy.migrate()
           await chancy.push(
             "default",
             Job.from_func(my_dummy_task, kwargs={"name": "world"})
           )

A Job is just a dataclass that holds the function, its arguments, and various
job-related metadata. Once created, a job is immutable, and can be re-used
safely, ex:


.. code-block:: python
   :caption: job.py

   import asyncio
   from chancy.executor import Job
   from chancy.queue import Queue
   from chancy.app import Chancy

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   hello_world = Job.from_func(my_dummy_task, kwargs={"name": "world"})

   async def main():
       async with Chancy(
           dsn="postgresql://localhost/postgres",
           queues=[
               Queue(name="default", concurrency=10),
           ],
       ) as chancy:
           await chancy.migrate()
           await chancy.push("default", hello_world)
           await chancy.push("default", hello_world)
           # with_priority will create a _new_ Job!
           await chancy.push("default", hello_world.with_priority(10))


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
   from chancy.executor import Job
   from chancy.queue import Queue
   from chancy.app import Chancy

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   hello_world = Job.from_func(my_dummy_task, kwargs={"name": "world"})

   async def main():
       async with Chancy(
           dsn="postgresql://localhost/postgres",
           queues=[
               Queue(name="default", concurrency=10),
           ],
       ) as chancy:
           await chancy.migrate()
           await chancy.push("default", hello_world)
           await chancy.push("default", hello_world.with_priority(10))
           await chancy.push("default", hello_world.with_priority(-10))