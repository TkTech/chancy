Workers
=======

A :class:`~chancy.worker.Worker` is an asyncio-based server that listens to
one or more :doc:`queues` for :doc:`jobs` to run. Once it has a job, it uses
:doc:`executors` to run the job.

Workers can be extended with :doc:`plugins` to add additional functionality,
such as leadership election, job recovery, pruning, scheduling, and more.
Internally, queues are implemented as just another plugin.

Chancy comes with no CLI, as we've found that almost every project will
end up needing to customize the worker in some way, and most projects
already have a CLI provided by django, flask, or some other framework.
So, to start a worker, you'll need to write a script that creates a
:class:`~chancy.worker.Worker` and starts it.

.. code-block:: python
   :caption: worker.py

   import asyncio
   from chancy import Chancy, Worker, Queue
   from chancy.plugins.pruner import Pruner
   from chancy.plugins.recovery import Recovery
   from chancy.plugins.leadership import Leadership

   chancy = Chancy(
       dsn="postgresql://localhost/postgres",
       plugins=[
           Queue(name="default", concurrency=10),
           Pruner(),
           Recovery(),
           Leadership()
       ],
   )

   async def main():
       async with chancy:
           await chancy.migrate()
           await Worker(chancy).start()


.. note::

   Calling ``chancy.migrate()`` will run database migrations for the core Chancy
   tables and any configured plugins. Calling this before starting a worker is
   fine in development and testing, but in a busy production system you'll
   probably want to run migrations yourself.
