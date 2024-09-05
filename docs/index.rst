Chancy
======

A postgres-backed task queue for Python.

.. warning::

  This project is currently in the early stages of development and its
  API may change often. Use at your own risk. It's practically guaranteed
  to have bugs and missing features.


Key Features:
-------------

- Support for job priorities, retries, timeouts, scheduling,
  global rate limits, memory limits, and more.
- Configurable job retention for easy debugging and tracking
- Minimal dependencies (only psycopg3 required)
- asyncio & sync APIs for easy integration with existing codebases
- Plugins for a :class:`dashboard<chancy.plugins.api.Api>`,
  :class:`workflows<chancy.plugins.workflow.WorkflowPlugin>`,
  :class:`cron jobs<chancy.plugins.cron.Cron>`, and more

Quick Start
-----------

1. Install Chancy:

.. code-block:: bash

   $ pip install chancy

2. Setup a basic worker (worker.py):

.. code-block:: python
  :caption: worker.py

   import asyncio
   from chancy import Chancy, Worker, Queue, Job

   def hello_world(*, name: str):
       print(f"Hello, {name}!")

   async def main():
       async with Chancy(dsn="postgresql://localhost/postgres") as chancy:
            # Migrate the database to the latest version (don't do this in prod)
           await chancy.migrate()
           # Declares a queue to all workers called "default".
           await chancy.declare(Queue("default", concurrency=10))
           # Push a job onto the default queue
           await chancy.push(
              Job.from_func(hello_world, kwargs={"name": "world"}, queue="default")
           )
           # Start the worker and keep it running until we terminate it.
           await Worker(chancy).start()

   if __name__ == "__main__":
       asyncio.run(main())

3. Run the worker:

.. code-block:: bash

   $ python worker.py

Congratulations! You've just run your first Chancy job.


Similar Projects
----------------

- celery_ is the most popular task queue for Python, but is also heavyweight
  and suffers from some design quirks that can make it difficult to use, like
  future scheduled tasks using up all worker memory.
- oban_ is a postgres-backed task queue for Elixir that inspired quite a few
  design decisions in Chancy. The oban section of the Elixir forum is a
  fantastic resource for finding the common pitfalls and uses of a
  postgres-backed task queue.
- river_ is a postgres-backed task queue for Go.
- procastinate_ is a postgres-backed task queue for Python that has been around
  for a long time and offers strong django integration.


.. _celery: https://docs.celeryproject.org/en/stable/
.. _oban: https://hexdocs.pm/oban/Oban.html
.. _river: https://github.com/riverqueue/river
.. _procastinate: https://procrastinate.readthedocs.io/

.. toctree::
   :maxdepth: 4
   :caption: Contents:
   :hidden:

   chancy


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`