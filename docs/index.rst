Chancy
======

A postgres-backed task queue for Python.

.. image:: https://img.shields.io/github/license/tktech/chancy
   :alt: MIT License

.. image:: https://img.shields.io/pypi/pyversions/chancy
   :alt: Supported Versions


Key Features:
-------------

- Support for job priorities, retries, timeouts, scheduling,
  global rate limits, memory limits, unique jobs, and more
- asyncio-based worker with support for asyncio, threading,
  process, and sub-interpreter job executors
- Configurable job retention for easy debugging and tracking
- Minimal dependencies (only psycopg3 required)
- Plugins for a :class:`dashboard<chancy.plugins.api.Api>`,
  :class:`workflows<chancy.plugins.workflow.WorkflowPlugin>`,
  :class:`cron jobs<chancy.plugins.cron.Cron>`, and much more
- Optional transactional enqueueing for atomic job creation
- asyncio & sync APIs for easy integration with existing codebases

Quick Start
-----------

1. Install Chancy:

.. tab:: Code

  .. code-block:: bash

     pip install chancy

.. tab:: CLI

  .. code-block:: bash

     pip install chancy[cli]


2. Setup a worker (worker.py) with all the bells and whistles:

.. tab:: Code

  .. code-block:: python
    :caption: worker.py

     import asyncio
     from chancy import Chancy, Worker, Queue, Job
     from chancy.plugins.pruner import Pruner
     from chancy.plugins.recovery import Recovery
     from chancy.plugins.leadership import Leadership
     from chancy.plugins.cron import Cron
     from chancy.plugins.api import Api
     from chancy.plugins.workflow import WorkflowPlugin

     def hello_world(*, name: str):
         print(f"Hello, {name}!")

     async def main():
         async with Chancy(
             dsn="postgresql://localhost/postgres",
             plugins=[
                  Pruner(),
                  Recovery(),
                  Leadership(),
                  Cron(),
                  Api(),
                  WorkflowPlugin(),
             ]
         ) as chancy:
              # Migrate the database to the latest version (don't do this in prod)
             await chancy.migrate()
             # Declares a queue to all workers called "default".
             await chancy.declare(Queue("default", concurrency=10))
             # Push a job onto the default queue
             await chancy.push(
                 Job.from_func(hello_world, kwargs={"name": "world"}, queue="default")
             )
             # Start the worker and keep it running until we terminate it.
             async with Worker(chancy) as worker:
                 await worker.wait_until_complete()

     if __name__ == "__main__":
         asyncio.run(main())

.. tab:: CLI

  .. code-block:: python
     :caption: worker.py

     from chancy import Chancy
     from chancy.plugins.pruner import Pruner
     from chancy.plugins.recovery import Recovery
     from chancy.plugins.leadership import Leadership
     from chancy.plugins.cron import Cron
     from chancy.plugins.api import Api
     from chancy.plugins.workflow import WorkflowPlugin

     def hello_world(*, name: str):
         print(f"Hello, {name}!")

     chancy = Chancy(
         dsn="postgresql://localhost/postgres",
         plugins=[
             Pruner(),
             Recovery(),
             Leadership(),
             Cron(),
             Api(),
             WorkflowPlugin(),
         ]
     )


3. Run the worker:

.. tab:: Code

  .. code-block:: bash

     python worker.py

.. tab:: CLI

  .. code-block:: bash

     chancy --app worker.chancy misc migrate
     chancy --app worker.chancy queue declare default --concurrency 10
     chancy --app worker.chancy queue push worker.hello_world --kwargs '{"name": "world"}' --queue default
     chancy --app worker.chancy worker start


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

   howto/index
   chancy


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`