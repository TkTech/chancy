Chancy
======

A postgres-backed task queue for Python.

.. image:: https://img.shields.io/github/license/tktech/chancy
   :alt: MIT License

.. image:: https://img.shields.io/pypi/pyversions/chancy
   :alt: Supported Versions


Key Features:
-------------

- Jobs support priorities, retries, timeouts, scheduling,
  global rate limits, memory limits, global uniqueness, error
  capture, and more
- Minimal dependencies (only psycopg3 required)
- Plugins for a :class:`dashboard<chancy.plugins.api.Api>`,
  :class:`workflows<chancy.plugins.workflow.WorkflowPlugin>`,
  :class:`cron jobs<chancy.plugins.cron.Cron>`, and :doc:`much more <chancy.plugins>`
- Optional transactional enqueueing for atomic job creation
- asyncio & sync APIs for easy integration with existing codebases
- 100% open & free - no enterprise or paid features. Checkout
  the repo on `GitHub <https://github.com/tktech/chancy>`_.

Quick Start
-----------

1. Install Chancy:

.. tab:: Code

  .. code-block:: bash

     pip install chancy

.. tab:: CLI

  .. code-block:: bash

     pip install chancy[cli]


2. Setup a worker (worker.py), run the migrations, make a queue, and push a job:

.. tab:: Code

  .. code-block:: python
    :caption: worker.py

     import asyncio
     from chancy import Chancy, Worker, Queue, as_job

     @as_job(queue="default")
     def hello_world(*, name: str):
         print(f"Hello, {name}!")

     async def main():
         async with Chancy("postgresql://localhost/postgres") as chancy:
             await chancy.migrate()
             await chancy.declare(Queue("default", concurrency=10))
             await chancy.push(hello_world.job.with_kwargs({"name": "world"}))
             async with Worker(chancy) as worker:
                 await worker.wait_until_complete()

     if __name__ == "__main__":
         asyncio.run(main())

.. tab:: CLI

  .. code-block:: python
     :caption: worker.py

     from chancy import Chancy, as_job

     @as_job(queue="default")
     def hello_world(*, name: str):
         print(f"Hello, {name}!")

     chancy = Chancy("postgresql://localhost/postgres")


3. Run the worker:

.. tab:: Code

  .. code-block:: bash

     python worker.py

.. tab:: CLI

  .. code-block:: bash

     chancy --app worker.chancy misc migrate
     chancy --app worker.chancy queue declare default --concurrency 10
     chancy --app worker.chancy queue push worker.hello_world --kwargs '{"name": "world"}'
     chancy --app worker.chancy worker start


Congratulations! You've just run your first Chancy job.


Similar Projects
----------------

With the addition of modern Postgres features like ``LISTEN/NOTIFY`` and
``SELECT FOR UPDATE...SKIP LOCKED``, postgres-backed task queues have
become a viable alternative to other task queues built on RabbitMQ or
redis like celery_. As such the space is exploding with new projects.
Here are some of the most popular ones:

- celery_ is the most popular task queue for Python, but is also heavyweight
  and suffers from some design quirks that can make it difficult to use, like
  future scheduled tasks using up all worker memory.
- procastinate_ is a postgres-backed task queue for Python that has been around
  for a long time and offers strong django integration.
- oban_ is a postgres-backed task queue for Elixir that inspired quite a few
  design decisions in Chancy. The oban section of the Elixir forum is a
  fantastic resource for finding the common pitfalls and uses of a
  postgres-backed task queue.
- river_ is a postgres-backed task queue for Go.
- graphile_ is a postgres-backed task queue for Node.js
- neoq_ is a task queue for Go which supports postgres
- faktory_ is a postgres-backed task queue for Go
- pg-boss_ is a postgres-backed task queue for Node.js

.. _celery: https://docs.celeryproject.org/en/stable/
.. _oban: https://hexdocs.pm/oban/Oban.html
.. _river: https://github.com/riverqueue/river
.. _procastinate: https://procrastinate.readthedocs.io/
.. _graphile: https://worker.graphile.org/
.. _neoq: https://github.com/acaloiaro/neoq
.. _faktory: https://github.com/contribsys/faktory
.. _pg-boss: https://github.com/timgit/pg-boss

.. toctree::
   :maxdepth: 4
   :caption: Contents:
   :hidden:

   howto/index
   chancy
   faq


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`