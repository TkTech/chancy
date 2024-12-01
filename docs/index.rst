Chancy
======

A postgres-backed task queue for Python.

.. image:: https://img.shields.io/github/license/tktech/chancy
   :alt: MIT License

.. image:: https://img.shields.io/pypi/pyversions/chancy
   :alt: Supported Versions

.. image:: https://img.shields.io/codecov/c/github/TkTech/chancy
   :alt: Codecov

.. image:: https://img.shields.io/pypi/v/chancy
   :alt: PyPI Version


Key Features:
-------------

- Jobs support priorities, retries, timeouts, scheduling,
  global rate limits, memory limits, global uniqueness, error
  capture, cancellation, and more
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

.. tab:: Code

  Install Chancy:

  .. code-block:: bash

     pip install chancy

  Create a new file called ``worker.py``:

  .. code-block:: python
    :caption: worker.py

     import asyncio
     from chancy import Chancy, Worker, Queue, job

     @job(queue="default")
     def hello_world(*, name: str):
         print(f"Hello, {name}!")

     chancy = Chancy("postgresql://localhost/postgres")

     async def main():
         async with chancy:
             # Run the database migrations
             await chancy.migrate()
             # Declare a queue
             await chancy.declare(Queue("default", concurrency=10))
             # Push a job
             await chancy.push(hello_world.job.with_kwargs(name="World"))
             # Start the worker (ctrl+c to exit)
             async with Worker(chancy) as worker:
                 await worker.wait_until_complete()

     if __name__ == "__main__":
         asyncio.run(main())

.. tab:: CLI

  Install Chancy & its CLI:

  .. code-block:: bash

     pip install chancy[cli]

  Create a new file called ``worker.py``:

  .. code-block:: python
     :caption: worker.py

     from chancy import Chancy, job

     @job(queue="default")
     def hello_world(*, name: str):
         print(f"Hello, {name}!")

     chancy = Chancy("postgresql://localhost/postgres")

  Then run the database migrations:

  .. code-block:: bash

     chancy --app worker.chancy misc migrate

  Declare a queue:

  .. code-block:: bash

     chancy --app worker.chancy queue declare default --concurrency 10

  Push a job:

  .. code-block:: bash

     chancy --app worker.chancy queue push worker.hello_world --kwargs '{"name": "world"}'

  Start the worker:

  .. code-block:: bash

     chancy --app worker.chancy worker start



Congratulations! You've just run your first Chancy job. Next, explore the
:doc:`How To <howto/index>` or :doc:`plugins <chancy.plugins>`.


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