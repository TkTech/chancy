Chancy
======

.. image:: https://img.shields.io/github/license/tktech/chancy
   :alt: MIT License

.. image:: https://img.shields.io/codecov/c/github/TkTech/chancy
   :alt: Codecov

.. image:: https://img.shields.io/pypi/v/chancy
   :alt: PyPI Version

.. image:: https://img.shields.io/pypi/pyversions/chancy
   :alt: Supported Versions

.. image:: https://img.shields.io/badge/os-Linux%20|%20macOS-blue
   :alt: OS Platforms

.. image:: https://img.shields.io/badge/postgres-%2014%20|%2015%20|%2016%20|%2017-blue
   :alt: PostgreSQL Versions

Chancy is a distributed task queue that uses Postgres as its message broker.
It's designed to be simple to use, easy to deploy, and easily extendable. Chancy
is a great alternative to Celery, RabbitMQ, or redis-based task queues.


Key Features:
-------------

- **Robust Jobs** - support for priorities, retries, timeouts, scheduling,
  global rate limits, memory limits, global uniqueness, error
  capture, cancellation, and more
- **Minimal dependencies** - Core functionality requires only psycopg3 - which
  can be installed side-by-side with psycopg2.
- **Minimal infrastructure** - No need to run a separate service like
  RabbitMQ or redis. Every feature is built on top of Postgres.
- **Plugins** - Several plugins including a :class:`dashboard<chancy.plugins.api.Api>`,
  :class:`workflows<chancy.plugins.workflow.WorkflowPlugin>`,
  :class:`cron jobs<chancy.plugins.cron.Cron>`, and :doc:`much more <chancy.plugins>`
- **Flexible & Dynamic** - Queues can be created, deleted, and modified
  at runtime. A single worker can handle many queues and mix threads,
  processes, sub-interpreters, and asyncio jobs.
- **async-first** - Internals designed from the ground up to be async-first,
  but has minimal sync APIs for easy integration with existing non-async
  codebases.
- **Transactional enqueueing** - Atomically enqueue jobs and the data they
  depend on in a single transaction.
- **100% open & free** - no enterprise tiers or paid features. Checkout
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

     @job()
     def hello_world(*, name: str):
         print(f"Hello, {name}!")

     chancy = Chancy("postgresql://localhost/postgres")

     async def main():
         async with chancy:
             # Declare the default queue
             await chancy.declare(Queue("default"))
             # Run the database migrations
             await chancy.migrate()
             # Push a job
             await chancy.push(hello_world.job.with_kwargs(name="World"))
             # Start the worker (ctrl+c to exit)
             async with Worker(chancy) as worker:
                 await worker.wait_for_shutdown()

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

     @job()
     def hello_world(*, name: str):
         print(f"Hello, {name}!")

     chancy = Chancy("postgresql://localhost/postgres")

  Then run the database migrations:

  .. code-block:: bash

     chancy --app worker.chancy misc migrate

  Declare the default queue:

  .. code-block:: bash

      chancy --app worker.chancy queue declare default

  Push a job:

  .. code-block:: bash

     chancy --app worker.chancy queue push worker.hello_world --kwargs '{"name": "world"}'

  Start a worker:

  .. code-block:: bash

     chancy --app worker.chancy worker start





Congratulations! You've just run your first Chancy job. Next, explore the
:doc:`How To <howto/index>` or :doc:`plugins <chancy.plugins>`.

Dashboard
---------

Chancy comes with a built-in :class:`dashboard <chancy.plugins.api.Api>` -
no need to run a separate service like Flower.

.. image:: ../misc/ux_jobs.png
    :alt: Jobs page

.. image:: ../misc/ux_job_failed.png
    :alt: Failed job page

.. image:: ../misc/ux_queue.png
    :alt: Queue page

.. image:: ../misc/ux_workflow.png
    :alt: Worker page


Similar Projects
----------------

With the addition of modern Postgres features like ``LISTEN/NOTIFY`` and
``SELECT FOR UPDATE...SKIP LOCKED``, postgres-backed task queues have
become a viable alternative to other task queues built on RabbitMQ or
redis like celery_. As such the space is exploding with new projects.
Here are some of the most popular ones if Chancy doesn't fit your
needs:

.. list-table::
  :header-rows: 1
  :widths: 20 20 60

  * - Project
    - Language
    - Note
  * - celery_
    - Python
    - The defacto Python task queue
  * - procastinate_
    - Python
    -
  * - oban_
    - Elixir
    - Inspired many of the features in Chancy
  * - river_
    - Go
    -
  * - neoq_
    - Go
    -
  * - faktory_
    - Go
    -
  * - pg-boss_
    - Node.js
    -
  * - graphile_
    - Node.js
    -
  * - Minion_
    - Perl
    -

.. _celery: https://docs.celeryproject.org/en/stable/
.. _oban: https://hexdocs.pm/oban/Oban.html
.. _river: https://github.com/riverqueue/river
.. _procastinate: https://procrastinate.readthedocs.io/
.. _graphile: https://worker.graphile.org/
.. _neoq: https://github.com/acaloiaro/neoq
.. _faktory: https://github.com/contribsys/faktory
.. _pg-boss: https://github.com/timgit/pg-boss
.. _Minion: https://github.com/mojolicious/minion

.. toctree::
   :maxdepth: 4
   :caption: Contents:
   :hidden:

   howto/index
   chancy
   design
   faq


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`