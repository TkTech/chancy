Chancy
======

A postgres-backed task queue for Python.

.. warning::

  This project is currently in the early stages of development and its
  API may change often. Use at your own risk. It's practically guaranteed
  to have bugs and missing features.


Key Features:
-------------

- Postgres-backed for reliability and familiarity
- Support for job priorities, retries, timeouts, and scheduling
- Configurable job retention for easy debugging and tracking
- Minimal dependencies (only psycopg3 required)
- Optional transactional job queueing
- asyncio-based worker for efficient processing

Quick Start
-----------

1. Install Chancy:

.. code-block:: bash

   $ pip install chancy

2. Setup a basic worker (worker.py):

.. code-block:: python
  :caption: worker.py

   import asyncio
   from chancy import Chancy, Worker, Queue

   chancy = Chancy(dsn="postgresql://localhost/postgres")

   async def main():
       async with chancy:
           await chancy.migrate()
           await chancy.declare(Queue("default"))
           await Worker(chancy).start()

   if __name__ == "__main__":
       asyncio.run(main())

3. Create and queue a job (job.py):

.. code-block:: python
  :caption: job.py

   import asyncio
   from chancy import Job
   from worker import chancy

   def my_dummy_task(name: str):
       print(f"Hello, {name}!")

   async def main():
       async with chancy:
           job = Job(func="job.my_dummy_task", kwargs={"name": "world"}
           await chancy.push("default", job)

   if __name__ == "__main__":
       asyncio.run(main())

4. Run the worker in one terminal:

.. code-block:: bash

   $ python worker.py

5. Push the job in another terminal:

.. code-block:: bash

   $ python job.py


Congratulations! You've just run your first Chancy job.

Next Steps
----------

- Learn about :doc:`Jobs <guide/jobs>` in detail
- Understand how :doc:`Queues <guide/queues>` work
- Explore :doc:`Workers <guide/workers>` and their configuration
- Dive into :doc:`Plugins <guide/plugins>` for extending Chancy's functionality
- Read the :doc:`chancy` module documentation for a complete reference of
  Chancy's API.


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

   guide/index
   chancy


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
