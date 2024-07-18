Chancy
======

A postgres-backed task queue for Python.

.. warning::

  This project is currently in the early stages of development and its
  API may change often. Use at your own risk. It's practically guaranteed
  to have bugs and missing features.

Why
---

The goal is to provide a simple, easy-to-use task queue that can be used in a
wide variety of projects where the only infrastructure requirement is a postgres
server that you're probably already using anyway.

Key Features
------------

- Fully-featured jobs support priorities, retries, timeouts, memory limits,
  future scheduling, and more.
- Completed jobs stick around in the database for easy debugging and job
  tracking, with configurable retention policies. Inspect your jobs with plain
  old SQL, an HTTP API, or the dashboard.
- Core is dependency-free except for psycopg3, but plugins can add additional
  dependencies.
- Optional transactional job queueing - only queue a job if your transaction
  commits successfully.
- asyncio-based worker, can be run in-process with your web server on small
  projects or as a standalone worker on larger projects.
- Statically declare your queues or create and manage them on-the-fly,
  assigning them to workers based on tags.

Installation
------------

Chancy is available on PyPI. You can install it with pip:

.. code-block:: bash

   pip install chancy

From Source
~~~~~~~~~~~

You can also install Chancy from source by cloning the repository and running
``pip install .`` in the root directory:

.. code-block:: bash

    git clone https://github.com/TkTech/chancy.git
    cd chancy
    pip install .

Usage
-----

Using Chancy is fairly straightforward. Just like any work queue, you need to
have a worker that listens for jobs and a client that submits jobs to the queue.
First, we'll create a file called ``worker.py``:

.. code-block:: python
  :caption: worker.py

   import asyncio
   from chancy import Chancy, Worker, Queue
   from chancy.plugins.pruner import Pruner
   from chancy.plugins.recovery import Recovery
   from chancy.plugins.leadership import Leadership
   from chancy.plugins.web import Web

   chancy = Chancy(
       dsn="postgresql://localhost/postgres",
       plugins=[
           Pruner(),
           Recovery(),
           Leadership(),
           Web(),
       ],
   )

   async def main():
       async with chancy:
           await chancy.migrate()
           await chancy.declare(Queue(name="default", concurrency=10))
           await Worker(chancy).start()


   if __name__ == "__main__":
       asyncio.run(main())

Start up one or more of the workers by running ``python worker.py``. This will
start a worker that listens on the "default" queue and runs up to 10 jobs
concurrently with a few :doc:`guide/plugins` that help keep the queue running
smoothly. The ``Web()`` plugin also enables an API and dashboard that you can
access at ``http://localhost:8000`` by default.

Next, we need to create a job that we want to run. Let's create a file called
``job.py`` and add a job that does nothing:

.. code-block:: python
  :caption: job.py

   import asyncio
   from chancy import Job
   from worker import chancy


   def my_dummy_task(name: str):
       print(f"Hello, {name}!")


   async def main():
       async with chancy:
           await chancy.push(
               "default", Job(func="job.my_dummy_task", kwargs={"name": "world"})
           )


   if __name__ == "__main__":
       asyncio.run(main())


When we run this file with ``python job.py``, we'll see the job get picked up
by the worker and run in the background. That's it, all done! You can use any
function your code can import as a Job.


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
   advanced/index
   chancy


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
