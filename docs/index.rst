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
server that you're probably already using anyway. Its features are added
as needed by the author's projects, so it may not be suitable for all use cases.
You shouldn't need RabbitMQ/Redis + celery to send a few emails or run a few
background tasks.

Features
--------

- Jobs support priorities, retries, timeouts, memory limits, future scheduling,
  and more.
- Completed jobs stick around in the database for easy debugging and job
  tracking, with configurable retention policies. Inspect your jobs with plain
  old SQL.
- Dependency-free except for psycopg3.
- Multi-tenant support with prefixes for all database tables.
- Optional transactional job queueing - only queue a job if your transaction
  commits successfully.
- asyncio-based worker, can be run in-process with your web server on small
  projects or as a standalone worker on larger projects.

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


   if __name__ == "__main__":
       asyncio.run(main())

Start up one or more of the workers by running ``python worker.py``. This will
start a worker that listens on the "default" queue and runs up to 10 jobs
concurrently with a few :doc:`plugins` that help keep the queue running
smoothly.

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



.. toctree::
   :maxdepth: 4
   :caption: Contents:
   :hidden:

   jobs
   plugins
   executors
   advanced/index
   chancy


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
