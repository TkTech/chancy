Chancy
======

A postgres-backed task queue for Python.

This project is currently in the early stages of development. Use at your own
risk. It's guaranteed to be buggy and incomplete.

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

- Fully-featured Jobs, with priorities, retries, timeouts, memory limits, future
  scheduling, and more.
- Transactional job queueing. Jobs are only inserted into the database if the
  transaction they were created in is committed.
- Completed jobs stick around in the database for easy debugging and job
  tracking, with configurable retention policies.
- Dependency-free except for psycopg3.
- Multi-tenant support with prefixes for all database tables.
- Inspect your queue with plain old SQL.

Installation
------------

Chancy is available on PyPI. You can install it with pip:

.. code-block:: bash

   pip install chancy

Usage
-----

Using Chancy is fairly straightforward. Just like any work queue, you need to
have a worker that listens for jobs and a client that submits jobs to the queue.
First, we'll create a file called `worker.py`:

.. code-block:: python
  :caption: worker.py

   import asyncio
   from chancy.app import Chancy, Queue, Job, Limit
   from chancy.worker import Worker

   async def main():
       async with Chancy(
           dsn="postgresql://username:password@localhost:8190/postgres",
           queues=[
               Queue(name="default", concurrency=10),
           ],
       ) as app:
           await app.migrate()
           await Worker(app).start()

   if __name__ == "__main__":
       asyncio.run(main())


Start up one or more of the workers by running `python worker.py`. This will
start a worker that listens on the "default" queue and runs up to 10 jobs
concurrently.

Next, we need to create a job that we want to run. Let's create a file called
`job.py` and add a job that does nothing:

.. code-block:: python
  :caption: job.py

   import asyncio
   from chancy.app import Chancy, Queue, Job, Limit


   def my_long_running_job():
     pass


   async def main():
       async with Chancy(
           dsn="postgresql://username:password@localhost:8190/postgres",
           queues=[
               Queue(name="default", concurrency=10),
           ],
       ) as app:
           await app.submit(Job(func=my_long_running_job), "default")


   if __name__ == "__main__":
       asyncio.run(main())

When we run this file, we'll see the job get picked up by the worker and run
in the background. That's it, all done!

Of course, we might want to have the job run with a higher priority, or
schedule it to run in the future, or retry if it fails. To learn more about
these features, check out the :doc:`guide/index`.


.. toctree::
   :maxdepth: 4
   :caption: Contents:
   :hidden:

   guide/index
   plugins/index
   chancy


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
