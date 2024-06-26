Workers
=======

Chancy comes with an asyncio-based worker which executes available jobs in a
process pool.

To start a basic worker, configure your application and run the worker loop:

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
          await Worker(app).start()


  if __name__ == "__main__":
      asyncio.run(main())

.. code-block:: text
  :caption: Example output

  2024-05-26 00:16:07 • INFO • POLL • Started periodic polling for tasks.
  2024-05-26 00:16:07 • INFO • LEADER • Acquired/refreshed leader lock for worker 35252a75-33b5-4145-917f-1f67faff70ba.
  2024-05-26 00:16:07 • DEBUG • POLL • Found 0 job(s) in queue default.
  2024-05-26 00:16:07 • INFO • BUS • Now listening for cluster events.


This is enough to start a single worker server which will process jobs from the
"default" queue, running up to 10 jobs concurrently.


Connection Overhead
-------------------

Each worker will create up to 2 connections to the postgres database, one for
polling for work and the other to listen for realtime events. Out of the
entire cluster, 1 will be elected as the leader and may open additional
connections in order to run leader-specific plugins.

It's possible to use :py:attr:`chancy.app.Chancy.disable_events` to disable
realtime event listening, which will reduce the number of connections to 1.
However, this isn't recommended unless you're heavily constrained on the
number of connections you can open to the database.