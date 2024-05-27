Job Recovery
============

When a job dies, the worker will automatically mark the job as failed or attempt
to restart it, depending on the job's configuration. But what happens if the
worker itself dies suddenly or becomes unreachable? In this case, the job will
continue to be in the `running` state.

The `Recovery()` plugin exists to handle this situation. It will periodically
check for jobs that are marked as running on workers that have stopped sending
their heartbeat. If it finds any, it will reset the job back to the "pending"
state so that it can be picked up by another worker.

Chancy doesn't enable any plugins by default, but this one is highly recommended
for production use.

.. warning::

    It's possible for jobs to run more than once. This is a trade-off between
    reliability and consistency. Remember, your jobs should always be
    idempotent - that is, they should be safe to run multiple times.

.. code-block:: python
  :caption: worker.py

   import asyncio
   from chancy.app import Chancy, Queue, Job, Limit
   from chancy.worker import Worker
   from chancy.plugins.recovery import Recovery

   async def main():
       async with Chancy(
           dsn="postgresql://username:password@localhost:8190/postgres",
           queues=[
               Queue(name="default", concurrency=10),
           ],
           plugins=[
               Recovery(interval=60, rescue_after=60 * 60)
            ],
           ]
       ) as app:
           await app.migrate()
           await Worker(app).start()

   if __name__ == "__main__":
       asyncio.run(main())
