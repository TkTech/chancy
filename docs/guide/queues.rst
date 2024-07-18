Queues
======

A Queue provides a way to push, pull, and manage :doc:`jobs <jobs>`. The
default, and only officially supported queue, is the postgres-backed
:class:`~chancy.queue.Queue`. You can have as many queues as you want,
each with their own executor, concurrency, polling intervals,
and other settings.

Queues are global, meaning that all workers can see all queues that
have been declared somewhere. By default, a newly declared queue will
run on all workers, but you can control which workers run which queues
by using worker tags.

Each queue will periodically check the database to see if its settings
have changed, and update itself accordingly. This means you can change
the concurrency, polling interval, or other settings of a queue at runtime
without restarting the worker, or even move queues between workers.


.. code-block:: python
   :caption: worker.py

    import asyncio
    from chancy import Chancy, Worker, Queue

    chancy = Chancy(dsn="postgresql://localhost/postgres")

    async def main():
        async with chancy:
            await chancy.migrate()

            await chancy.declare(Queue(name="default", concurrency=10))
            await chancy.declare(Queue(name="high-priority", concurrency=5, polling_interval=1))
            # This queue will only run on workers with the "has=gpu" tag.
            await chancy.declare(Queue(name="video", tags=["has=gpu"], concurrency=1))
            # This queue will _replace_ any settings in the database
            await chancy.declare(
                Queue(
                    name="low-priority",
                    concurrency=5,
                    polling_interval=5
                ),
                upsert=True
            )

            await Worker(chancy).start()

    if __name__ == "__main__":
        asyncio.run(main())


You don't need to call ``declare`` all the time, as the queues and their
settings will persist in the database. You only need to declare a queue
if you want to change its settings, or if you want to create a new queue.
