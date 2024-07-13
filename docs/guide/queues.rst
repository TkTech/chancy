Queues
======

A :class:`~chancy.queue.QueuePlugin` provides a way to push, pull, and manage
:doc:`jobs <jobs>`. The default, and only officially supported queue, is the
postgres-backed :class:`~chancy.queues.pg.Queue`. You can have as many queues
as you want, each with their own executor, concurrency, polling intervals,
and other settings.

.. code-block:: python
   :caption: worker.py

    import asyncio
    from chancy import Chancy, Worker, Queue

    chancy = Chancy(
        dsn="postgresql://localhost/postgres",
        plugins=[
            Queue(name="default", concurrency=10),
            Queue(name="high-priority", concurrency=5, polling_interval=1),
            Queue(
                name="leaky-memory",
                concurrency=1,
                # This queue will restart the worker process after every single
                # job, which is helpful when you have a memory leak in external
                # libraries.
                executor=lambda queue: ProcessExecutor(
                    queue,
                    maximum_jobs_per_worker=1
                ),
            ),
        ],
    )

    async def main():
        async with chancy:
            await chancy.migrate()
            await Worker(chancy).start()

    if __name__ == "__main__":
        asyncio.run(main())


Since Queues are just plugins, you can implement your own queue by subclassing
:class:`~chancy.queue.QueuePlugin`. Using this you can implement queues from
other backends, such as Redis, RabbitMQ, or even in-memory queues while still
using Chancy's workers and executors. Or you may just want to change the
queries that the default queue uses to better fit your database usage.

References
----------

Anytime a job is pushed onto a queue, it's given a unique identifier which
can be used to reference the job later. Functions like
:meth:`~chancy.app.Chancy.push` will return a :class:`~chancy.job.Reference`
object that can be used to retrieve the job or wait until it's finished.


.. code-block:: python
   :caption: worker.py

    import asyncio
    from chancy import Chancy, Worker, Queue

    chancy = Chancy(
        dsn="postgresql://localhost/postgres",
        plugins=[
            Queue(name="default", concurrency=10),
        ],
    )

    async def main():
        async with chancy:
            reference = await chancy.push(my_task, "world")
            job = await reference.wait()
            print(f"Job finished with status {job.state}")

    if __name__ == "__main__":
        asyncio.run(main())