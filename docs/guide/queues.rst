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
            await Worker(chancy).start()

    if __name__ == "__main__":
        asyncio.run(main())


You don't need to call ``declare`` all the time, as the queues and their
settings will persist in the database. You only need to declare a queue
if you want to change its settings, or if you want to create a new queue.

Worker Tags
-----------

You can control which workers run which queues by using worker tags. A worker
tag is just a string (conventionally in the format ``key=value``) that you can
assign to a worker. You can then declare a queue with a list of tags, and only
workers with one of those matching tags will run that queue.

Every worker automatically gets some tags assigned to it:

.. list-table::
   :header-rows: 1

   * - Tag
     - Description
   * - ``hostname``
     - The hostname of the machine the worker is running on.
   * - ``worker_id``
     - A unique identifier for the worker.
   * - ``python``
     - The version of Python the worker is running.
   * - ``arch``
     - The architecture of the machine the worker is running on.
   * - ``os``
     - The operating system the worker is running on.
   * - ``*``
     - A wildcard tag that all workers get.

You can specify extra tags when you create a worker, which allows you to load
them from environment variables, or other sources:

.. code-block:: python
   :caption: worker.py

    import asyncio
    from chancy import Chancy, Worker, Queue

    chancy = Chancy(dsn="postgresql://localhost/postgres")

    async def main():
        async with chancy:
            await chancy.migrate()
            await Worker(chancy, tags={"has=gpu", "has=large-disk"}).start()

    if __name__ == "__main__":
        asyncio.run(main())


Then you could change the ``default`` queue in the earlier example to only run
on workers with the ``has=gpu`` tag:

.. code-block:: python
   :caption: worker.py

    import asyncio
    from chancy import Chancy, Worker, Queue

    chancy = Chancy(dsn="postgresql://localhost/postgres")

    async def main():
        async with chancy:
            await chancy.declare(
                Queue(name="default", tags={"has=gpu"}, concurrency=10),
                # Replaces the settings in the database with the new settings.
                upsert=True
            )

    if __name__ == "__main__":
        asyncio.run(main())

There's no need to restart the worker when you change the tags of a queue, as
the worker will periodically check the database for changes and update itself
accordingly.

Tags are regexes, so you can get creative, like using a tag to run a queue only
on workers with a specific version of Python:

.. code-block:: python

    Queue(name="default", tags={r"python=3\.11\.[0-9]+"}, concurrency=10),
