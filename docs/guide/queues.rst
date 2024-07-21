Queues
======

A Queue in Chancy provides a way to push, pull, and manage :doc:`jobs <jobs>`. The
default, and only officially supported queue, is the postgres-backed
:class:`~chancy.queue.Queue`. You can have as many queues as you want,
each with their own executor, concurrency, polling intervals,
and other settings.

Key Concepts
------------

- **Global**: All workers can see all queues that have been declared.
- **Dynamic Configuration**: Queue settings can be updated at runtime without restarting workers.
- **Worker Assignment**: Queues can be assigned to specific workers using tags.
- **Executors**: Each queue has its own executor for job processing.

Creating and Declaring Queues
-----------------------------

Here's a basic example of creating and declaring a queue:

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

You don't need to call ``declare`` every time, as queue settings persist in
the database. You only need to declare a queue to change its settings or
to create a new one. If you run this file again without the ``declare``
call, it'll still see the ``default`` queue since it was saved to the
database.

Worker Tags
-----------

Worker tags control which workers run which queues. A tag is a string
(typically in the format ``key=value``) assigned to a worker. Declare
a queue with a list of tags, and only workers with matching tags will
run that queue.

Every worker automatically gets some tags, like ``hostname``, ``worker_id``,
``python``, ``arch``, ``os``, and ``*`` (wildcard). You can also add custom
tags when creating a worker.

.. code-block:: python

    await Worker(chancy, tags={"has=gpu", "has=large-disk"}).start()

You could then assign a queue to only run on workers with the ``has=gpu`` tag:

.. code-block:: python

    await chancy.declare(
        Queue(name="default", tags={"has=gpu"}, concurrency=10),
        upsert=True  # Replaces existing settings
    )

Tags are regexes, allowing for flexible matching:

.. code-block:: python

    Queue(name="default", tags={r"python=3\.11\.[0-9]+"}, concurrency=10)

Next Steps
----------
- Learn about :doc:`jobs` to understand the core unit of work in Chancy
- Dive into :doc:`workers` to understand how jobs are processed
- Check out :doc:`executors` to learn about different ways of running jobs
- Discover :doc:`plugins` to extend Chancy's functionality