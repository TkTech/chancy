Plugins
=======

Some core Chancy functionality is implemented as plugins to provide
flexibility for users to customize their worker behavior. There are
a few plugins most users will always want to enable:

- :class:`chancy.plugins.pruner.Pruner` - Prunes old jobs from the
  database to keep it from growing indefinitely.
- :class:`chancy.plugins.recovery.Recovery` - Recovers jobs that
  were running when a worker crashed or otherwise disappeared.

To enable these plugins, simply pass them to the :class:`Worker`
constructor:

.. code-block:: python
   :caption: worker.py

    import asyncio
    from chancy.queue import Queue
    from chancy.app import Chancy
    from chancy.worker import Worker
    from chancy.plugins.pruner import Pruner
    from chancy.plugins.recovery import Recovery

    async def main():
        async with Chancy(
            dsn="postgresql://localhost/postgres",
            queues=[
                Queue(name="default", concurrency=10),
            ],
        ) as chancy:
            await chancy.migrate()
            await Worker(chancy, plugins=[Pruner(), Recovery()]).start()