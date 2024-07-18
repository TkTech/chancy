Plugins
=======

Some core Chancy functionality is implemented as plugins to provide
flexibility for users to customize their worker behavior. There are
a few plugins most users will always want to enable, or implement
themselves:

- :class:`chancy.plugins.leadership.Leadership` - Elects one
  worker to be the leader, which is responsible for running
  certain tasks that should only be run one at a time.
- :class:`chancy.plugins.pruner.Pruner` - Prunes old jobs from the
  database to keep it from growing indefinitely.
- :class:`chancy.plugins.recovery.Recovery` - Recovers jobs that
  were running when a worker crashed or otherwise disappeared.
- :class:`chancy.plugins.cron.Cron` - Provides a way to define
  jobs that run on a schedule using cron syntax.
- :class:`chancy.plugins.web.Web` - Provides a web interface and
  API for managing jobs and queues.

To enable these plugins, simply pass them to the
:class:`~chancy.app.Chancy` constructor:

.. code-block:: python
   :caption: worker.py

    import asyncio
    from chancy import Chancy, Worker, Queue
    from chancy.plugins.pruner import Pruner
    from chancy.plugins.recovery import Recovery
    from chancy.plugins.leadership import Leadership

    async def main():
        async with Chancy(
            dsn="postgresql://localhost/postgres",
            plugins=[
                Pruner(),
                Recovery(),
                Leadership(),
            ],
        ) as chancy:
            await chancy.migrate()
            await Worker(chancy).start()


Most Chancy plugins will provide configuration options, such as polling
frequency. For example, if we wanted the pruner to run every 10 seconds,
and only delete jobs that are older than 1 hour, we could pass a
:class:`Pruner` instance with a custom `poll_interval` and `rule`:

.. code-block:: python
   :caption: worker.py

    import asyncio
    from chancy import Chancy, Worker, Queue
    from chancy.plugins.rule import Age
    from chancy.plugins.pruner import Pruner

    async def main():
        async with Chancy(
            dsn="postgresql://localhost/postgres",
            plugins=[
                Queue(name="default", concurrency=10),
                Pruner(Age() > 60 * 60, poll_interval=10),
            ],
        ) as chancy:
           ...