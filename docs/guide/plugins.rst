Plugins
=======

Chancy implements some core functionality as plugins, providing flexibility for
users to customize worker behavior. Here are some key plugins that most users
will want to enable or implement:

Leadership
----------

:class:`chancy.plugins.leadership.Leadership`

Elects one worker to be the leader, responsible for running tasks that should
only be executed once across the cluster. Most other plugins require Leadership
to function correctly.

Pruner
------

:class:`chancy.plugins.pruner.Pruner`

Removes old jobs from the database to prevent indefinite growth.

Recovery
--------

:class:`chancy.plugins.recovery.Recovery`

Recovers jobs that were running when a worker crashed or disappeared
unexpectedly.

Cron
----

:class:`chancy.plugins.cron.Cron`

Provides a way to define jobs that run on a schedule using cron syntax.

Workflows
---------

:class:`chancy.plugins.workflow.Workflow`

Allows you to define complex job workflows with dependencies between jobs.

Web
---

:class:`chancy.plugins.web.Web`

Offers a web interface and API for managing jobs and queues.

Enabling Plugins
----------------

To enable plugins, pass them to the :class:`~chancy.app.Chancy` constructor:

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

Configuring Plugins
-------------------

Most Chancy plugins offer configuration options. For example, to customize the
Pruner's behavior:

.. code-block:: python
   :caption: worker.py

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

This configuration sets the Pruner to remove jobs older than 1 hour and run
every 10 seconds.

Creating Custom Plugins
-----------------------

You can create custom plugins by subclassing :class:`chancy.plugin.Plugin`.
Here's a simple example:

.. code-block:: python

    from chancy.plugin import Plugin, PluginScope
    from chancy.app import Chancy
    from chancy.worker import Worker
    from psycopg import sql

    class RunningJobsCounter(Plugin):
        def __init__(self, *, poll_interval: int = 60):
            super().__init__()
            self.poll_interval = poll_interval

        @classmethod
        def get_scope(cls) -> PluginScope:
            return PluginScope.WORKER

        async def run(self, worker: Worker, chancy: Chancy):
            while await self.sleep(self.poll_interval):
                await self.wait_for_leader(worker)
                async with chancy.pool.connection() as conn:
                    async with conn.cursor() as cursor:
                        query = sql.SQL(
                            """
                            SELECT COUNT(*) FROM {jobs}
                            WHERE state = 'running';
                            """
                        ).format(jobs=sql.Identifier(f"{chancy.prefix}jobs"))
                        await cursor.execute(query)
                        count = await cursor.fetchone()
                        chancy.log.info(f"Current number of running jobs: {count[0]}")

    # Use your custom plugin
    chancy = Chancy(
        dsn="postgresql://localhost/postgres",
        plugins=[RunningJobsCounter(poll_interval=30)],
    )

This custom plugin will print the number of running jobs every 30 seconds,
but only on the leader worker.


Next Steps
----------
- Learn about :doc:`jobs` to understand the core unit of work in Chancy
- Explore :doc:`queues` to see how jobs are organized and distributed
- Dive into :doc:`workers` to understand how jobs are processed
- Check out :doc:`executors` to learn about different ways of running jobs