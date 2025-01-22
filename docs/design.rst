Design
======

.. warning::

   This section is a work in progress.

Chancy is designed to fit the vast majority of use cases, and to do so
in a way that requires just the availability of a Postgres database.

The Application
---------------

A Chancy application is made when you define a :class:`~chancy.app.Chancy`
instance. This object is responsible for managing the database connection,
and exposes common functionality like pushing jobs to the queue or
running migrations.

.. code-block:: python

  from chancy import Chancy

  async with Chancy("postgresql://localhost/postgres") as chancy:
      pass


The Worker
----------

Now when we want to run a job, we do so using a long-running
:class:`~chancy.worker.Worker` process. This worker process is responsible
for pulling jobs from the queue, co-ordinating the execution of the job, and
updating the job status in the database as it progresses.

.. code-block:: python

  from chancy import Chancy, Worker

  async with Chancy("postgresql://localhost/postgres") as chancy:
      async with Worker(chancy) as worker:
          await worker.wait_for_shutdown()

The worker will use Postgres' ``SELECT...FOR UPDATE SKIP LOCKED`` to guarantee
that only one worker is running a given job at a time. If the worker happens
to crash while running a job, the :class:`~chancy.plugins.recovery.Recovery`
plugin will periodically restore them.

Each worker also listens for realtime events using Postgres' ``LISTEN/NOTIFY``
mechanism. This allows the worker to be notified of new jobs being pushed to
the queue (among other things) nearly instantly. In the case that this fails,
the worker will also poll the queue periodically.

Leadership
----------

Instead of having a hardcoded "coordinator" process or requiring separate
setup for periodic tasks like Celery's "beat" process, all of the workers
in a Chancy application will periodically attempt to become the "leader".
Certain plugins like :doc:`cron <chancy.plugins.cron>` and
:doc:`workflows <chancy.plugins.workflow>` will only run on the worker that
currently holds leadership to prevent race conditions.

The default leadership process is handled by the
:class:`~chancy.plugins.leadership.Leadership` plugin, and can easily be
replaced with a custom implementation.

Multi-mode concurrency
----------------------

Each queue can use its own concurrency model, and multiple queues can mix and
match models in the same worker. By default, queues use the
:class:`~chancy.executors.process.ProcessExecutor` which is similar to the
default in most other task queues. This means you can mix asyncio-based
jobs which are crawling external APIs with a CPU-bound task aggregating
the results and it'll just work.


Extendable
----------

Almost all functionality beyond queue management and job fetching in Chancy is
implemented as a plugin. This allows us to easily add new features without
breaking backwards compatibility, and to make it easy to swap out the
underlying implementation as your needs change. Workflows, cron jobs,
job recovery, job pruning, and more are all implemented as swappable plugins.

This is especially useful for busy applications where you might need to tweak
queries or behaviors to optimize for your specific use case.


Reliable By Default
-------------------

In Chancy, jobs are guaranteed to be run *at least once*. In the case of a
worker crash, networking issues, or other failure, the job can be recovered
with the :class:`~chancy.plugins.recovery.Recovery` plugin. This ensures
that jobs are never lost, but care must be taken to ensure that jobs are
idempotent. That is, the job should be able to be run multiple times without
causing any side effects.

This contrasts to Celery, which may lose jobs in the case of a worker crash
if the `acks_late` setting is left on its default of disabled.