Compare to Celery
=================

`Celery`_ is the defacto standard for background task processing in Python,
most commonly using RabbitMQ or Redis as a broker. It is a mature project with a
large user base and a lot of documentation available.

Celery is a good choice for many use cases, but it is not without its drawbacks,
and Chancy was written specifically to address some of those drawbacks.

Future Scheduling
-----------------

When Celery schedules a task to run in the future using `eta` or `countdown`,
it's pulled into the memory of a currently running worker and the worker's own
scheduler is responsible for executing the task at the right time.

This is fine if you only have a couple of tasks but can cause severe memory
issues if you have a lot of tasks scheduled to run in the future. Too many
jobs (~65k) also disables QoS for the worker, which can cause other issues.

Chancy doesn't depend on the worker to schedule jobs in the future. It simply
stores the timestamp it should execute at when it saves the job into the
database and every worker is given a chance to pick up the job when it's time
to run.

Rate Limiting
-------------

Celery has only very basic rate limiting support, which makes it almost
impossible to properly do basic tasks like fetching from an API with a rate
limit.

Chancy has built-in support for globally rate limiting a queue.


Asyncio-first (but everything else too)
---------------------------------------

Celery predates the existence of asyncio, and to this day it doesn't have
any support for running asyncio workers nor for executing asyncio tasks.
When your tasks are I/O bound, this can be a significant drawback, such as when
calling external APIs.

Chancy is built on top of asyncio and can offer drastically improved resource
utilization for I/O bound tasks using its
:class:`~chancy.executors.asyncex.AsyncExecutor`.

Introspection
-------------

Things go wrong - it's inevitable. When they do, you need to be able to
introspect the state of your workers and queues to figure out what's going on.
Unfortunately, this is often extremely difficult with Celery.

Since Chancy is just using your existing Postgres database, you can simply
query the database with plain SQL to figure out what's going on. Want to see
how many of each type of job is in the queue? Just run a query.

.. code-block:: sql

    SELECT func, COUNT(*) FROM chancy_jobs GROUP BY func;


Chancy also comes with a built-in :class:`~chancy.plugins.api.Api` plugin that
provides a dashboard for monitoring the state of your workers, queues,
workflows, and cron jobs. No extra setup or services required.


Mixed-mode Workers
------------------

Celery has a number of different worker pool implementations, like processes,
gevent, eventlet and threads. However, a single worker process can only *use*
one of these pools at a time.

In Chancy, every queue can specify its own pool (which we call Executors),
allowing a single worker to mix-and-match pools to optimize for different
types of jobs without having to run multiple workers. One worker can have
thousands of parallel asyncio tasks pulling reports from an external API
while using another core to generate PDFs.


.. _Celery: https://docs.celeryproject.org/en/stable/