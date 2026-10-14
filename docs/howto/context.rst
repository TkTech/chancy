Get The Job Context
===================

Sometimes you need access to details about the job that is currently running. For
example you might want to know the job's ID to log it, or the number of times the
job has been retried. Getting the job context is easy:

.. code-block:: python

    from chancy import QueuedJob, job

    @job()
    def my_job(*, context: QueuedJob):
        print(f"Job ID: {context.id}")
        print(f"Job attempts: {context.attempts}")


That's it! When Chancy runs a job, it checks to see if the type signature for that
job function includes a :class:`chancy.job.QueuedJob` and assumes you want the
context for the job.

.. tip::

  The name of the argument doesn't matter, as long as the type is correct. For
  example, you could name the argument ``job_context`` instead of ``context``.

The job context is immutable, *except* for the ``meta`` attribute, which you can
use to store arbitrary data about the job:

.. code-block:: python

    from chancy import QueuedJob, job

    @job()
    def my_job(*, context: QueuedJob):
        # This will raise an exception because the job context is
        # generally immutable.
        context.id = "new_id"
        # This will work because the meta attribute is mutable.
        context.meta["attempts"] = context.meta.get("attempts", 0) + 1


Cooperative Time Limits
-----------------------

Time limits in the threaded and sub-interpreter executors are cooperative.
Both synchronous and coroutine jobs must accept the job context and call
:meth:`~chancy.job.QueuedJob.checkpoint` at points where execution can be
safely interrupted.

.. code-block:: python

    from chancy import Limit, QueuedJob, job

    @job(limits=[Limit(Limit.Type.TIME, 60)])
    def process_items(*, context: QueuedJob):
        for item in get_items():
            process(item)
            context.checkpoint()
