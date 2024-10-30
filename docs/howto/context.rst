Get The Job Context
===================

Sometimes you need access to details about the job that is currently running. For
example you might want to know the job's ID to log it, or the number of times the
job has been retried. Getting the job context is easy:

.. code-block:: python

    from chancy import QueuedJob

    def my_job(*, job: QueuedJob):
        print(f"Job ID: {job.id}")
        print(f"Job attempts: {job.attempts}")


That's it! When Chancy runs a job, it checks to see if the type signature for that
job function includes a :class:`chancy.job.QueuedJob` and assumes you want the
context for the job.

.. tip::

  The name of the argument doesn't matter, as long as the type is correct. For
  example, you could name the argument ``job_context`` instead of ``job``.

The job context is immutable, *except* for the ``meta`` attribute, which you can
use to store arbitrary data about the job:

.. code-block:: python

    from chancy import QueuedJob

    def my_job(*, job: QueuedJob):
        # This will raise an exception because the job context is
        # generally immutable.
        job.id = "new_id"
        # This will work because the meta attribute is mutable.
        job.meta["attempts"] = job.meta.get("attempts", 0) + 1


