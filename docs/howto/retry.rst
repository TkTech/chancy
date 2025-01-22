Use Advanced Retries
====================

When you create a Job, you can specify a ``max_attempts`` argument to control how
many times the job will be retried when an exception occurs:

.. code-block:: python

    from chancy import job

    @job(max_attempts=3)
    def my_job():
      raise ValueError("This job should fail.")

This is very simplistic, and sometimes you need more control over how retries are
handled in your application. For example, you might want to retry a job only if a
specific exception is raised, or ensure that a random jitter is applied to the
delay between retries to prevent the thundering herd problem.

Chancy comes with a :class:`~chancy.plugins.retry.RetryPlugin` plugin that supports
backoff, jitter, exponential backoff, and more:

.. code-block:: python

    from chancy import job
    from chancy.plugins.retry import RetryPlugin

    @job()
    def job_that_fails():
        raise ValueError("This job should fail.")

    async with Chancy(..., plugins=[RetryPlugin()]) as chancy:
        await chancy.declare(Queue("default"))
        await chancy.push(
            job_that_fails.job.with_max_attempts(3).with_meta({
                "retry_settings": {
                    "backoff": 2,
                    "backoff_factor": 3,
                    "backoff_limit": 300,
                    "backoff_jitter": [1, 5],
                }
            })
        )

The ``RetryPlugin`` is very simple, being about 60 lines. You can easily use it as
the basis for your own complex retry strategies.