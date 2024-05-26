Limiting Resources
==================

It's possible to limit the resources that are used by a Job process.
You can do this by using the `limits` key in the Job configuration.

It's important to keep in mind that these limitations only work
when the job is well-behaved. It's possible for a Job to catch
signals and exceptions to ignore them.

Limiting Memory
---------------

Since Chancy uses a process pool to run jobs, we can use the host
mechanisms to limit the memory usage of the process pool. This is
done by setting the `limits` key in the Job configuration and
specifying the memory limit in bytes.

.. code-block:: python

  from chancy.app import Job, Limit

  job = Job(
      func=my_dummy_task,
      max_attempts=3,
      limits=[
          Limit(Limit.Type.MEMORY, (1024 ** 3) * 80),
      ],
  )

If the job exceeds the specific memory usage, a MemoryError will be raised.

.. note::

  This is only available on UNIX-like platforms.

Limiting Runtime
----------------

You can also limit the runtime of a job. If the job exceeds the specified
time, a TimeoutError will be raised.

.. code-block:: python

  from chancy.app import Job, Limit

  job = Job(
      func=my_dummy_task,
      max_attempts=3,
      limits=[
          Limit(Limit.Type.TIME, 60),
      ],
  )

Jobs that use timeouts will start a second thread when they start running
which will raise a TimeoutError in the main thread if the job exceeds the
specified time.
