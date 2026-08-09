"""
Executors
=========

This package contains the built-in executors for Chancy. Executors are
responsible for managing the execution of jobs after they've been retrieved
from a queue. A single worker can have multiple queues, each using its own
executor, allowing you to run jobs in the way that best suits their needs.

Capabilities
------------

Not all executors have the same capabilities, typically due to limits in how
threads are implemented in Python. The following table summarizes the
capabilities of each executor:

.. list-table:: Executor Capabilities
   :header-rows: 1
   :widths: 20 10 10 15 20 15

   * - Executor
     - Sync Jobs
     - Async Jobs
     - Cancellation [#f1]_
     - Time Limits
     - Memory Limits
   * - ProcessExecutor
     - ✓
     - ✓
     - Platform [#f3]_
     - Automatic [#f3]_
     - Platform [#f3]_
   * - AsyncExecutor
     - ✗
     - ✓
     - ✓
     - Automatic
     - ✗
   * - ThreadedExecutor
     - ✓
     - ✓
     - ✗
     - Cooperative [#f2]_
     - ✗
   * - SubInterpreter
     - ✓
     - ✓
     - ✗
     - Cooperative [#f2]_
     - ✗

.. [#f1] Cancellation is always possible before a job is started. Cancellation
         here refers to the ability to stop a job that is actively running.
.. [#f2] Jobs must accept a :class:`chancy.job.QueuedJob` context and call
         :meth:`~chancy.job.QueuedJob.checkpoint` periodically.
.. [#f3] Process capabilities depend on operating-system signal and resource
         support. Use :meth:`~chancy.executors.base.Executor.supports` to
         inspect them at runtime.

Capabilities can also be inspected programmatically:

.. code-block:: python

    from chancy.executors.base import Executor
    from chancy.executors.thread import ThreadedExecutor

    ThreadedExecutor.supports(
        Executor.Capability.COOPERATIVE_TIME_LIMITS
    )

ProcessExecutor (Default)
~~~~~~~~~~~~~~~~~~~~~~~~~
:class:`chancy.executors.process.ProcessExecutor`
Optimized for CPU-bound jobs, uses separate processes for true parallelism.

AsyncExecutor
~~~~~~~~~~~~~
:class:`chancy.executors.asyncex.AsyncExecutor`
Optimized for IO-bound jobs like API calls and database operations. Uses
asyncio for efficient concurrent execution. Thousands of jobs can be
executed concurrently with limited resources.

ThreadedExecutor
~~~~~~~~~~~~~~~~
:class:`chancy.executors.thread.ThreadedExecutor`
Suitable for IO-bound jobs that can't use asyncio. Uses threads for concurrent
execution.

SubInterpreterExecutor
~~~~~~~~~~~~~~~~~~~~~~
:class:`chancy.executors.sub.SubInterpreterExecutor`
Experimental executor using Python sub-interpreters. Provides GIL avoidance
with lower overhead than processes.


Custom Executors
~~~~~~~~~~~~~~~~
You can implement your own executor by subclassing the
:class:`chancy.executors.base.Executor` class and implementing the
:meth:`~chancy.executors.base.Executor.push` method. Custom executors should
declare their supported :class:`~chancy.executors.base.Executor.Capability`
values in ``capabilities``. The shared execution preparation then rejects
unsupported function types and resource limits consistently.
"""
