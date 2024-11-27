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
   :widths: 20 15 15 15

   * - Executor
     - Cancellation [#f1]_
     - Timeouts
     - Memory Limits
   * - ProcessExecutor
     - ✓
     - ✓
     - ✓
   * - AsyncExecutor
     - ✓
     - ✓
     - ✗
   * - ThreadedExecutor
     - ✗
     - ✓
     - ✗
   * - SubInterpreter
     - ✗
     - ✓
     - ✗

.. [#f1] Cancellation is always possible before a job is started. Cancellation
         here refers to the ability to stop a job that is actively running.

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
:meth:`~chancy.executors.base.Executor.push` method.
"""
