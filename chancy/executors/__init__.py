"""
Executors
=========

This package contains the built-in executors for Chancy. Executors are
responsible for managing the execution of jobs after they've been retrieved
from a queue.

Chancy comes with two built-in executors:

- :class:`chancy.executors.process.ProcessExecutor`: This executor will execute jobs
    in a separate process. This is the default executor.
- :class:`chancy.executors.asyncex.AsyncExecutor`: This executor will execute asyncio
    jobs in the same process as the worker.

You can implement your own executor by subclassing the
:class:`chancy.executors.base.Executor` class and implementing the
:meth:`~chancy.executors.base.Executor.push` method.
"""
