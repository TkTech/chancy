Changelog
=========

0.19.0
------
- All executors now support a default concurrency, which typically mimics the
  default of the underlying pool.
- The ProcessExecutor, ThreadExecutor, and SubInterpreterExecutor now
  support running async jobs. This works by starting new event loops in the
  child process or thread, and running the job in that event loop.
- A `default` queue backed by a ProcessExecutor is now created whenever a
  worker starts up if it is missing.
- The internal event hub now supports wildcard handlers, useful for tracing and
  debugging.
- Workers now attempt to drain their queues before shutting down, to ensure
  that all jobs are processed. If the timeout is reached, the worker will
  forcefully shut down and raise a `worker.shutdown_timeout` event.
- Queues can be fully reconfigured on the fly - a worker will now stop and
  restart its queues if the configuration changes, including changing the type
  of executor.