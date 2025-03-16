Changelog
=========

0.21.0
------

- Worker's page on the dashboard now shows worker tags and a badge if the
  worker is the cluster's current leader.

0.20.1
------

- Workers that had no metrics to _push_ skipped _pulling_ metrics from other
  workers, which mainly affected a standalone API worker (since it would never
  have metrics)

0.20.0
------
- Built-in support for metrics via the `Metrics()` such as job, queue and worker
  throughput, job completion times, and more. Available via the API and the 
  dashboard.
- Added the ability to filter by job name in the dashboard.
- Added a `--plugin` option to the `chancy misc migrate` command to migrate only
  a specific plugin.
- Fix the `JobRule.Job` preset, which still referenced the old `payload->>func`
  field.

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
- Limited Windows support - suitable for development, but not recommended for
  production use.