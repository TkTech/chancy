Changelog
=========

0.24.0
------

🚨 This release requires that you run migrations, as it adds a `deadline` column
to the jobs table and updates the unique_key index to include the expired
job state.

- ✨ Added support for job deadlines, after which the job will refuse to execute
  and be marked as expired. Plugins can now implement `on_job_expired` to handle
  job expiration with custom logic.
- ✨ `chancy worker web` CLI now accepts `--allow-credentials` to allow
  credentials to be sent with CORS requests. This is useful for local
  development of the dashboard itself, but should not be used in production.
- ✨ Various UX improvements to the dashboard, including job progress timelines.
- 📝 Documentation fix to the cron plugin example (Thanks @PaulM5406)

0.23.0
------

- Support for django-style database settings as the first argument to the
  `Chancy` constructor to ease integration with Django projects.
- Chancy's dashboard and API now require a login by default. If the CLI is
  used to start the dashboard without an API configured, a random temporary
  password will be generated. Authentication backends are pluggable, and Chancy
  comes with an optional DjangoAuthBackend.
- The dashboard's live event stream is temporarily removed due to Websocket
  routes not properly supporting authentication.
- Many documentation tweaks.

0.22.0
------

🚨 This release requires that you run migrations, as it converts all JSON
columns to JSONB. Call `chancy.migrate()` or use the CLI:

```bash
chancy --app <your app> misc migrate
```


- Chancy now starts some default plugins with reasonable defaults if they are
  not configured. This includes the `Metrics`, `Leadership`, `Pruner`,
  `Recovery` and `WorkflowPlugin`.
- Queue's now support a `resume_at` option, which allows them to automatically
  go active after the specified time. This can be used to implement circuit
  breakers.
- Added the convenience functions `Chancy.pause_queue()` and
  `Chancy.resume_queue()`. Calling this functions will emit `queue.paused` and
  `queue.resumed` events.
- Plugins *must* now provide a `get_identifier()` method, which should return a
  unique identifier for the plugin.
- Plugins *may* now implement `get_depdenencies()` to return a list of other
  plugins that they depend on.
- The default queue polling interval for new queues has been increased from
  1 to 5 seconds.
- All JSON columns have been converted to JSONB, which is more efficient and
  allows for indexing, and is supported by Django's JSONField type.

**Metrics Plugin**

- Internal implementation of the Metrics plugin has been significantly
  simplified.
- Metrics plugin now tracks job completion status on a per-queue basis as well
  as globally, which allows for the implementation of global, per-queue circuit
  breaker logic.

**Django**

- Chancy now comes with a Django app that gives you ORM and Admin access to
  Chancy's data. To install it, add `chancy.contrib.django` to your
  `INSTALLED_APPS`.
- The ProcessExecutor now has a django shim by default, simplifying setup.


0.21.0
------

- Worker's page on the dashboard now shows worker tags and a badge if the
  worker is the cluster's current leader.
- The `workflow.upserted` event has been split into `workflow.created` and
  `workflow.updated` events. Added a new `workflow.step_completed` event.
- `WorkflowPlugin.push` now only pushes the workflow if it has actually changed.
- `workflow.step_completed` is now used to nearly instantly progress a workflow
   to the next step, instead of waiting for the next polling interval.
- The `WorkflowPlugin` now exposes `_ex` versions of most functions which touch
  the database, allowing you to pass in your own cursor to ensure that a
  workflow is created in the same transaction as other operations.
- The `WorkflowPlugin` now locks the workflow row when updating it and performs
  all updates in a single transaction.
- All `_ex` functions which take a cursor now expect a cursor using a dict_row
  row factory.
- Improved metrics display by adding automatic formatting for large numbers
  (K, M, G) on chart Y-axes.
- Added `get_schedules()` helper method to the Cron plugin to retrieve
  scheduled jobs.
- Added a live stream of the internal events queue to the dashboard.
- Table size metrics are now collected for all tables known to plugins.

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