"""
This package contains the various built-in plugins available for Chancy.
Plugins are used to extend the functionality of Chancy, while allowing for
significant customization.

Some Chancy plugins are enabled by default. You can pass your own into the
list of plugins to customize them or set ``no_default_plugins`` to ``True`` to
disable them completely:

.. code-block:: python

    async with Chancy(..., no_default_plugins=True, plugins=[]):
        ...

Default Plugins
---------------

- :class:`chancy.plugins.workflow.WorkflowPlugin`: This plugin provides a
  workflow system for managing complex job dependencies.
- :class:`chancy.plugins.metrics.Metrics`: This plugin collects metrics about
  the performance of the Chancy cluster and makes them available to the API,
  dashboard, and database.
- :class:`chancy.plugins.leadership.Leadership`: This plugin is responsible for
  managing leadership elections between multiple workers. Most other plugins
  require this plugin to be enabled.
- :class:`chancy.plugins.pruner.Pruner`: This plugin is responsible for pruning
  old jobs from the database when they are no longer needed.
- :class:`chancy.plugins.recovery.Recovery`: This plugin is responsible for
  recovering jobs that were interrupted during execution, such as when a
  worker crashes or is restarted.

Optional Plugins
----------------

Chancy also comes with some additional plugins that can be enabled as needed,
and typically require additional dependencies:

- :class:`chancy.plugins.api.Api`: This plugin provides an API and dashboard
  for viewing the state of the Chancy cluster.
- :class:`chancy.plugins.cron.Cron`: This plugin is responsible for running
  jobs to at recurring schedules, such as "once an hour" or "every Monday".
- :class:`chancy.plugins.retry.RetryPlugin`: This plugin provides a slightly
  more advanced job retry system with jitter, backoff, and other features.
- :class:`chancy.plugins.reprioritize.Reprioritize`: This plugin increases
  the priority of jobs based on how long they've been in the queue.
- :class:`chancy.plugins.sentry.SentryPlugin`: This plugin sends job exceptions
  to Sentry, along with metadata about the job and worker.
- :class:`chancy.plugins.trigger.Trigger`: This plugin allows you to register
  triggers that run jobs when certain database operations occur, such as
  INSERT, UPDATE, or DELETE operations on a table.
"""
