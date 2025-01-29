"""
Plugins
=======

This package contains the various built-in plugins available for Chancy.
Plugins are used to extend the functionality of Chancy, while allowing for
significant customization.

Chancy comes with several built-in plugins:

- :class:`chancy.plugins.leadership.Leadership`: This plugin is responsible for
    managing leadership elections between multiple workers. Most other plugins
    require this plugin to be enabled.
- :class:`chancy.plugins.pruner.Pruner`: This plugin is responsible for pruning
    old jobs from the database when they are no longer needed.
- :class:`chancy.plugins.recovery.Recovery`: This plugin is responsible for
    recovering jobs that were interrupted during execution, such as when a
    worker crashes or is restarted.
- :class:`chancy.plugins.cron.Cron`: This plugin is responsible for scheduling
    jobs to run at recurring schedules.
- :class:`chancy.plugins.api.Api`: This plugin provides an API and dashboard
    for viewing the state of the Chancy cluster.
- :class:`chancy.plugins.workflow.WorkflowPlugin`: This plugin provides a
    workflow system for managing complex job dependencies.
- :class:`chancy.plugins.retry.RetryPlugin`: This plugin provides a slightly
    more advanced job retry system with jitter, backoff, and other features.
- :class:`chancy.plugins.reprioritize.Reprioritize`: This plugin increases
    the priority of jobs based on how long they've been in the queue.
- :class:`chancy.plugins.sentry.SentryPlugin`: This plugin sends job exceptions
    to Sentry, along with metadata about the job and worker.
"""
