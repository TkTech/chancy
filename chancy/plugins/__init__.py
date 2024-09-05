"""
Plugins
=======

This package contains the various built-in plugins available for Chancy.
Plugins are used to extend the functionality of Chancy, while allowing for
significant customization.

Chancy comes with several built-in plugins:

- :class:`chancy.plugins.pruner.Pruner`: This plugin is responsible for pruning
    old jobs from the database.
- :class:`chancy.plugins.recovery.Recovery`: This plugin is responsible for
    recovering jobs that were interrupted during execution.
- :class:`chancy.plugins.leadership.Leadership`: This plugin is responsible for
    managing leadership elections between multiple workers.
- :class:`chancy.plugins.cron.Cron`: This plugin is responsible for scheduling
    jobs to run at specific times.
- :class:`chancy.plugins.api.Api`: This plugin provides an API and dashboard
    for viewing the state of the Chancy cluster.
- :class:`chancy.plugins.workflow.WorkflowPlugin`: This plugin provides a
    workflow system for managing complex job dependencies.
"""
