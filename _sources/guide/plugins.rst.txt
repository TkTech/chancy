Plugins
=======

Chancy comes with a number of plugins that can be used to extend its
functionality. Some of these, like the
:class:`~chancy.plugins.recovery.Recovery`, should be used in every
Chancy application if not replaced by a custom implementation.

Plugins are any class that is a subclass of :class:`~chancy.plugin.Plugin`.
Plugins are pretty simple, and only have 3 methods that need to be
implemented:

.. code-block:: python

  from chancy.hub import Hub
  from chancy.plugin import Plugin
  from chancy.worker import Worker
  from chancy.logger import logger


  class Example(Plugin):
      @classmethod
      def get_type(cls):
          return cls.Type.LEADER

      async def on_startup(self, hub: Hub):
          hub.on(Worker.Event.ON_LEADERSHIP_LOOP, self._on_loop)

      async def on_shutdown(self, hub: Hub):
          hub.remove(Worker.Event.ON_LEADERSHIP_LOOP, self._on_loop)

      async def _on_loop(self, worker: Worker):
          logger.info("I'm the leader!")


The `get_type` method should return the type of plugin. This can be
either `Plugin.Type.LEADER`, in which case the plugin will only run
on a single worker in the entire cluster, or `Plugin.Type.WORKER`
which will run on every worker in the cluster.

The `on_startup` method is called when the plugin is started, and
the `on_shutdown` method is called when the plugin is stopped. This
doesn't mean the worker is shutting down, just that the plugin is
being stopped. For example if a worker loses leadership, any
leader plugins will be stopped and then started again on the new
leader.

All plugins work by hooking onto events emitted by the
:class:`~chancy.hub.Hub`. These events run in the event loop of the
worker, so they should be non-blocking. If you need to do something
that is blocking, you should use the `loop.run_in_executor` method
to run it in a separate thread.

Required Plugins
----------------

Chancy doesn't enable any plugins by default, but there are some that
are required for Chancy to work properly in production. You should
either enable and configure these plugins or provide your own:

.. list-table:: Required Plugins
   :header-rows: 1

   * - Name
     - Description
     - Type

   * - :class:`~chancy.plugins.recovery.Recovery`
     - Handles the recovery of jobs whose workers have crashed.
     - Leader

   * - :class:`~chancy.plugins.pruner.Pruner`
     - Deletes completed jobs from the database.
     - Leader


Optional Plugins
----------------

.. list-table:: Required Plugins
   :header-rows: 1

   * - Name
     - Description
     - Type

   * - :class:`~chancy.plugins.sentry.Sentry`
     - Adds extra contextual information to Sentry.
     - Worker
