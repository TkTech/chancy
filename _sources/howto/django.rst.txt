Use with Django
===============

Chancy works with both synchronous and asynchronous Django applications. You
shouldn't need to make any changes to your existing code.

Install chancy:

.. code-block:: bash

    $ pip install chancy[cli]


Next to your Django settings module, create a new file called ``worker.py``.
This file will contain the code that defines your chancy app:

.. code-block:: python

  import os
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_application.settings")
  import django
  django.setup()
  from django.conf import settings

  from chancy import Chancy
  from chancy.plugins.leadership import Leadership
  from chancy.plugins.pruner import Pruner
  from chancy.plugins.recovery import Recovery

  chancy_app = Chancy(
      settings.my_database_dsn,
      plugins=[
          Pruner(Pruner.Rules.Age() > 60 * 60 * 48),
          Recovery(),
          Leadership(),
      ],
  )


And then use the CLI to migrate the database and start a worker process:

.. code-block:: bash

    chancy --app my_application.worker.chancy_app misc migrate
    chancy --app my_application.worker.chancy_app worker start


If you're just using the :class:`~chancy.executors.asyncex.AsyncExecutor` or the
:class:`~chancy.executors.thread.ThreadedExecutor`, you're probably done here.
If you're using the :class:`~chancy.executors.process.ProcessExecutor`, you'll
want to add a little stub to ensure that django gets setup when the process
starts. Make a file next to your settings.py called ``utils.py``:

.. code-block:: python

  import os

  from chancy.executors.process import ProcessExecutor


  class DjangoExecutor(ProcessExecutor):
      """
      A process-based executor that ensures django is fully setup before
      processing any jobs.
      """

      @staticmethod
      def on_initialize_worker():
          os.environ.setdefault(
              "DJANGO_SETTINGS_MODULE",
              "my_application.settings",
          )
          import django

          django.setup()


When you declare a queue using the ``ProcessExecutor``, you'll want to tell it to use the
executor you just defined instead:

.. code-block:: python

    await chancy_app.declare(
        Queue(
            "default",
            concurrency=5,
            executor="my_application.utils.DjangoExecutor",
        ),
    )

And that's it! You can now use all your ORM models, plugins, and other Django
goodies in your chancy tasks.
