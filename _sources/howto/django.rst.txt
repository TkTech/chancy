Use with Django
===============

Chancy works with both synchronous and asynchronous Django applications. You
shouldn't need to make any changes to your existing code.

Install chancy:

.. code-block:: bash

    $ pip install chancy[cli,django]


Django Tasks Backend
--------------------

.. note::

    This feature requires Django 6.0+ and is experimental. Feedback is welcome!

Chancy provides a backend for Django's built-in tasks framework, allowing you
to use the standard Django Tasks API with Chancy as the underlying queue.

Configure the backend in your Django settings:

.. code-block:: python

    # settings.py
    TASKS = {
        "default": {
            "BACKEND": "chancy.contrib.django.backend.ChancyBackend",
            "OPTIONS": {
                "dsn": None,  # Uses DATABASES["default"] if not set
                "prefix": "chancy_",
            },
        },
    }

Then use the standard Django Tasks API:

.. code-block:: python

    from django.tasks import task

    @task
    def my_task(arg1, arg2):
        return arg1 + arg2

    # Enqueue the task
    result = my_task.enqueue(1, 2)

    # Check the result
    result = result.refresh()
    if result.is_successful:
        print(result.return_value)

The Chancy backend supports:

- Synchronous and asynchronous tasks
- Task priorities
- Deferred execution (``run_after``)
- Task context (``takes_context=True``)
- Result retrieval

You'll still need to use the Chancy CLI or library to declare your queues
and run workers. See the :doc:`How To guides </howto/index>` for details on
setting up workers.


Using Django models and features in Chancy
------------------------------------------

Next to your Django ``settings.py`` module, create a new file called
``worker.py``. This file will contain the code that defines your chancy
app:

.. code-block:: python

  import os
  os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_application.settings")
  import django
  django.setup()
  from django.conf import settings

  from chancy import Chancy

  chancy_app = Chancy(settings.DATABASES["default"])


And then use the CLI to migrate the database and start a worker process:

.. code-block:: bash

    chancy --app my_application.worker.chancy_app misc migrate
    chancy --app my_application.worker.chancy_app worker start


This ensures django is fully setup before processing any jobs.


Using Chancy from the django ORM and Admin
------------------------------------------

.. note::

    This feature is new in version 0.22.0 and seeking feedback on desired
    functionality and use cases.

Chancy can be used from the Django ORM and Admin interface. To do this, you
need to add the following to your Django settings:

.. code-block:: python

    INSTALLED_APPS = [
        ...
        "chancy.contrib.django",
    ]


This gives you access to the Jobs, Queues, and Workers models. Some plugins,
like the Cron and Workflow plugins, also provide their own django extensions:

.. code-block:: python

    INSTALLED_APPS = [
        ...
        "chancy.contrib.django",
        "chancy.plugins.cron.django",
        "chancy.plugins.workflow.django",
    ]

Now you can create new cron jobs in the admin, query the status of your jobs,
workers and workflows from the comfort of the Django ORM.

.. code-block:: python

    from chancy.contrib.django.models import Job

    j = await chancy.push(test_job)

    orm_job = await Job.objects.aget(id=j.identifier)


.. important::

  The current implementation assumes that the chancy tables live in the same
  database as your Django "default" database.


Login to Chancy with Django's superusers
----------------------------------------

To use Django's authentication system with Chancy's API and dashboard,
you can use the included ``DjangoAuthBackend`` authentication backend. This
backend supports lets you login to chancy using the username and password
of a Django superuser.

.. code-block:: python

    import os
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "my_application.settings")
    import django
    django.setup()
    from django.conf import settings
    from chancy.contrib.django.auth import DjangoAuthBackend

    app = Chancy(
        dsn=settings.DATABASES["default"],
        plugins=[
            Api(
                authentication_backend=DjangoAuthBackend(),
                secret_key=settings.SECRET_KEY,
            ),
        ],
    )