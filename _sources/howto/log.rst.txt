Customize Logging
=================

Chancy will setup a default logger if you don't provide one which will log
``INFO`` and above to the console. If you want to customize the logging, such
as to use an existing application logger, you can pass a logger to the
:class:`~chancy.app.Chancy` constructor:

.. code-block:: python

    import logging
    from chancy import Chancy

    logger = logging.getLogger("my_application")

    chancy_app = Chancy(
        settings.my_database_dsn,
        log=logger,
    )

If you want to modify the logger after it's been created, you can get it from
the app:

.. code-block:: python

    import logging

    chancy_app = Chancy(settings.my_database_dsn)
    chancy_app.log.setLevel(logging.DEBUG)

.. tip::

  Setting logging to ``DEBUG`` can be pretty useful when developing locally,
  since it'll give you stack traces from failed jobs in the console. Just
  remember to turn it back down to ``INFO`` or ``WARNING`` in production since
  you may otherwise accidentally log sensitive information.

Or you can get it at any time using the normal global logging functions:

.. code-block:: python

    import logging

    logger = logging.getLogger("chancy")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
