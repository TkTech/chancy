Similar Projects
----------------

With the addition of modern Postgres features like ``LISTEN/NOTIFY`` and
``SELECT FOR UPDATE...SKIP LOCKED``, postgres-backed task queues have
become a viable alternative to other task queues built on RabbitMQ or
redis like celery_. As such the space is exploding with new projects.
Here are some of the most popular ones if Chancy doesn't fit your
needs:

.. list-table::
  :header-rows: 1
  :widths: 20 20 60

  * - Project
    - Language
    - Note
  * - celery_
    - Python
    - The defacto Python task queue
  * - procastinate_
    - Python
    -
  * - oban_
    - Elixir
    - Inspired many of the features in Chancy
  * - river_
    - Go
    -
  * - neoq_
    - Go
    -
  * - faktory_
    - Go
    -
  * - pg-boss_
    - Node.js
    -
  * - graphile_
    - Node.js
    -
  * - Minion_
    - Perl
    -

.. _celery: https://docs.celeryproject.org/en/stable/
.. _oban: https://hexdocs.pm/oban/Oban.html
.. _river: https://github.com/riverqueue/river
.. _procastinate: https://procrastinate.readthedocs.io/
.. _graphile: https://worker.graphile.org/
.. _neoq: https://github.com/acaloiaro/neoq
.. _faktory: https://github.com/contribsys/faktory
.. _pg-boss: https://github.com/timgit/pg-boss
.. _Minion: https://github.com/mojolicious/minion
