Basic Concepts
==============

Chancy runs :doc:`jobs` on one or more :doc:`workers` that listen to
one or more :doc:`queues`, using :doc:`executors` to run the jobs.

You can use :doc:`plugins` to add additional functionality to Chancy,
such as leadership election, job recovery, pruning, scheduling, and
more.

.. toctree::
   :maxdepth: 4
   :caption: Contents:

   jobs
   queues
   workers
   plugins
   executors