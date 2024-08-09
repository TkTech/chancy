Workers
=======

In Chancy, a Worker is responsible for executing jobs from one or more queues. Understanding how to configure and manage workers is crucial for optimizing your task processing pipeline.

What is a Worker?
-----------------

A Worker is an asyncio-based server that:

- Listens to one or more queues for jobs
- Executes jobs using specified executors
- Can be extended with plugins for additional functionality

Creating a Worker
-----------------

Here's a basic example of creating and starting a worker:

.. code-block:: python

   import asyncio
   from chancy import Chancy, Worker, Queue

   chancy = Chancy(dsn="postgresql://localhost/postgres")

   async def main():
       async with chancy:
           await chancy.migrate()
           await chancy.declare(Queue(name="default", concurrency=10))
           await Worker(chancy).start()

   if __name__ == "__main__":
       asyncio.run(main())

This script:

1. Creates a Chancy instance connected to a PostgreSQL database
2. Runs any necessary database migrations
3. Declares a "default" queue with a concurrency of 10
4. Creates and starts a worker

Worker Tags
-----------

Tags are used to assign queues to specific workers. Every worker automatically gets some tags:

- ``hostname=<machine hostname>``
- ``worker_id=<worker id>``
- ``python=<Python version>``
- ``arch=<machine architecture>``
- ``os=<operating system>``
- ``*`` (wildcard tag)

You can add custom tags when creating a worker:

.. code-block:: python

   Worker(chancy, tags={"has=gpu", "env=production"})

Queues can then be assigned to workers based on these tags:

.. code-block:: python

   Queue(name="gpu_jobs", tags={"has=gpu"}, concurrency=2)

This queue will only run on workers with the "has=gpu" tag.

Plugins
-------

Workers can be extended with plugins for additional functionality:

.. code-block:: python

   from chancy.plugins.pruner import Pruner
   from chancy.plugins.recovery import Recovery
   from chancy.plugins.leadership import Leadership

   chancy = Chancy(
       dsn="postgresql://localhost/postgres",
       plugins=[
           Pruner(),
           Recovery(),
           Leadership(),
       ],
   )

Built-in plugins include:

- **Pruner**: Removes old jobs from the database
- **Recovery**: Recovers jobs from crashed workers
- **Leadership**: Elects a leader worker for certain tasks
- **Cron**: Schedules recurring jobs with crontab syntax
- **Web**: An API, WebSocket, and dashboard for monitoring Chancy

Example: A Complete Worker Setup
--------------------------------

Here's a more comprehensive example of setting up a worker with multiple queues and plugins:

.. code-block:: python

   import asyncio
   from chancy import Chancy, Worker, Queue
   from chancy.plugins.rules import Age
   from chancy.plugins.pruner import Pruner
   from chancy.plugins.recovery import Recovery
   from chancy.plugins.leadership import Leadership
   from chancy.plugins.cron import Cron
   from chancy.plugins.web import Web

   async def main():
       chancy = Chancy(
           dsn="postgresql://localhost/postgres",
           plugins=[
               Pruner(
                   # Remove jobs older than 1 hour
                   Age() > 3600,
                   # and only prune every hour
                   poll_interval=3600
               ),
               Recovery(),
               Leadership(),
               Cron(),
               Web()
           ],
       )

       async with chancy:
           await chancy.migrate()

           # Declare queues
           await chancy.declare(Queue(name="default", concurrency=10))
           await chancy.declare(Queue(
               name="high_priority",
               concurrency=5,
               tags={"role=priority"}
           ))
           await chancy.declare(Queue(name="low_priority", concurrency=2))

           # Start worker
           worker = Worker(
               chancy,
               worker_id="worker-1",
               tags={"role=priority", "env=dev"},
               heartbeat_poll_interval=15
           )
           await worker.start()

   if __name__ == "__main__":
       asyncio.run(main())

This example sets up a worker with multiple queues, plugins, and custom
configuration, demonstrating how to create a more complex and feature-rich
worker setup.

Next Steps
----------
- Learn about :doc:`jobs` to understand the core unit of work in Chancy
- Explore :doc:`queues` to see how jobs are organized and distributed
- Check out :doc:`executors` to learn about different ways of running jobs
- Discover :doc:`plugins` to extend Chancy's functionality