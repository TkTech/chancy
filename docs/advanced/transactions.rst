Push A Job With A Transaction
=============================

It's possible to push jobs to the queue within a non-Chancy controlled
transaction. This is useful when you want to ensure that a job is pushed to the
queue atomically with other database operations.

As a simple example, consider a money transfer operation. You want to ensure
that the transfer is atomic with the job that notifies the user of the
transfer:

.. code-block:: python
   :caption: job.py

   import asyncio
   from psycopg import AsyncConnection
   from chancy import Job, Queue, Chancy

   q = Queue(name="default", concurrency=10)
   chancy = Chancy(
       dsn="postgresql://localhost/postgres",
       plugins=[q],
   )

   async def main():
       async with chancy:
           # Create a non-Chancy controlled transaction
           async with await AsyncConnection.connect(chancy.dsn) as conn:
               async with conn.cursor() as cursor:
                   async with conn.transaction():
                       await cursor.execute(
                           "UPDATE accounts SET balance = balance - 100 WHERE id = 1"
                       )
                       await cursor.execute(
                           "UPDATE accounts SET balance = balance + 100 WHERE id = 2"
                       )
                       await q.push_jobs(
                           cursor,
                           Job(func="my_module.notify_transfer", kwargs={"amount": 100}),
                       )

In this example, the job that notifies the user of the transfer is pushed to
the queue _only_ if the ``UPDATE`` is successful. If the transaction fails, the
job is not pushed to the queue.