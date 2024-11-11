Use with Fastapi
================

While you can keep your Chancy worker separate from your FastAPI application,
you can also run your Chancy worker in the same process as your FastAPI
application by using FastAPI's lifespan events:

.. code-block:: python

  import asyncio
  import contextlib
  from typing import AsyncIterator

  from fastapi import FastAPI
  from chancy import Chancy, Worker, Queue, Job

  chancy = Chancy(
      dsn="postgresql://postgres:localtest@localhost:8190/postgres",
      plugins=[],
  )

  @contextlib.asynccontextmanager
  async def lifespan(app: FastAPI) -> AsyncIterator[None]:
      """
      FastAPI lifespan handler that starts and stops the Chancy worker.
      This ensures the worker starts when FastAPI starts and shuts down properly.
      """
      await chancy.pool.open()
      await chancy.migrate()

      # Declare any queues we need
      await chancy.declare(Queue("default"))

      # Create and start the worker
      worker = Worker(chancy)
      asyncio.create_task(worker.start())

      try:
          yield
      finally:
          await chancy.pool.close()


  app = FastAPI(lifespan=lifespan)


  async def send_an_email():
      print("Sending an email")


  @app.get("/")
  async def read_root():
      await chancy.push(Job.from_func(send_an_email))
      return {"Hello": "World"}
