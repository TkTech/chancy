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
  from chancy import Chancy, Worker, Queue, job

  chancy = Chancy("postgresql://localhost/postgres")

  @contextlib.asynccontextmanager
  async def lifespan(app: FastAPI) -> AsyncIterator[None]:
      """
      FastAPI lifespan handler that starts and stops the Chancy worker.
      This ensures the worker starts when FastAPI starts and shuts down properly.
      """
      # Run the database migrations (don't do this in production)
      await chancy.migrate()

      # Declare any queues we need (do this just once in production)
      await chancy.declare(Queue("default"))

      async with Worker(chancy) as worker:
        yield


  app = FastAPI(lifespan=lifespan)


  @job(queue="default")
  async def send_an_email():
      print("Sending an email")


  @app.get("/")
  async def read_root():
      await chancy.push(send_an_email)
      return {"Hello": "World"}
