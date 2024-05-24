# Chancy

A postgres-backed task queue for Python.

This project is currently in the early stages of development. Use at your own
risk. It's guaranteed to be buggy and incomplete.

The goal is to provide a simple, easy-to-use task queue that can be used in a
wide variety of projects where the only infrastructure requirement is a postgres
database that you're probably already using anyway. Its features are added
as needed by the author's projects, so it may not be suitable for all use cases.

## Features

This project is designed to be simple and easy to use.

- asyncio-first design with a synchronous API for convenience.
- Fully-featured Jobs, with retries, timeouts, memory limits and more.
- Dependency-free except for psycopg3.
- Transactional job queueing. Jobs are only inserted into the database if the
  transaction they were created in is committed.

## Installation

Chancy is available on PyPI. You can install it with pip:

```bash
pip install chancy
```

Chancy follows SemVer, so you can pin your dependencies to a specific version
if you want to avoid breaking changes.

## Usage

First, start at least one worker to process jobs. You can do this with the
`chancy.worker.Worker` class. This example starts a worker that processes jobs
from the "default" queue, running at most 1 at a time.

```python
import asyncio
from chancy.app import Chancy, Queue, Job
from chancy.worker import Worker


def my_long_running_job():
  pass


async def main():
    async with Chancy(
        dsn="postgresql://postgres:localtest@localhost:8190/postgres",
        queues=[
            Queue(name="default", concurrency=1),
        ],
    ) as app:
        # Migrate the database to create the necessary tables if they don't
        # already exist. Don't do this automatically in production!
        await app.migrate()
        
        # Submit a job to the "default" queue. This job will run at most 3
        # times, with a timeout of 15 seconds and a memory limit of 1 GiB.
        await app.submit(
          Job(
            func=my_long_running_job,
            max_attempts=3,
            timeout=15,
            memory_limit=1024 ** 3,
          ),
          "default"
        )
        
        # Start the worker to process jobs from the "default" queue.
        await Worker(app).start()


if __name__ == "__main__":
    asyncio.run(main())

```

## Similar Work

Many similar projects exist. Some of them are:

- https://worker.graphile.org/ (Node.js)
- https://riverqueue.com/ (Go)
- https://github.com/acaloiaro/neoq (Go)
- https://github.com/contribsys/faktory (Go)
- https://github.com/sorentwo/oban (Elixir)
- https://github.com/procrastinate-org/procrastinate (Python)