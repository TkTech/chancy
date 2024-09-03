# Chancy

![Chancy Logo](misc/logo_small.png)

A postgres-backed task queue for Python.

## Key Features

- Postgres-backed for reliability and familiarity
- Support for job priorities, retries, timeouts, scheduling,
  global rate limits, and more
- Configurable job retention for easy debugging and tracking
- Minimal dependencies (only psycopg3 required)
- Optional transactional job queueing
- asyncio-based worker for efficient processing
- Plugins for a dashboard, workflows, and more

## Documentation

Checkout the getting-started guide and the API documentation at
https://tkte.ch/chancy/.

## Similar Work

Many similar projects exist. Some of them are:

- https://worker.graphile.org/ (Node.js)
- https://riverqueue.com/ (Go)
- https://github.com/acaloiaro/neoq (Go)
- https://github.com/contribsys/faktory (Go)
- https://github.com/sorentwo/oban (Elixir)
- https://github.com/procrastinate-org/procrastinate (Python)