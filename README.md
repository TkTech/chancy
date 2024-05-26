# Chancy

![Chancy Logo](misc/logo_small.png)

A postgres-backed task queue for Python.

This project is currently in the early stages of development. Use at your own
risk. It's guaranteed to be buggy and incomplete.

## Features

- Fully-featured Jobs, with priorities, retries, timeouts, memory limits, future
  scheduling, and more.
- Transactional job queueing. Jobs are only inserted into the database if the
  transaction they were created in is committed.
- Completed jobs stick around in the database for easy debugging and job
  tracking, with configurable retention policies.
- Dependency-free except for psycopg3.
- Multi-tenant support with prefixes for all database tables.

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