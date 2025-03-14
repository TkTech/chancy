# Chancy

![Chancy Logo](misc/logo_small.png)

Chancy is a distributed task queue and scheduler for Python built on top of
Postgres.

![MIT License](https://img.shields.io/github/license/tktech/chancy)
![Codecov](https://img.shields.io/codecov/c/github/TkTech/chancy)
![PyPI Version](https://img.shields.io/pypi/v/chancy)
![Python Version](https://img.shields.io/pypi/pyversions/chancy)
![OS Platforms](https://img.shields.io/badge/OS-Linux%20|%20macOS%20|%20Windows-blue)
![PostgreSQL Versions](https://img.shields.io/badge/PostgreSQL-%2014%20|%2015%20|%2016%20|%2017-blue)

## Key Features

- **Robust Jobs** - support for priorities, retries, timeouts, scheduling,
  global rate limits, memory limits, global uniqueness, error
  capture, cancellation, and more
- **Minimal dependencies** - Core functionality requires only psycopg3 - which
  can be installed side-by-side with psycopg2.
- **Minimal infrastructure** - No need to run a separate service like
  RabbitMQ or redis. Every feature is built on top of Postgres. No need
  for separate monitoring services like Flower or schedulers like Celery
  Beat - everything is built-in to the worker.
- **Plugins** - Several plugins including a dashboard, workflows, cron jobs,
  and more.
- **Flexible** - A single worker can handle many queues and mix threads,
  processes, sub-interpreters, and asyncio jobs, allowing powerful workflows
  that use the optimal concurrency model for each job. Queues can be created,
  deleted, modified, and paused at runtime.
- **async-first** - Internals designed from the ground up to be async-first,
  but has minimal sync APIs for easy integration with existing non-async
  codebases.
- **Transactional enqueueing** - Atomically enqueue jobs and the data they
  depend on in a single transaction.
- **Portable** - Supports Linux, OS X, and Windows.
- **100% open & free** - no enterprise tiers or paid features.

## Documentation

Check out the getting-started guide and the API documentation at
https://tkte.ch/chancy/.

## Screenshots

Chancy comes with an optional dashboard that provides a basic
look into the status of your queues:

![Workflows](misc/ux_workflow.png)
![Queue Details](misc/ux_queue.png)
![Jobs](misc/ux_jobs.png)
![Job](misc/ux_job_failed.png)