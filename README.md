# Chancy

![Chancy Logo](misc/logo_small.png)

A postgres-backed task queue for Python.

![MIT License](https://img.shields.io/github/license/tktech/chancy)
![Codecov](https://img.shields.io/codecov/c/github/TkTech/chancy)
![PyPI Version](https://img.shields.io/pypi/v/chancy)
![Python Version](https://img.shields.io/pypi/pyversions/chancy)
![OS Platforms](https://img.shields.io/badge/OS-Linux%20|%20macOS-blue)
![PostgreSQL Versions](https://img.shields.io/badge/PostgreSQL-%2014%20|%2015%20|%2016%20|%2017-blue)

## Key Features

- Support for job priorities, retries, timeouts, scheduling,
  global rate limits, memory limits, unique jobs, and more
- asyncio-based worker with support for asyncio, threaded,
  process-based, and sub-interpreter job execution.
- Configurable job retention for easy debugging and tracking
- Minimal dependencies (only psycopg3 required)
- Plugins for a dashboard, workflows, cron jobs, and more
- Optional transactional enqueueing for atomic job creation
- asyncio & sync APIs for easy integration with existing codebases

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