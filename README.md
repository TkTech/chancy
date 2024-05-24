# Chancy

A postgres-backed task queue for Python.

This project is currently in the early stages of development. Use at your own
risk. It's guaranteed to be buggy and incomplete.

The goal is to provide a simple, easy-to-use task queue that can be used in a
wide variety of projects where the only infrastructure requirement is a postgres
database that you're probably already using anyway. Its features are developed
when needed for personal projects.

## TODO

- [x] Database migrations and version checker
- [ ] Implement a basic task queue worker (push a job, pull a job)
- [ ] Implement a basic plugin system
- [ ] Implement a basic cluster leader (leader election, leader-only plugins)

## Running the tests

The tests assume that a working postgres instance is available on port
8190 with the username "postgres" and the password "localtest". To run postgres
using docker and then run the tests:

```bash
docker-compose up -d
poetry install
poetry run pytest
```

## Similar Work

Many similar projects exist. Some of them are:

- https://worker.graphile.org/ (Node.js)
- https://riverqueue.com/ (Go)
- https://github.com/acaloiaro/neoq (Go)
- https://github.com/contribsys/faktory (Go)
- https://github.com/sorentwo/oban (Elixir)
- https://github.com/procrastinate-org/procrastinate (Python)

In particular, the blogs of the Oban authors and their excellent documentation
have been a good source of inspiration. Highly recommended for anyone that
needs a commercial-grade library.