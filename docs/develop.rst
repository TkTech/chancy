Developing Chancy
=================

Want to extend Chancy itself or help fix a bug? Awesome, lets go!

Get the code
------------

.. code-block:: bash

    git clone https://github.com/tktech/chancy.git
    cd chancy
    uv sync --all-extras

Start PostgreSQL
----------------

The tests, the demo, and the examples all expect PostgreSQL at:

.. code-block:: text

    postgresql://postgres:localtest@localhost:8190/postgres

The repository's ``docker-compose.yml`` starts a server that matches:

.. code-block:: bash

    docker compose up -d

This server is configured to log every statement along with its query plan,
which helps when tracking down slow or unexpected queries. Follow the logs with
``docker compose logs -f db``.

.. warning::

    The tests create Chancy's tables and drop them when they finish. Use a
    server dedicated to development.

Run the tests
-------------

Run the whole suite in parallel:

.. code-block:: bash

    uv run pytest -n auto

The django integration is a bit of a special case can't can't be run in
parallel, so kick it off on its own:

.. code-block:: bash

    uv run pytest tests/contrib/django/test_models.py

Lint and format
---------------

Chancy uses `ruff <https://docs.astral.sh/ruff/>`_ for linting and formatting.
CI fails if either reports a problem:

.. code-block:: bash

    uv run ruff check --fix
    uv run ruff format

To run these automatically on every commit, install the pre-commit hooks:

.. code-block:: bash

    uv run pre-commit install

The hooks also type-check the dashboard when you change it, which needs the
dashboard's npm dependencies installed (see below).

Work on the dashboard
---------------------

The dashboard is a React app in ``chancy/plugins/api/ui``. Install its
dependencies and build it:

.. code-block:: bash

    npm --prefix chancy/plugins/api/ui ci
    npm --prefix chancy/plugins/api/ui run build

The build is written to ``chancy/plugins/api/dist``, which the
:class:`~chancy.plugins.api.Api` plugin serves. It isn't checked into Git, so
build it at least once before running the demo or using the dashboard from a
source checkout.

To get live reloading while you work, start the demo. It generates a steady
stream of fake traffic and serves the API on port 8000:

.. code-block:: bash

    uv run python -m examples.demo

Then, in a second terminal, start the Vite dev server, which forwards API
requests to the demo:

.. code-block:: bash

    npm --prefix chancy/plugins/api/ui run dev

Open the URL that Vite prints and sign in with ``demo`` / ``demo``. Some jobs
in the demo fail on purpose so the failure views have something to show.

Before pushing, run the checks CI and the pre-commit hooks run:

.. code-block:: bash

    npm --prefix chancy/plugins/api/ui test
    npm --prefix chancy/plugins/api/ui run lint
    npm --prefix chancy/plugins/api/ui run build

Build the documentation
-----------------------

Build the docs the same way CI does:

.. code-block:: bash

    uv run make -C docs clean doctest html SPHINXOPTS="-W"

Then open ``docs/_build/html/index.html``. ``-W`` turns warnings, such as
broken references, into errors.

The dashboard screenshots aren't checked into Git either, and a strict build
fails without them. Build the dashboard first, then:

.. code-block:: bash

    uv run --group screenshots playwright install chromium
    uv run make -C docs screenshots

This starts the demo, lets it run for 90 seconds so there's real activity to
show, and captures each page with a headless browser. A bit annoying, but
there's no risk of the documentation screenshots ever getting out of date with
the real dashboard.
