FAQ
===

**Q.** Why?

**A.** For the most part, due to the issues laid out in the
:doc:`Compare with Celery <howto/celery>`, which is the current uncontested
incumbent in the Python task queue space. While there are many alternatives,
none of them checked off all of the boxes needed for my existing projects
to migrate off of Celery.

-----

**Q.** Why not sell 'pro' features like Oban Pro and Sidekiq Pro?

**A.** Chancy is not the product - it's a tool used in my *actual* products.
Having a fully open and community-driven project benefits all of the projects
using it. The more users using workflows, dashboards, etc the more reliable
and robust the project becomes for the revenue-generating products built on it.

-----

**Q.** Why ``Job`` instead of ``Task``?

**A.** To simplify developer QoL when merging with existing codebases that
already use Celery, Chancy uses the term "job" instead of "task". That
way you can import:

.. code-block:: python

    from chancy import Job
    from celery import Task

... and progressively migrate your codebase to Chancy without having to
rewrite all of your tasks or alias the imports.

-----

**Q.** Why can't I just do ``job.delay()``?

**A.** Supporting ``job.delay()`` like Celery does would require global
state to keep track of the currently active Chancy application. Chancy
has **absolutely no global state** as a hard rule, which helps minimize
the risk of bugs when dealing with many different types of concurrency
models in a single application.


-----

**Q.** Why not use advisory locks to make implementation of some features
easier?

**A.** Advisory locks don't support any form of namespacing - they're just
a numeric ID. This makes them unsuitable for use in a multi-tenant system
where you might have multiple different applications using the same database.
Advisory locks also have several issues when used with pgbouncer that might
stop us from officially supporting pgbouncer in the future.

-----

**Q.** Why does Chancy not support other databases or message brokers?

**A.** Chancy is intended to be highly customizable and reliant on the
implementation of features through plugins. Requiring plugins to support
multiple databases or message brokers would make them significantly more
complex or force targeting a lowest common denominator. We'd rather get
have everything we can out of Postgres and have a very narrow focus done
well than to have a broad focus done poorly.

Celery supports numerous backends and may be a better option of you're
looking for portability, but this comes at the cost of complexity and
limited features - no locking, rate limiting, workflows, etc.