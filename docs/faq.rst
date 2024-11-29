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