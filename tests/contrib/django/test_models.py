"""
Tests for the Django models integration.
"""

import pytest

from chancy import job


@job()
def test_job():
    pass


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_chancy_job_django_query(chancy, worker):
    """
    Test that a job created in Chancy can be queried using the Django ORM.
    """
    from chancy.contrib.django.models import Job

    j = await chancy.push(test_job)

    orm_job = await Job.objects.aget(id=j.identifier)

    assert orm_job.id == j.identifier
    assert orm_job.queue == "default"
    assert orm_job.func.endswith("test_job")


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_chancy_worker_django_query(chancy, worker):
    """
    Test that we can query the Worker table.
    """
    from chancy.contrib.django.models import Worker

    orm_worker = await Worker.objects.aget(worker_id=worker.worker_id)

    assert orm_worker.worker_id == worker.worker_id
    assert len(orm_worker.tags) > 1 and "*" in orm_worker.tags


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_chancy_queue_django_query(chancy, worker):
    """
    Test that we can query the Queue table.
    """
    from chancy.contrib.django.models import Queue

    orm_queue = await Queue.objects.aget(name="default")

    assert orm_queue.name == "default"
    assert orm_queue.state == "active"
    assert orm_queue.concurrency is None
    assert len(orm_queue.tags) == 1
