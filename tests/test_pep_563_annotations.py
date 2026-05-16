"""Regression test for `from __future__ import annotations` (PEP 563).

`Executor.get_function_and_kwargs` inspects the job function's
signature to detect a `context: QueuedJob` keyword-only parameter and
inject the QueuedJob instance into the kwargs. Before the fix,
`param.annotation` returned the bare string `"QueuedJob"` when the
defining module used `from __future__ import annotations`. The
subsequent `issubclass(param.annotation, QueuedJob)` raised TypeError
on a string, the catch swallowed it, and the function was then called
without the required `context` arg — a TypeError logged only at DEBUG
on chancy's stdlib logger.

The fix resolves annotations via `typing.get_type_hints(func)` first,
falling back to `param.annotation` when resolution fails (e.g.
forward references unresolvable in the function's globals).

This test is purely static — it does not require a Postgres
connection or a running worker; it drives
`Executor.get_function_and_kwargs` directly.
"""

from __future__ import annotations

from datetime import datetime, timezone

from chancy import QueuedJob, job
from chancy.executors.base import Executor


@job()
async def _job_with_future_annotations(execution_id: str, *, context: QueuedJob) -> None:
    """Job with `from __future__ import annotations` at module top.

    The annotation `context: QueuedJob` is stored as the string
    `"QueuedJob"` at runtime under PEP 563.
    """
    del context, execution_id


def _build_queued_job(func) -> QueuedJob:
    """Mirror what chancy would have stored after `chancy.push(...)`."""
    template = func.job
    return QueuedJob(
        id="test-job",
        func=template.func,
        kwargs={**(template.kwargs or {}), "execution_id": "abc"},
        queue=template.queue,
        priority=template.priority,
        max_attempts=template.max_attempts,
        scheduled_at=template.scheduled_at,
        created_at=datetime.now(tz=timezone.utc),
        attempts=0,
        limits=template.limits,
        unique_key=template.unique_key,
        meta=template.meta,
    )


def test_get_function_and_kwargs_resolves_future_annotations() -> None:
    """Context injection works when the @job module uses PEP 563.

    Before the fix this assertion failed because `param.annotation`
    was the string `"QueuedJob"` and chancy's `issubclass` check
    raised TypeError (silently swallowed).
    """
    queued = _build_queued_job(_job_with_future_annotations)

    func, kwargs = Executor.get_function_and_kwargs(queued)

    assert func is _job_with_future_annotations
    assert "context" in kwargs, (
        "QueuedJob context was not injected — likely because the "
        "annotation was a stringified `QueuedJob` (PEP 563) and "
        "`issubclass(str, QueuedJob)` swallowed silently. See fix in "
        "chancy/executors/base.py."
    )
    assert isinstance(kwargs["context"], QueuedJob)
    # The user-provided kwarg must also round-trip alongside the
    # injected context.
    assert kwargs["execution_id"] == "abc"
