import asyncio
import inspect
from unittest.mock import AsyncMock, Mock

import click
import pytest

from chancy import Chancy, Worker
from chancy.cli.worker import web_command
from chancy.plugins.api import Api, SimpleAuthBackend
from chancy.plugins.metrics import Metrics


@pytest.mark.asyncio
@pytest.mark.parametrize("with_metrics", [False, True])
@pytest.mark.parametrize("configured_api", [False, True])
@pytest.mark.parametrize("failure", ["cancel", "service", "setup"])
async def test_dashboard_session_lifecycle(
    chancy_just_app, monkeypatch, with_metrics, configured_api, failure
):
    app = chancy_just_app
    monkeypatch.setattr(Chancy, "__aenter__", AsyncMock(return_value=app))
    monkeypatch.setattr(Chancy, "__aexit__", AsyncMock(return_value=False))
    monkeypatch.setattr(Worker, "_maintain_notifications", AsyncMock())
    monkeypatch.setattr(Worker, "_maintain_updates", AsyncMock())
    monkeypatch.setattr(Worker, "_maintain_queues", AsyncMock())
    monkeypatch.setattr(Worker, "flush", AsyncMock())
    metrics = Metrics()
    monkeypatch.setattr(metrics, "flush", AsyncMock())
    if with_metrics:
        app.plugins[metrics.get_identifier()] = metrics
    if configured_api:
        app.plugins[Api.get_identifier()] = Api(
            authentication_backend=SimpleAuthBackend({}), port=9000
        )
    entered = asyncio.Event()
    cleanup = Mock()
    sessions = []
    setup = Api.setup

    async def prepare(self, session):
        sessions.append(session)
        session.callback(cleanup)
        await setup(self, session)
        if failure == "setup":
            raise ValueError("setup failed")

    async def serve(self, ctx):
        assert self.port == (9000 if configured_api else 8000)
        if with_metrics:
            assert ctx.worker in metrics._workers
        entered.set()
        if failure == "service":
            raise ValueError("service failed")
        await asyncio.Future()

    monkeypatch.setattr(Api, "setup", prepare)
    monkeypatch.setattr(Api, "_serve", serve)
    context = click.Context(web_command, obj={"app": app})
    for name in ("host", "port", "debug", "allow_origin"):
        context.set_parameter_source(name, click.core.ParameterSource.DEFAULT)
    task = asyncio.create_task(
        inspect.unwrap(web_command.callback)(
            context, "localhost", 8000, False, ["*"]
        )
    )
    if failure == "cancel":
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(ValueError, match=f"{failure} failed"):
            await asyncio.wait_for(task, timeout=1)

    cleanup.assert_called_once_with()
    assert not sessions[0].worker.manager._tasks
    assert not metrics._workers
    assert not sessions[0].worker.hub._handlers.get("metrics.counter")
    Worker._maintain_queues.assert_not_awaited()
    if failure == "setup":
        assert not entered.is_set()
        metrics.flush.assert_not_awaited()
    elif with_metrics:
        metrics.flush.assert_awaited_once_with(app)
