import datetime
import json
from unittest.mock import AsyncMock, Mock
from urllib.parse import quote, urljoin
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from psycopg import sql
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from chancy import Chancy, Job, Queue, Worker
from chancy.plugins.api import Api, SimpleAuthBackend, _SPAStaticFiles
from chancy.plugins.api.plugin import ApiPlugin
from chancy.plugins.workflow import Workflow, WorkflowPlugin
from chancy.plugins.workflow.api import WorkflowApiPlugin


@pytest.fixture
def api(monkeypatch, tmp_path):
    # Tests also run from source checkouts without a built dashboard.
    (tmp_path / "index.html").write_text(
        '<html><head><base href="/">'
        '<script src="./assets/app.js"></script></head></html>',
        encoding="utf-8",
    )
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "app.js").write_text("/* dashboard */")
    monkeypatch.setattr(
        "chancy.plugins.api._SPAStaticFiles",
        lambda **kwargs: _SPAStaticFiles(directory=tmp_path, html=True),
    )
    return Api(authentication_backend=SimpleAuthBackend({"admin": "password"}))


def mount(app, prefix):
    # Separate mounts exercise accumulation of root_path at each level.
    for segment in reversed(prefix.strip("/").split("/")):
        if segment:
            parent = Starlette()
            parent.mount(f"/{segment}", app)
            app = parent
    return app


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"tags": []},
        {"tags": ["reporting"]},
        {"concurrency": 2, "polling_interval": 10, "eager_polling": True},
    ],
)
def test_create_queue_uses_python_defaults(
    api, chancy_just_app, monkeypatch, overrides
):
    declare = AsyncMock(side_effect=lambda queue, **kwargs: queue)
    monkeypatch.setattr(chancy_just_app, "declare", declare)
    app = api.build_starlette_app(Worker(chancy_just_app), chancy_just_app)
    with TestClient(app) as client:
        login = client.post(
            "/api/v1/login",
            json={"username": "admin", "password": "password"},
        )
        response = client.post(
            "/api/v1/queues",
            json={"name": "review", **overrides},
            headers={"Authorization": f"Bearer {login.json()['token']}"},
        )

    options = dict(overrides)
    if "tags" in options:
        options["tags"] = set(options["tags"])
    expected = Queue("review", **options)
    assert response.status_code == 200
    assert response.json() == expected.pack()
    declare.assert_awaited_once_with(expected, upsert=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("pagination", [False, True])
@pytest.mark.parametrize("limit", [2, 5])
@pytest.mark.parametrize(
    "filters",
    [
        {},
        {"queue": "pages"},
        {"filters": json.dumps([["queue", "=", "pages"]])},
    ],
)
async def test_jobs_pagination_uses_id_order(
    api, chancy, worker_no_start, filters, pagination, limit
):
    await chancy.declare(Queue("pages"))
    now = datetime.datetime.now(datetime.UTC)
    refs = [
        await chancy.push(
            Job(
                func="unused",
                queue="pages",
                # Oppose ID order, with tied dates and null completion times.
                scheduled_at=now + datetime.timedelta(days=3 - i // 2),
            )
        )
        for i in range(5)
    ]
    expected = sorted((str(ref.identifier) for ref in refs), reverse=True)
    app = api.build_starlette_app(worker_no_start, chancy)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        login = await client.post(
            "/api/v1/login",
            json={"username": "admin", "password": "password"},
        )
        client.headers["Authorization"] = f"Bearer {login.json()['token']}"
        params = {"limit": limit, **filters}
        if pagination:
            params["pagination"] = "true"
        for offset in range(0, len(expected), limit):
            response = await client.get("/api/v1/jobs", params=params)
            assert response.status_code == 200
            payload = response.json()
            items = payload["items"] if pagination else payload
            ids = [job["id"] for job in items]
            assert ids == expected[offset : offset + limit]
            if pagination:
                has_more = offset + limit < len(expected)
                assert payload["has_more"] is has_more
                assert payload["next_cursor"] == (ids[-1] if has_more else None)
            params["before"] = ids[-1]

            if offset == 0:
                # Completion and rescheduling between pages must not move
                # jobs across the cursor boundary.
                async with chancy.pool.connection() as conn:
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET state = 'succeeded', "
                            "completed_at = NOW(), scheduled_at = NOW()"
                        ).format(sql.Identifier(f"{chancy.prefix}jobs"))
                    )

        response = await client.get("/api/v1/jobs", params=params)
        assert response.status_code == 200
        assert response.json() == (
            {"items": [], "has_more": False, "next_cursor": None}
            if pagination
            else []
        )


@pytest.mark.parametrize("limit", ["0", "-1", "invalid"])
def test_jobs_reject_invalid_page_limits(api, chancy_just_app, limit):
    app = api.build_starlette_app(Worker(chancy_just_app), chancy_just_app)
    with TestClient(app) as client:
        login = client.post(
            "/api/v1/login",
            json={"username": "admin", "password": "password"},
        )
        response = client.get(
            "/api/v1/jobs",
            params={"pagination": "true", "limit": limit},
            headers={"Authorization": f"Bearer {login.json()['token']}"},
        )
    assert response.status_code == 422


@pytest.mark.parametrize("prefix", ["", "/chancy", "/internal/chancy"])
@pytest.mark.parametrize("root_path", ["", "/proxy"])
def test_dashboard_mount(api, chancy_just_app, prefix, root_path):
    app = mount(
        api.build_starlette_app(Worker(chancy_just_app), chancy_just_app),
        prefix,
    )
    base = root_path + prefix + "/"
    with TestClient(app, root_path=root_path) as client:
        for page in ["", "dashboard", "jobs/example", "queues/default/"]:
            response = client.get(base + page)
            assert response.status_code == 200
            assert f'<base href="{base}">' in response.text
            assert response.headers["cache-control"] == "no-store"
            assert "etag" not in response.headers

            asset = client.get(urljoin(base, "./assets/app.js"))
            assert asset.status_code == 200
            assert asset.text == "/* dashboard */"
            assert "javascript" in asset.headers["content-type"]
            # Hashed assets retain ordinary conditional caching.
            assert (
                client.get(
                    urljoin(base, "./assets/app.js"),
                    headers={"If-None-Match": asset.headers["etag"]},
                ).status_code
                == 304
            )

        head = client.head(base + "jobs/example")
        assert head.status_code == 200
        assert head.content == b""
        assert int(head.headers["content-length"]) > 0

        if prefix:
            redirect = client.get(base.rstrip("/"), follow_redirects=False)
            assert redirect.status_code == 307
            assert redirect.headers["location"].endswith(base)


def test_base_href_is_encoded_and_varies_by_mount(api, chancy_just_app):
    dashboard = api.build_starlette_app(
        Worker(chancy_just_app), chancy_just_app
    )
    app = Starlette()
    prefixes = ["/first", '/second/é"<>']
    for prefix in prefixes:
        app.mount(prefix, dashboard)

    with TestClient(app) as client:
        for prefix in prefixes:
            response = client.get(prefix + "/jobs/example")
            assert response.status_code == 200
            expected = quote(prefix + "/", safe="/")
            assert f'<base href="{expected}">' in response.text


@pytest.mark.parametrize("prefix", ["", "/internal/chancy"])
def test_mounted_authentication_and_websocket(api, chancy_just_app, prefix):
    worker = Worker(chancy_just_app)
    app = mount(api.build_starlette_app(worker, chancy_just_app), prefix)
    base = prefix + "/api/v1"
    with TestClient(app) as client:
        assert client.get(base + "/configuration").status_code == 403
        assert (
            client.post(
                base + "/login", json={"username": "admin", "password": "wrong"}
            ).status_code
            == 401
        )
        login = client.post(
            base + "/login", json={"username": "admin", "password": "password"}
        )
        assert login.status_code == 200
        token = login.json()["token"]
        configuration = client.get(
            base + "/configuration",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert configuration.status_code == 200
        assert "plugins" in configuration.json()

        with (
            pytest.raises(WebSocketDisconnect),
            client.websocket_connect(base + "/ws"),
        ):
            pass
        with client.websocket_connect(base + f"/ws?token={token}") as ws:
            client.portal.call(worker.hub.emit, "test.event", {"value": 1})
            assert ws.receive_json() == {
                "event": "test.event",
                "data": {"value": 1},
            }
        assert not worker.hub._wildcard_handlers


def test_app_factory_binds_each_app_and_discovers_plugins(api, monkeypatch):
    class ContextApiPlugin(ApiPlugin):
        def name(self):
            return "context"

        def routes(self):
            async def context(request, *, chancy, worker):
                return JSONResponse([chancy.prefix, worker.worker_id])

            return [
                {
                    "path": "/context",
                    "endpoint": context,
                    "methods": ["GET"],
                    "name": "context",
                }
            ]

    monkeypatch.setattr(
        "chancy.plugins.api.import_string", lambda path: ContextApiPlugin
    )
    apps = []
    for name in ["first", "second"]:
        chancy = Chancy("postgresql://localhost/test", prefix=name)
        chancy.plugins["test"] = Mock(
            api_plugin=lambda: "test.ContextApiPlugin"
        )
        worker = Worker(chancy, worker_id=name)
        apps.append(api.build_starlette_app(worker, chancy))

    for name, app in zip(["first", "second"], apps):
        with TestClient(app) as client:
            assert client.get("/context").json() == [name, name]
    assert ContextApiPlugin not in api.plugins


@pytest.mark.asyncio
async def test_standalone_server_uses_factory(
    api, chancy_just_app, monkeypatch
):
    app = Starlette()
    factory = Mock(return_value=app)
    monkeypatch.setattr(api, "build_starlette_app", factory)
    server = Mock(serve=AsyncMock())
    server_factory = Mock(return_value=server)
    monkeypatch.setattr("chancy.plugins.api.uvicorn.Server", server_factory)
    worker = Worker(chancy_just_app)

    await api.run(worker, chancy_just_app)

    factory.assert_called_once_with(worker, chancy_just_app)
    assert server_factory.call_args.kwargs["config"].app is app
    server.serve.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["retry", "purge", "cancel"])
async def test_batch_jobs_reports_each_committed_outcome(
    api, chancy, worker_no_start, monkeypatch, action
):
    await chancy.declare(Queue("default"))
    states = [
        "pending",
        "pending",
        "pending",
        "running",
        "succeeded",
        "retrying",
    ]
    refs = [await chancy.push(Job(func="unused")) for _ in states]
    ids = [str(ref.identifier) for ref in refs]
    async with chancy.pool.connection() as conn:
        for ref, state in zip(refs, states):
            await conn.execute(
                sql.SQL("UPDATE {} SET state = %s WHERE id = %s").format(
                    sql.Identifier(f"{chancy.prefix}jobs")
                ),
                [state, ref.identifier],
            )

    method = {
        "retry": "retry_jobs_ex",
        "purge": "purge_jobs_ex",
        "cancel": "cancel_job_ex",
    }[action]
    original = getattr(chancy, method)

    async def fail_one(cursor, references):
        result = await original(cursor, references)
        ref = references[0] if isinstance(references, list) else references
        if ref == refs[1]:
            # Even a failure after the write must roll back only this item.
            raise RuntimeError("Simulated item failure")
        return result

    monkeypatch.setattr(chancy, method, fail_one)
    notify = AsyncMock(wraps=chancy.notify)
    monkeypatch.setattr(chancy, "notify", notify)
    missing = "00000000-0000-0000-0000-000000000000"
    app = api.build_starlette_app(worker_no_start, chancy)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        login = await client.post(
            "/api/v1/login",
            json={"username": "admin", "password": "password"},
        )
        client.headers["Authorization"] = f"Bearer {login.json()['token']}"
        response = await client.post(
            "/api/v1/jobs",
            json={"action": action, "ids": [*ids, missing, "invalid", ids[0]]},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is False
    results = payload["results"]
    assert [result["id"] for result in results] == [*ids, missing, "invalid"]
    expected = [
        "completed",
        "failed",
        "completed",
        "completed",
        "completed",
        "completed",
        "skipped",
        "failed",
    ]
    if action == "retry":
        expected[3] = "skipped"
    elif action == "cancel":
        expected[4] = "skipped"
    assert [result["status"] for result in results] == expected
    assert all(
        result.get("message")
        for result in results
        if result["status"] != "completed"
    )
    assert "Simulated" not in response.text

    async with chancy.pool.connection() as conn:
        cursor = await conn.execute(
            sql.SQL("SELECT id, state FROM {}").format(
                sql.Identifier(f"{chancy.prefix}jobs")
            )
        )
        remaining = {
            str(job_id): state for job_id, state in await cursor.fetchall()
        }
    for job_id, state, status in zip(ids, states, expected):
        if status != "completed":
            assert remaining[job_id] == state
        elif action == "purge":
            assert job_id not in remaining
        else:
            assert (
                remaining[job_id]
                == {"retry": "retrying", "cancel": "failed"}[action]
            )
    if action == "purge":
        notify.assert_not_awaited()
    else:
        assert notify.await_count == expected.count("completed")
        assert all(
            call.args[1]
            == {"retry": "queue.pushed", "cancel": "job.cancelled"}[action]
            for call in notify.await_args_list
        )


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"action": "unknown", "ids": ["invalid"]},
        {"action": "retry", "ids": []},
        {"action": "retry", "ids": "invalid"},
        {"action": "retry", "ids": [None]},
    ],
)
def test_batch_jobs_rejects_invalid_payload(api, chancy_just_app, payload):
    app = api.build_starlette_app(Worker(chancy_just_app), chancy_just_app)
    with TestClient(app) as client:
        login = client.post(
            "/api/v1/login",
            json={"username": "admin", "password": "password"},
        )
        response = client.post(
            "/api/v1/jobs",
            json=payload,
            headers={"Authorization": f"Bearer {login.json()['token']}"},
        )
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["retry", "purge", "cancel"])
async def test_batch_jobs_success(api, chancy, worker_no_start, action):
    await chancy.declare(Queue("default"))
    ref = await chancy.push(Job(func="unused"))
    job_id = str(ref.identifier)
    app = api.build_starlette_app(worker_no_start, chancy)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        login = await client.post(
            "/api/v1/login",
            json={"username": "admin", "password": "password"},
        )
        response = await client.post(
            "/api/v1/jobs",
            json={"action": action, "ids": [job_id, job_id]},
            headers={"Authorization": f"Bearer {login.json()['token']}"},
        )
    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "results": [{"id": job_id, "status": "completed"}],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy", [{"plugins": [WorkflowPlugin()]}], indirect=True
)
@pytest.mark.parametrize("entity", ["jobs", "workflows"])
@pytest.mark.parametrize("pagination", [False, True])
@pytest.mark.parametrize("filtered", [False, True])
async def test_history_pagination_across_mutations(
    api, chancy, worker_no_start, entity, pagination, filtered
):
    table = sql.Identifier(f"{chancy.prefix}{entity}")
    field = "func" if entity == "jobs" else "name"
    now = datetime.datetime.now(datetime.UTC)
    rows = [
        (
            UUID(int=i),
            "other" if i % 7 == 0 else "match",
            # Dates oppose ID order and contain ties; workflow dates may be null.
            None
            if entity == "workflows" and i % 11 == 0
            else now - datetime.timedelta(days=i // 2),
        )
        for i in range(1, 252)
    ]
    insert = sql.SQL(
        "INSERT INTO {} (id, {}, created_at, state{}) "
        "VALUES (%s, %s, %s, 'pending'{})"
    ).format(
        table,
        sql.Identifier(field),
        sql.SQL(", queue") if entity == "jobs" else sql.SQL(""),
        sql.SQL(", 'default'") if entity == "jobs" else sql.SQL(""),
    )
    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.executemany(insert, rows)
    expected = [
        str(row[0])
        for row in reversed(rows)
        if not filtered or row[1] == "match"
    ]
    app = api.build_starlette_app(worker_no_start, chancy)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        login = await client.post(
            "/api/v1/login", json={"username": "admin", "password": "password"}
        )
        client.headers["Authorization"] = f"Bearer {login.json()['token']}"
        # Both endpoints cap oversized pages at 100.
        params = {"limit": 999}
        if pagination:
            params["pagination"] = "true"
        if filtered:
            params["filters"] = json.dumps([[field, "~", "match"]])
        seen = []
        while len(seen) < len(expected):
            response = await client.get(f"/api/v1/{entity}", params=params)
            assert response.status_code == 200
            payload = response.json()
            items = payload["items"] if pagination else payload
            ids = [item["id"] for item in items]
            assert ids == expected[len(seen) : len(seen) + 100]
            seen.extend(ids)
            if pagination:
                has_more = len(seen) < len(expected)
                assert payload["has_more"] is has_more
                assert payload["next_cursor"] == (ids[-1] if has_more else None)
            params["before"] = ids[-1]
            if len(seen) == 100:
                async with chancy.pool.connection() as conn:
                    # Removing the cursor row must not invalidate its boundary.
                    await conn.execute(
                        sql.SQL("DELETE FROM {} WHERE id = ANY(%s)").format(
                            table
                        ),
                        [[UUID(ids[-1]), UUID(int=1)]],
                    )
                    await conn.execute(insert, [UUID(int=1000), "match", now])
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET created_at = NOW(), state = 'failed'"
                        ).format(table)
                    )
                    await conn.execute(
                        sql.SQL(
                            "UPDATE {} SET {} = 'other' WHERE id = %s"
                        ).format(table, sql.Identifier(field)),
                        [UUID(int=2)],
                    )
                expected.remove(str(UUID(int=1)))
                if filtered:
                    expected.remove(str(UUID(int=2)))
        assert seen == expected
        assert len(seen) > 100
        assert len(set(seen)) == len(seen)
        response = await client.get(f"/api/v1/{entity}", params=params)
        assert response.json() == (
            {"items": [], "has_more": False, "next_cursor": None}
            if pagination
            else []
        )


@pytest.mark.parametrize("entity", ["jobs", "workflows"])
@pytest.mark.parametrize(
    "params",
    [
        {"limit": "0"},
        {"limit": "-1"},
        {"limit": "invalid"},
        {"before": "invalid"},
    ],
)
def test_history_rejects_invalid_pagination(
    api, chancy_just_app, entity, params
):
    api.plugins.add(WorkflowApiPlugin)
    app = api.build_starlette_app(Worker(chancy_just_app), chancy_just_app)
    with TestClient(app) as client:
        login = client.post(
            "/api/v1/login", json={"username": "admin", "password": "password"}
        )
        response = client.get(
            f"/api/v1/{entity}",
            params=params,
            headers={"Authorization": f"Bearer {login.json()['token']}"},
        )
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy", [{"plugins": [WorkflowPlugin()]}], indirect=True
)
@pytest.mark.parametrize("count", [0, 100, 101])
async def test_workflow_page_boundaries(api, chancy, worker_no_start, count):
    async with chancy.pool.connection() as conn, conn.cursor() as cursor:
        await cursor.executemany(
            sql.SQL(
                "INSERT INTO {} (id, name, state) VALUES (%s, 'test', 'pending')"
            ).format(sql.Identifier(f"{chancy.prefix}workflows")),
            [(UUID(int=i),) for i in range(1, count + 1)],
        )
    app = api.build_starlette_app(worker_no_start, chancy)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        login = await client.post(
            "/api/v1/login", json={"username": "admin", "password": "password"}
        )
        client.headers["Authorization"] = f"Bearer {login.json()['token']}"
        response = await client.get(
            "/api/v1/workflows", params={"pagination": "true"}
        )
        assert response.status_code == 200
        page = response.json()
        assert len(page["items"]) == min(count, 100)
        assert page["has_more"] is (count > 100)
        assert page["next_cursor"] == (
            str(UUID(int=2)) if count > 100 else None
        )
        if page["has_more"]:
            response = await client.get(
                "/api/v1/workflows",
                params={"pagination": "true", "before": page["next_cursor"]},
            )
            page = response.json()
            assert [item["id"] for item in page["items"]] == [str(UUID(int=1))]
            assert page["has_more"] is False
            assert page["next_cursor"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chancy", [{"plugins": [WorkflowPlugin()]}], indirect=True
)
@pytest.mark.parametrize("pagination", [False, True])
@pytest.mark.parametrize(
    "states",
    [
        ["succeeded"],
        ["pending", "running", "succeeded", "failed", "retrying"],
    ],
)
async def test_workflow_progress_counts_unsubmitted_steps(
    api, chancy, worker_no_start, pagination, states
):
    await chancy.declare(Queue("default"))
    workflow = Workflow("progress")
    for i in range(len(states) + 2):
        workflow.add(str(i), Job(func="unused"), [str(i - 1)] if i else [])
    await WorkflowPlugin.push(chancy, workflow)
    app = api.build_starlette_app(worker_no_start, chancy)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        login = await client.post(
            "/api/v1/login", json={"username": "admin", "password": "password"}
        )
        client.headers["Authorization"] = f"Bearer {login.json()['token']}"

        async def counts():
            response = await client.get(
                "/api/v1/workflows",
                params={"pagination": str(pagination).lower()},
            )
            assert response.status_code == 200
            items = response.json()["items"] if pagination else response.json()
            assert len(items) == 1
            return {
                key: value
                for key, value in items[0].items()
                if key.endswith("_steps")
            }

        expected = {
            "total_steps": len(workflow.steps),
            "waiting_steps": len(workflow.steps),
            **{
                f"{state}_steps": 0
                for state in [
                    "pending",
                    "running",
                    "succeeded",
                    "failed",
                    "retrying",
                ]
            },
        }
        assert await counts() == expected

        refs = []
        for i, state in enumerate(states):
            ref = await chancy.push(Job(func="unused"))
            refs.append(ref)
            async with chancy.pool.connection() as conn:
                await conn.execute(
                    sql.SQL(
                        "UPDATE {} SET job_id = %s WHERE workflow_id = %s AND step_id = %s"
                    ).format(sql.Identifier(f"{chancy.prefix}workflow_steps")),
                    [ref.identifier, workflow.id, str(i)],
                )
                await conn.execute(
                    sql.SQL("UPDATE {} SET state = %s WHERE id = %s").format(
                        sql.Identifier(f"{chancy.prefix}jobs")
                    ),
                    [state, ref.identifier],
                )
            expected[f"{state}_steps"] += 1
            expected["waiting_steps"] -= 1
        assert await counts() == expected
        assert expected["waiting_steps"] == 2
        assert expected["succeeded_steps"] < expected["total_steps"]

        # A purged job is not an unsubmitted step, and must not shrink the total.
        await chancy.purge_jobs([refs[0]])
        expected[f"{states[0]}_steps"] -= 1
        assert await counts() == expected


@pytest.mark.asyncio
async def test_metrics_range_contract(api, chancy, worker_no_start):
    metrics = chancy.plugins["chancy.metrics"]
    await metrics.increment_counter("custom", 2)
    await metrics.record_histogram_value("queue:test:time", 1.5, unit="seconds")
    await metrics.flush(chancy)
    app = api.build_starlette_app(worker_no_start, chancy)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        login = await client.post(
            "/api/v1/login", json={"username": "admin", "password": "password"}
        )
        client.headers["Authorization"] = f"Bearer {login.json()['token']}"
        overview = await client.get("/api/v1/metrics")
        assert overview.json()["categories"]["custom"] == [""]
        response = await client.get(
            "/api/v1/metrics/queue:test",
            params={"resolution": "5min", "range": 86400},
        )
        assert response.status_code == 200
        payload = response.json()
        assert datetime.datetime.fromisoformat(
            payload["end"]
        ) - datetime.datetime.fromisoformat(
            payload["start"]
        ) == datetime.timedelta(days=1)
        metric = payload["series"]["queue:test:time"]
        assert metric["summary"]["avg"] == 1.5
        assert metric["unit"] == "seconds"
        assert metric["aggregation"] == "summary"
        assert metric["data"][0]["sampled_at"] == metric["sampled_at"]
        for params in (
            {"range": "bad"},
            {"resolution": "bad"},
            {"limit": 0},
            {"start": "2026-10-08T12:00:00"},
        ):
            assert (
                await client.get("/api/v1/metrics/custom", params=params)
            ).status_code == 422
