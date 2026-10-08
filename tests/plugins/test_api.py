from unittest.mock import AsyncMock, Mock
from urllib.parse import quote, urljoin

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from chancy import Chancy, Queue, Worker
from chancy.plugins.api import Api, SimpleAuthBackend, _SPAStaticFiles
from chancy.plugins.api.plugin import ApiPlugin


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
