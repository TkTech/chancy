import pytest

from chancy.plugins.api import Api, SimpleAuthBackend
from chancy.plugins.cron import Cron


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                Cron(),
                Api(
                    authentication_backend=SimpleAuthBackend(
                        {"test_user": "test_password"}
                    )
                ),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_plugin_autostart_disabled(chancy, worker):
    """
    Test that plugin autostart configuration works.
    """
    assert "Cron" in worker.manager
    assert "Api" not in worker.manager


@pytest.mark.parametrize(
    "chancy",
    [
        {
            "plugins": [
                Cron(),
                Api(
                    authentication_backend=SimpleAuthBackend(
                        {"test_user": "test_password"}
                    ),
                    autostart=True,
                ),
            ],
            "no_default_plugins": True,
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_plugin_autostart_enabled(chancy, worker):
    """
    Test that plugin autostart configuration works.
    """
    assert "Cron" in worker.manager
    assert "Api" in worker.manager
