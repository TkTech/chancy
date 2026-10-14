import pytest

from chancy import Chancy


@pytest.mark.django_db
@pytest.mark.asyncio
async def test_django_style_connection(settings, test_prefix):
    async with Chancy(settings.DATABASES["default"], prefix=test_prefix) as app:
        await app.migrate()
        await app.migrate(to_version=0)
