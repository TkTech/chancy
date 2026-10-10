import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from psycopg import AsyncConnection, sql


@pytest.mark.asyncio
async def test_notify_many_preserves_individual_payloads_and_order(chancy):
    # The total exceeds PostgreSQL's per-notification payload limit. Each
    # individual notification must retain its own payload and event name.
    events = [(f"event.{i}", {"data": "x" * 100}) for i in range(100)]
    async with await AsyncConnection.connect(
        chancy.dsn, autocommit=True
    ) as listener:
        await listener.execute(
            sql.SQL("LISTEN {}").format(
                sql.Identifier(f"{chancy.prefix}events")
            )
        )
        async with chancy.pool.connection() as conn, conn.cursor() as cursor:
            execute = AsyncMock(wraps=cursor.execute)
            await chancy.notify_many(SimpleNamespace(execute=execute), events)
            execute.assert_awaited_once()

        received = [
            json.loads(message.payload)
            async for message in listener.notifies(timeout=5, stop_after=100)
        ]
    assert received == [{"t": name, **payload} for name, payload in events]


@pytest.mark.asyncio
async def test_notifications_are_discarded_on_rollback(chancy):
    async with await AsyncConnection.connect(
        chancy.dsn, autocommit=True
    ) as listener:
        await listener.execute(
            sql.SQL("LISTEN {}").format(
                sql.Identifier(f"{chancy.prefix}events")
            )
        )
        with pytest.raises(RuntimeError):
            async with (
                chancy.pool.connection() as conn,
                conn.cursor() as cursor,
            ):
                await chancy.notify_many(cursor, [("rolled_back", {})])
                raise RuntimeError("abort transaction")

        async with chancy.pool.connection() as conn, conn.cursor() as cursor:
            await chancy.notify(cursor, "committed", {"value": 1})
        received = [
            json.loads(message.payload)
            async for message in listener.notifies(timeout=5, stop_after=1)
        ]
    assert received == [{"t": "committed", "value": 1}]


@pytest.mark.asyncio
async def test_notify_many_skips_empty_and_disabled_batches(chancy_just_app):
    execute = AsyncMock()
    cursor = SimpleNamespace(execute=execute)
    await chancy_just_app.notify_many(cursor, [])
    chancy_just_app.notifications = False
    await chancy_just_app.notify_many(cursor, [("ignored", {})])
    await chancy_just_app.notify(cursor, "ignored", {})
    execute.assert_not_awaited()
