import asyncio

import pytest
from psycopg import InterfaceError, OperationalError

from chancy import Worker


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [OperationalError, InterfaceError])
async def test_database_retry_limits_reset_and_independent_state(
    chancy_just_app, error_type
):
    worker = Worker(chancy_just_app, backoff_max_retries=2)
    attempts = []

    def delay(attempt):
        attempts.append(attempt)
        return 0

    worker._calculate_backoff = delay
    retry = worker.database_retry("test")
    other = worker.database_retry("other")
    error = error_type("disconnected")
    await retry.wait(error)
    await retry.wait(error)
    with pytest.raises(error_type) as raised:
        await retry.wait(error)
    assert raised.value is error
    assert attempts == [0, 1]
    assert other.failures == 0

    retry.reset()
    await retry.wait(error)
    assert attempts == [0, 1, 0]


@pytest.mark.asyncio
async def test_database_retry_propagates_non_transient_errors(chancy_just_app):
    retry = Worker(chancy_just_app).database_retry("test")
    for error in (ValueError("invalid data"), asyncio.CancelledError()):
        with pytest.raises(type(error)):
            await retry.wait(error)
    assert retry.failures == 0


@pytest.mark.asyncio
async def test_database_retry_sleep_is_cancellable(chancy_just_app):
    retry = Worker(chancy_just_app).database_retry("test")
    retry.calculate_delay = lambda attempt: 60
    task = asyncio.create_task(retry.wait(OperationalError("disconnected")))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [OperationalError, InterfaceError])
@pytest.mark.parametrize("max_retries", [None, 0])
async def test_database_retry_preserves_masked_cancellation(
    chancy_just_app, error_type, max_retries
):
    retry = Worker(
        chancy_just_app, backoff_initial=0, backoff_max_retries=max_retries
    ).database_retry("test")
    started = asyncio.Event()

    async def interrupted_operation():
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await retry.wait(
                error_type("cancellation masked by database error")
            )

    task = asyncio.create_task(interrupted_operation())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert retry.failures == 0


@pytest.mark.parametrize("initial", [0, 1])
def test_backoff_stays_capped_after_long_outages(chancy_just_app, initial):
    worker = Worker(chancy_just_app, backoff_initial=initial, backoff_max=60)
    assert 0 <= worker._calculate_backoff(100000) <= 60
