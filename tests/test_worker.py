import asyncio

import pytest

from chancy import Worker, Chancy


@pytest.mark.asyncio
async def test_worker(chancy: Chancy, worker: tuple[Worker, asyncio.Task]):
    """
    Ensure the worker can be started and stopped.
    """
