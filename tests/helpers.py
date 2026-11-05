import asyncio
from collections.abc import Callable


async def wait_until(
    predicate: Callable[[], bool], *, timeout: float = 15
) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)
