from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx


class Proxy:
    def __init__(self, client: httpx.AsyncClient, name: str):
        self.client = client
        self.path = f"/proxies/{name}"
        self.name = name

    async def create(self, port: int) -> None:
        response = await self.client.post(
            "/proxies",
            json={
                "name": self.name,
                "listen": f"0.0.0.0:{port}",
                "upstream": "db:5432",
            },
        )
        response.raise_for_status()

    async def delete(self) -> None:
        (await self.client.delete(self.path)).raise_for_status()

    async def enable(self, enabled: bool = True) -> None:
        response = await self.client.post(self.path, json={"enabled": enabled})
        response.raise_for_status()

    @asynccontextmanager
    async def disconnected(self) -> AsyncIterator[None]:
        await self.enable(False)
        try:
            yield
        finally:
            await self.enable()

    @asynccontextmanager
    async def toxic(
        self, kind: str, *, stream: str = "downstream", **attributes: float
    ) -> AsyncIterator[None]:
        name = f"{kind}_{stream}"
        response = await self.client.post(
            f"{self.path}/toxics",
            json={
                "name": name,
                "type": kind,
                "stream": stream,
                "toxicity": 1,
                "attributes": attributes,
            },
        )
        response.raise_for_status()
        try:
            yield
        finally:
            response = await self.client.delete(f"{self.path}/toxics/{name}")
            response.raise_for_status()
            if kind == "timeout":
                # Dropped responses leave existing connections unusable.
                await self.enable(False)
                await self.enable()

    async def restore(self) -> None:
        response = await self.client.get(f"{self.path}/toxics")
        response.raise_for_status()
        for toxic in response.json():
            response = await self.client.delete(
                f"{self.path}/toxics/{toxic['name']}"
            )
            response.raise_for_status()
        await self.enable()
