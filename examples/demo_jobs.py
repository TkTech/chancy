"""Synthetic jobs for the dashboard demo; no external services are contacted."""

import asyncio
import time

from chancy import QueuedJob


async def process_order(*, order: str, seconds: float = 1):
    await asyncio.sleep(seconds)


async def send_notification(
    *, context: QueuedJob, order: str, fail_until: int = 0
):
    await asyncio.sleep(0.3)
    if context.attempts < fail_until:
        raise ConnectionError(f"Notification provider unavailable for {order}")


async def import_catalogue(
    *,
    context: QueuedJob,
    stage: str,
    catalogue: str = "catalogue",
    seconds: float = 1,
    fail_until: int = 0,
):
    await asyncio.sleep(seconds)
    if context.attempts < fail_until:
        raise ValueError(f"{catalogue} {stage}: missing required SKU column")


def build_report(*, context: QueuedJob, report: str, seconds: float = 8):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        context.checkpoint()
        time.sleep(0.1)


def reconcile_inventory():
    time.sleep(0.3)


async def handle_inventory_change(*, context: QueuedJob):
    assert context.meta["trigger"]["operation"] == "UPDATE"
    await asyncio.sleep(0.2)
