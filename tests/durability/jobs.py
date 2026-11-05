import asyncio
from dataclasses import dataclass, field

from chancy import job


@dataclass
class FirstExecutionGate:
    """Hold the first execution; subsequent executions finish immediately."""

    started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    calls: int = 0


first_execution: FirstExecutionGate | None = None


@job()
async def hold_first_execution(*, fail_first: bool = False) -> None:
    gate = first_execution
    assert gate is not None, "The first_execution fixture must be active"
    gate.calls += 1
    if gate.calls == 1:
        gate.started.set()
        await gate.release.wait()
        if fail_first:
            raise ValueError("The original execution failed after recovery")


@job()
async def quick_job() -> str:
    return "done"
