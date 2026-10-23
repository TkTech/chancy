"""Generate every dashboard screenshot used by the documentation.

From the repository root, with PostgreSQL running:

    uv sync --extra web --extra cron --group screenshots
    uv run --group screenshots playwright install chromium
    npm --prefix chancy/plugins/api/ui ci
    npm --prefix chancy/plugins/api/ui run build
    uv run --extra web --extra cron --group screenshots python -m examples.screenshots

Starts and stops the demo automatically; stop any existing demo first. Uses
--dsn or CHANCY_DEMO_DSN, otherwise the repository's Compose database. Images
are generated from real activity after a 90-second warmup, not seeded metrics.
Outputs are ignored by Git. CI uploads them as the documentation-screenshots
artifact; download one locally with:

    gh run download RUN_ID -n documentation-screenshots -D docs/_static/screenshots
"""

import argparse
import asyncio
import json
import os
import re
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode

from playwright.async_api import Error, async_playwright, expect

from examples import demo

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "docs/_static/screenshots"
# Keep this inventory aligned with the image directives in docs/.
SCREENSHOTS = {
    "ux_jobs.png": (
        "/jobs?"
        + urlencode({"filters": json.dumps([["queue", "=", "notifications"]])}),
        "Jobs",
        1100,
    ),
    "ux_job_failed.png": ("/jobs/{job}", "Job", 1600),
    "ux_queue.png": ("/queues/orders", "Queue - orders", 1300),
    "ux_worker.png": ("/workers/{worker}", "Worker - {worker}", 1400),
    "ux_workflow.png": ("/workflows/{workflow}", "Workflow - {workflow}", 1700),
}


def check_inventory():
    referenced = set()
    sources = [
        *(ROOT / "docs").rglob("*.rst"),
        *(ROOT / "chancy").rglob("*.py"),
    ]
    for path in sources:
        if "_build" not in path.parts:
            referenced.update(
                re.findall(r"screenshots/(ux_[\w]+\.png)", path.read_text())
            )
    if referenced != SCREENSHOTS.keys():
        raise ValueError(
            f"Screenshot inventory differs from docs: {referenced ^ SCREENSHOTS.keys()}"
        )


async def capture(args, running):
    started = datetime.now(UTC)
    base = f"http://127.0.0.1:{args.port}"
    async with async_playwright() as playwright:
        request = await playwright.request.new_context(base_url=base)
        try:
            async with asyncio.timeout(60):
                while True:
                    if running.done():
                        running.result()
                        raise RuntimeError(
                            "Demo stopped before its API was ready"
                        )
                    try:
                        response = await request.get("/health", timeout=1000)
                        if response.ok:
                            break
                    except Error:
                        pass
                    await asyncio.sleep(1)
            response = await request.post(
                "/api/v1/login", data={"username": "demo", "password": "demo"}
            )
            if not response.ok:
                raise RuntimeError(f"Demo login failed: {response.status}")
            token = (await response.json())["token"]

            async def get(path, **params):
                response = await request.get(
                    path,
                    params=params,
                    headers={"Authorization": f"Bearer {token}"},
                )
                if not response.ok:
                    raise RuntimeError(f"{path}: HTTP {response.status}")
                return await response.json()

            print(f"Warming up demo for {args.warmup}s...", flush=True)
            done, _ = await asyncio.wait([running], timeout=args.warmup)
            if done:
                running.result()
                raise RuntimeError("Demo stopped during warmup")
            async with asyncio.timeout(120):
                while True:
                    if running.done():
                        running.result()
                        raise RuntimeError("Demo stopped before capture")
                    failed = await get(
                        "/api/v1/jobs",
                        state="failed",
                        queue="reports",
                        limit=1,
                    )
                    workflows = await get("/api/v1/workflows", limit=100)
                    workflow = next(
                        (w for w in workflows if w["state"] == "failed"), None
                    )
                    workers = await get("/api/v1/workers")
                    worker = next(
                        (
                            w
                            for w in workers
                            if "reports" in w["tags"]
                            and "reports" in w["queues"]
                            and datetime.fromisoformat(w["last_seen"])
                            >= started
                        ),
                        None,
                    )
                    metrics_ready = False
                    if worker:
                        metrics = await get(
                            "/api/v1/metrics/queue:reports",
                            worker_id=worker["worker_id"],
                            resolution="5min",
                        )
                        metrics_ready = all(
                            (
                                sample := metrics["series"].get(
                                    f"queue:reports:{name}"
                                )
                            )
                            and sample["sampled_at"]
                            and datetime.fromisoformat(sample["sampled_at"])
                            >= started
                            for name in (
                                "succeeded",
                                "failed",
                                "execution_time",
                            )
                        )
                    if failed and workflow and metrics_ready:
                        break
                    await asyncio.sleep(2)
            identifiers = {
                "job": failed[0]["id"],
                "workflow": workflow["id"],
                "worker": worker["worker_id"],
            }
            browser = await playwright.chromium.launch()
            try:
                page = await browser.new_page(
                    viewport={"width": 1440, "height": 1100},
                    device_scale_factor=1,
                    color_scheme="light",
                    reduced_motion="reduce",
                    locale="en-US",
                    timezone_id="UTC",
                )
                await page.add_init_script(
                    f"localStorage.setItem('auth.token:' + location.origin, {json.dumps(token)});"
                    "localStorage.setItem('chancy-theme', 'light');"
                )
                for filename, (route, heading, height) in SCREENSHOTS.items():
                    await page.set_viewport_size(
                        {"width": 1440, "height": height}
                    )
                    await page.goto(base + route.format(**identifiers))
                    await expect(
                        page.get_by_role(
                            "heading",
                            name=heading.format(**identifiers),
                            exact=True,
                        )
                    ).to_be_visible()
                    await page.wait_for_function(
                        "!document.querySelector('[aria-busy=true], "
                        ".chancy-spinner, .spinner-border')"
                    )
                    await page.evaluate("document.fonts.ready")
                    if filename == "ux_worker.png":
                        await expect(
                            page.get_by_text(
                                "Avg final attempt time", exact=True
                            ).first
                        ).to_be_visible()
                    if filename == "ux_workflow.png":
                        await expect(
                            page.get_by_role(
                                "button",
                                name=re.compile("Inspect step publish"),
                            )
                        ).to_be_visible()
                    # Let chart layout and JavaScript animations finish.
                    await page.wait_for_timeout(1000)
                    if await page.locator(".alert-danger").count():
                        raise RuntimeError(
                            f"Dashboard error on {route}: {await page.locator('.alert-danger').all_text_contents()}"
                        )
                    await page.screenshot(
                        path=OUTPUT / filename,
                        animations="disabled",
                    )
                    print(f"Captured {filename}", flush=True)
            finally:
                await browser.close()
        finally:
            await request.dispose()


async def run(args):
    check_inventory()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for filename in SCREENSHOTS:
        (OUTPUT / filename).unlink(missing_ok=True)
    running = asyncio.create_task(demo.run(args))
    try:
        await capture(args, running)
    finally:
        running.cancel()
        with suppress(asyncio.CancelledError):
            await running


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dsn",
        default=os.getenv(
            "CHANCY_DEMO_DSN",
            "postgresql://postgres:localtest@localhost:8190/postgres",
        ),
    )
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--warmup",
        type=int,
        default=90,
        help="seconds of real activity before capture",
    )
    parser.set_defaults(seed=42, duration=0)
    args = parser.parse_args()
    if args.warmup < 0:
        parser.error("--warmup must be nonnegative")
    demo.main(run(args))
