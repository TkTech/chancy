"""Worker-owned leadership leases and transaction fencing."""

import asyncio
import math
import time
from typing import TYPE_CHECKING

from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow, dict_row

from chancy.errors import LeadershipLost

if TYPE_CHECKING:
    from chancy.worker import Worker


class LeadershipLease:
    """One worker's view of the cluster lease; PostgreSQL is authoritative."""

    def __init__(
        self,
        worker: "Worker",
        *,
        eligible: bool,
        poll_interval: float,
        timeout: float,
        idle_timeout: int | None,
    ):
        if (
            not math.isfinite(poll_interval)
            or not math.isfinite(timeout)
            or poll_interval <= 0
            or timeout <= poll_interval
        ):
            raise ValueError(
                "Leadership intervals must be finite and positive, with "
                "leadership_timeout > leadership_poll_interval"
            )
        self.worker = worker
        self.eligible = eligible
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.idle_timeout = idle_timeout
        self._elected = asyncio.Event()
        self._changed = asyncio.Event()
        self._deadline = 0.0

    @property
    def is_leader(self) -> bool:
        return (
            self.eligible
            and self._elected.is_set()
            and time.monotonic() < self._deadline
            and not self.worker.shutdown_event.is_set()
        )

    async def wait(self) -> None:
        while not self.is_leader:
            self.clear()
            await self._elected.wait()

    def clear(self) -> None:
        if self._elected.is_set():
            self.worker.chancy.log.info(
                "Worker has lost leadership of the cluster."
            )
        self._elected.clear()
        self._deadline = 0.0
        self._changed.set()

    async def maintain(self) -> None:
        try:
            while True:
                await self.renew()
                await asyncio.sleep(self.poll_interval)
        finally:
            self.clear()

    async def expire(self) -> None:
        # Expire local state even while renewal is blocked.
        try:
            while True:
                self._changed.clear()
                remaining = self._deadline - time.monotonic()
                if remaining <= 0:
                    self.clear()
                    self._changed.clear()
                    await self._changed.wait()
                else:
                    try:
                        await asyncio.wait_for(self._changed.wait(), remaining)
                    except TimeoutError:
                        self.clear()
        finally:
            self.clear()

    async def renew(self) -> None:
        if not self.eligible or self.worker.shutdown_event.is_set():
            return
        chancy = self.worker.chancy
        table = sql.Identifier(f"{chancy.prefix}leader")
        retry = self.worker.database_retry("leadership renewal")
        while True:
            try:
                # Start before any database wait: our local expiry must be no
                # later than the lease expiry computed by PostgreSQL.
                started = time.monotonic()
                was_leader = self.is_leader
                async with (
                    chancy.pool.connection() as conn,
                    conn.transaction(),
                    conn.cursor(row_factory=dict_row) as cursor,
                ):
                    await cursor.execute(
                        sql.SQL(
                            "DELETE FROM {} WHERE expires_at <= clock_timestamp()"
                        ).format(table)
                    )
                    await cursor.execute(
                        sql.SQL(
                            """
                            INSERT INTO {table} (id, worker_id, expires_at)
                            VALUES (1, %s, clock_timestamp() + %s * INTERVAL '1 second')
                            ON CONFLICT (id) DO UPDATE
                            SET expires_at = EXCLUDED.expires_at
                            WHERE {table}.worker_id = EXCLUDED.worker_id
                            RETURNING expires_at
                            """
                        ).format(table=table),
                        [self.worker.worker_id, self.timeout],
                    )
                    lease = await cursor.fetchone()
                    if lease or was_leader:
                        if lease:
                            event = (
                                "leadership.renewed"
                                if was_leader
                                else "leadership.gained"
                            )
                        else:
                            event = "leadership.lost"
                        await chancy.notify(
                            cursor, event, {"worker_id": self.worker.worker_id}
                        )
                break
            except retry.errors as exc:
                await retry.wait(exc)

        if (
            lease
            and time.monotonic() < started + self.timeout
            and not self.worker.shutdown_event.is_set()
        ):
            was_leader = self.is_leader
            self._deadline = started + self.timeout
            self._elected.set()
            self._changed.set()
            if not was_leader:
                chancy.log.info("Worker has become the leader of the cluster.")
            chancy.log.debug(
                "Worker has renewed its leadership until %s.",
                lease["expires_at"],
            )
        else:
            self.clear()

    async def guard(self, cursor: AsyncCursor[DictRow]) -> None:
        if not self.eligible or self.worker.shutdown_event.is_set():
            raise LeadershipLost()
        if self.idle_timeout is not None:
            await cursor.execute(
                """
                SELECT set_config(
                    'idle_in_transaction_session_timeout',
                    LEAST(NULLIF(setting::bigint, 0), %s)::text,
                    true
                )
                FROM pg_settings
                WHERE name = 'idle_in_transaction_session_timeout'
                """,
                [self.idle_timeout * 1000],
            )
            if await cursor.fetchone() is None:
                raise RuntimeError(
                    "Database does not expose idle_in_transaction_session_timeout; "
                    "set leadership_idle_timeout=None to use its existing policy"
                )
        # Hold the lease against takeover until this work commits.
        await cursor.execute(
            sql.SQL(
                "SELECT worker_id, expires_at FROM {} WHERE id = 1 FOR KEY SHARE"
            ).format(sql.Identifier(f"{self.worker.chancy.prefix}leader"))
        )
        lease = await cursor.fetchone()
        await cursor.execute("SELECT clock_timestamp() AS now")
        clock = await cursor.fetchone()
        assert clock is not None
        if (
            lease is None
            or lease["worker_id"] != self.worker.worker_id
            or lease["expires_at"] <= clock["now"]
        ):
            self.clear()
            raise LeadershipLost()
