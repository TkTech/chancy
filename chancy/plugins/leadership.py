from datetime import datetime, timezone, timedelta

from psycopg import sql
from psycopg import AsyncConnection

from chancy.plugin import Plugin, PluginScope
from chancy.app import Chancy
from chancy.worker import Worker


class Leadership(Plugin):
    """
    A plugin that manages cluster leadership.

    Leadership is used to ensure some plugins are run by only one worker at a
    time. For example, we wouldn't want every worker running database pruning
    every 60 seconds, as that would be immensely wasteful.

    This plugin is responsible for ensuring that only one worker is the leader
    at any given time. This plugin uses the {prefix}_leader table to store
    leadership information, which will only ever contain at most 1 row, the
    current leader.

    Events
    ------

    The following events are emitted by the leadership plugin:

    +---------------------+------------------------------------------------+
    | Event Name          | Description                                    |
    +=====================+================================================+
    | `leadership.gained` | Emitted when a worker has gained leadership.   |
    +---------------------+------------------------------------------------+
    | `leadership.lost`   | Emitted when a worker has lost leadership.     |
    +---------------------+------------------------------------------------+
    | `leadership.renewed`| Emitted when a worker has renewed an existing  |
    |                     | leadership.                                    |
    +---------------------+------------------------------------------------+

    The leadership plugin reacts to the following event:

    +---------------------+------------------------------------------------+
    | Event Name          | Description                                    |
    +=====================+================================================+
    | `worker.stopped`    | Emitted when a worker has stopped.             |
    +---------------------+------------------------------------------------+

    :param poll_interval: The number of seconds between leadership poll
                          intervals.
    :param timeout: The number of seconds before a worker is considered to have
                    lost leadership.
    """

    def __init__(self, poll_interval: int = 60, timeout: int = 60 * 2):
        super().__init__()
        self.poll_interval = poll_interval
        self.timeout = timeout

        if self.timeout < self.poll_interval:
            raise ValueError(
                "The timeout must be greater than the poll interval. Typically,"
                " the timeout should be at least twice the poll interval."
            )

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    async def run(self, worker: Worker, chancy: Chancy):
        prune_q = sql.SQL(
            """
            DELETE FROM {table} WHERE expires_at < %(now)s
            """
        ).format(table=sql.Identifier(f"{chancy.prefix}leader"))
        upsert_q = sql.SQL(
            """
            INSERT INTO {table} (id, worker_id, expires_at)
            VALUES (1, %(worker_id)s, %(expires_at)s)
            ON CONFLICT (id) DO UPDATE SET expires_at = %(expires_at)s
            """
        ).format(table=sql.Identifier(f"{chancy.prefix}leader"))
        insert_q = sql.SQL(
            """
            INSERT INTO {table} (id, worker_id, expires_at)
            VALUES (1, %(worker_id)s, %(expires_at)s) ON CONFLICT DO NOTHING
            """
        ).format(table=sql.Identifier(f"{chancy.prefix}leader"))

        # Trigger an immediate leadership check when a worker cleanly shuts
        # down.
        worker.hub.on(
            "worker.stopped",
            lambda worker_id: self.wakeup_signal.set(),
        )

        while await self.sleep(self.poll_interval):
            now = datetime.now(tz=timezone.utc)
            expires = now + timedelta(seconds=self.timeout)

            async with chancy.pool.connection() as conn:
                conn: AsyncConnection
                async with conn.cursor() as cur:
                    async with conn.transaction():
                        await cur.execute(prune_q, {"now": now})
                        if worker.is_leader.is_set():
                            await cur.execute(
                                upsert_q,
                                {
                                    "worker_id": worker.worker_id,
                                    "expires_at": expires,
                                },
                            )
                        else:
                            await cur.execute(
                                insert_q,
                                {
                                    "worker_id": worker.worker_id,
                                    "expires_at": expires,
                                },
                            )

                    is_leader = cur.rowcount == 1
                    was_leader = worker.is_leader.is_set()

                    if is_leader and was_leader:
                        self.log.debug(
                            f"Worker has renewed its leadership of the cluster"
                            f" until {expires}."
                        )
                        await chancy.notify(
                            cur,
                            "leadership.renewed",
                            {
                                "worker_id": worker.worker_id,
                            },
                        )
                    elif is_leader:
                        self.log.info(
                            f"Worker has become the leader of the cluster"
                            f" until {expires}."
                        )
                        worker.is_leader.set()
                        await chancy.notify(
                            cur,
                            "leadership.gained",
                            {
                                "worker_id": worker.worker_id,
                            },
                        )
                    elif was_leader:
                        self.log.info(
                            "Worker has lost leadership of the cluster."
                        )
                        worker.is_leader.clear()
                        await chancy.notify(
                            cur,
                            "leadership.lost",
                            {
                                "worker_id": worker.worker_id,
                            },
                        )
