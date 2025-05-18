from datetime import datetime, timezone, timedelta

from psycopg import sql
from psycopg.rows import dict_row

from chancy.plugin import Plugin, PluginScope
from chancy.app import Chancy
from chancy.worker import Worker


class Leadership(Plugin):
    """
    Leadership is used to ensure some plugins are run by only one worker at a
    time. For example, we wouldn't want every worker running database pruning
    every 60 seconds, as that would be immensely wasteful. Most other plugins
    require a Leadership plugin to be enabled.

    .. note::

        This plugin is enabled by default, you only need to provide it in the
        list of plugins to customize its arguments or if ``no_default_plugins``
        is set to ``True``.

    .. code-block:: python

        from chancy.plugins.leadership import Leadership

        async with Chancy(..., plugins=[
            Leadership()
        ]) as chancy:
            ...

    Implementation
    --------------
    The plugin implements a simple database-backed leader election system
    through a 'leader' table with the following characteristics:

    - Uses a single row with ID=1 to track the current leader worker.
    - Workers attempt to insert/update this row with their worker_id and an
      expiration timestamp.
    - Leadership is acquired when a worker successfully inserts the row (when
      no leader exists) or when the previous leader's entry has expired.
    - The leader renews its leadership by updating its expiration timestamp
      during each poll cycle.
    - If leadership renewal fails, the worker loses its leader status.
    - When a worker gains or loses leadership, appropriate notifications are
      sent via ``leadership.gained``, ``leadership.lost``, and
      ``leadership.renewed`` events.
    - Worker leadership status is tracked via a worker.is_leader event flag.

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

    @staticmethod
    def get_identifier() -> str:
        return "chancy.leadership"

    def get_tables(self) -> list[str]:
        """Get the names of all tables this plugin is responsible for."""
        return ["leader"]

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
                async with conn.cursor(row_factory=dict_row) as cur:
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
                        chancy.log.debug(
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
                        chancy.log.info(
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
                        chancy.log.info(
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


class ImmediateLeadership(Plugin):
    """
    A plugin that simply sets the worker as the leader immediately upon startup.

    This plugin is only ever intended for testing purposes, and should not be
    used in a production environment.
    """

    @staticmethod
    def get_identifier() -> str:
        return "chancy.leadership"

    async def run(self, worker: Worker, chancy: Chancy):
        worker.is_leader.set()
        chancy.log.warning(
            "Worker has been set as leader without going through election."
            " This plugin should only be used for testing & debugging."
        )
