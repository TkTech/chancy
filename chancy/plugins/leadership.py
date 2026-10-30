import asyncio

from psycopg import sql

from chancy.plugin import Plugin
from chancy.session import OperationContext, PluginSession


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
    - Successful checks send ``leadership.gained``, ``leadership.lost``, or
      ``leadership.renewed`` notifications when applicable.
    - Connection failures and lease expiry clear the local flag without a
      database notification. Leader-only transactions verify the lease again.
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

        if (
            self.poll_interval <= 0
            or self.timeout <= 0
            or self.timeout < self.poll_interval
        ):
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

    async def setup(self, session: PluginSession):
        deadline = [0.0]
        changed = asyncio.Event()

        async def renew(ctx: OperationContext):
            worker, chancy = ctx.worker, ctx.chancy

            async def update(tx):
                started = asyncio.get_running_loop().time()
                await tx.cursor.execute(
                    sql.SQL(
                        "DELETE FROM {} WHERE expires_at < clock_timestamp()"
                    ).format(sql.Identifier(f"{chancy.prefix}leader"))
                )
                await tx.cursor.execute(
                    sql.SQL(
                        """
                        INSERT INTO {table} (id, worker_id, expires_at)
                        VALUES (1, %s, clock_timestamp() + %s * INTERVAL '1 second')
                        ON CONFLICT (id) DO UPDATE
                        SET expires_at = EXCLUDED.expires_at
                        WHERE {table}.worker_id = EXCLUDED.worker_id
                        RETURNING expires_at
                        """
                    ).format(table=sql.Identifier(f"{chancy.prefix}leader")),
                    [worker.worker_id, self.timeout],
                )
                lease = await tx.cursor.fetchone()
                was_leader = worker.is_leader.is_set()
                event = (
                    (
                        "leadership.renewed"
                        if was_leader
                        else "leadership.gained"
                    )
                    if lease
                    else "leadership.lost"
                )
                if lease or was_leader:
                    await tx.notify(event, {"worker_id": worker.worker_id})
                return lease, started

            lease, started = await ctx.database(update)
            if (
                lease
                and asyncio.get_running_loop().time() < started + self.timeout
            ):
                deadline[0] = started + self.timeout
                changed.set()
                worker.is_leader.set()
                chancy.log.debug(
                    "Worker has renewed its leadership of the cluster until %s.",
                    lease["expires_at"],
                )
            else:
                worker.is_leader.clear()

        async def expire(ctx):
            try:
                while True:
                    changed.clear()
                    remaining = deadline[0] - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        ctx.worker.is_leader.clear()
                        await changed.wait()
                    else:
                        try:
                            await asyncio.wait_for(changed.wait(), remaining)
                        except TimeoutError:
                            ctx.worker.is_leader.clear()
            finally:
                ctx.worker.is_leader.clear()

        operation = session.periodic(
            "renew", renew, interval=self.poll_interval
        )
        session.service("expire", expire)
        session.subscribe("worker.stopped", lambda event: operation.wake_up())


class ImmediateLeadership(Plugin):
    """
    A plugin that simply sets the worker as the leader immediately upon startup.

    This plugin is only ever intended for testing purposes, and should not be
    used in a production environment.
    """

    @staticmethod
    def get_identifier() -> str:
        return "chancy.leadership"

    async def setup(self, session: PluginSession):
        session.worker._leadership_unfenced = True
        session.worker.is_leader.set()
        session.callback(session.worker.is_leader.clear)
        session.callback(setattr, session.worker, "_leadership_unfenced", False)
        session.worker.chancy.log.warning(
            "Worker has been set as leader without going through election."
            " This plugin should only be used for testing & debugging."
        )
