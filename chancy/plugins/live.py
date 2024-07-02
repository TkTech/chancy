import json

from psycopg import AsyncConnection
from psycopg import sql

from chancy.app import Chancy
from chancy.worker import Worker
from chancy.plugins.plugin import Plugin, PluginScope
from chancy.logger import logger, PrefixAdapter


class Live(Plugin):
    """
    Enables support for Postgres LISTEN/NOTIFY events.

    This plugin improves the reactivity of a worker by allowing it to almost
    immediately react to database events.

    For example, using this lets you wake up a worker as soon as a new job is
    pushed to the database, typically taking a couple of milliseconds to pick up
    a job instead of the default polling interval of 5s.

    .. note::

        Unlike most plugins, the Live plugin will use its own persistent
        connection to the database instead of using the shared connection pool.
    """

    @classmethod
    def get_scope(cls) -> PluginScope:
        return PluginScope.WORKER

    async def run(self, worker: Worker, chancy: Chancy):
        log = PrefixAdapter(logger, {"prefix": "Live"})

        connection = await AsyncConnection.connect(chancy.dsn, autocommit=True)
        await connection.execute(
            sql.SQL("LISTEN {channel};").format(
                channel=sql.Identifier(f"{chancy.prefix}events")
            )
        )

        log.info("Started listening for live notifications.")

        async for notification in connection.notifies():
            log.debug(f"Received notification: {notification.payload}")
            j = json.loads(notification.payload)

            match j["t"]:
                case "job_pushed":
                    await worker[j["q"]].wake_up()
