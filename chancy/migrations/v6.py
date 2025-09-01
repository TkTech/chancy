from psycopg import sql

from chancy.migrate import Migration


class QueueEagerPolling(Migration):
    """
    Adds the eager polling field to queues.
    """

    async def up(self, migrator, cursor):
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {queues} 
                ADD COLUMN IF NOT EXISTS eager_polling BOOLEAN NOT NULL DEFAULT FALSE
                """
            ).format(queues=sql.Identifier(f"{migrator.prefix}queues"))
        )

    async def down(self, migrator, cursor):
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {queues} 
                DROP COLUMN IF EXISTS eager_polling
                """
            ).format(queues=sql.Identifier(f"{migrator.prefix}queues"))
        )
