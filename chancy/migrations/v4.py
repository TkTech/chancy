from psycopg import sql

from chancy.migrate import Migration


class AddResumeAtField(Migration):
    """
    Adds a resume_at datetime to the queues table.
    """

    async def up(self, migrator, cursor):
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {queues}
                ADD COLUMN resume_at TIMESTAMP WITH TIME ZONE
                """
            ).format(queues=sql.Identifier(f"{migrator.prefix}queues"))
        )

    async def down(self, migrator, cursor):
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {queues}
                DROP COLUMN resume_at
                """
            ).format(queues=sql.Identifier(f"{migrator.prefix}queues"))
        )
