from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow

from chancy.migrate import Migration, Migrator


class V7Migration(Migration):
    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        """
        Rename the leader_worker_id_unique constraint to use the prefix.

        This fixes an oversight where the constraint name was hardcoded
        instead of using the configurable prefix, which prevented running
        multiple Chancy instances with different prefixes in the same database.
        """
        old_name = "leader_worker_id_unique"
        new_name = f"{migrator.prefix}leader_worker_id_unique"

        # Check if the old unprefixed constraint exists
        await cursor.execute(
            """
            SELECT 1 FROM pg_constraint
            WHERE conname = %s
            """,
            (old_name,),
        )
        if await cursor.fetchone():
            await cursor.execute(
                sql.SQL(
                    "ALTER TABLE {table} RENAME CONSTRAINT {old} TO {new}"
                ).format(
                    table=sql.Identifier(f"{migrator.prefix}leader"),
                    old=sql.Identifier(old_name),
                    new=sql.Identifier(new_name),
                )
            )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        """
        Rename the constraint back to the unprefixed name.
        """
        old_name = f"{migrator.prefix}leader_worker_id_unique"
        new_name = "leader_worker_id_unique"

        # Check if the prefixed constraint exists
        await cursor.execute(
            """
            SELECT 1 FROM pg_constraint
            WHERE conname = %s
            """,
            (old_name,),
        )
        if await cursor.fetchone():
            await cursor.execute(
                sql.SQL(
                    "ALTER TABLE {table} RENAME CONSTRAINT {old} TO {new}"
                ).format(
                    table=sql.Identifier(f"{migrator.prefix}leader"),
                    old=sql.Identifier(old_name),
                    new=sql.Identifier(new_name),
                )
            )
