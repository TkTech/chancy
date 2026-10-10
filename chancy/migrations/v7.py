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
        table = f"{migrator.prefix}leader"

        # conname is unique per-table, so scope the check to this table or a
        # different prefix's legacy constraint would match.
        await cursor.execute(
            """
            SELECT 1
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE c.conname = %s AND t.relname = %s
            """,
            (old_name, table),
        )
        if await cursor.fetchone():
            await cursor.execute(
                sql.SQL(
                    "ALTER TABLE {table} RENAME CONSTRAINT {old} TO {new}"
                ).format(
                    table=sql.Identifier(table),
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
        table = f"{migrator.prefix}leader"

        # conname is unique per-table, so scope the check to this table or a
        # different prefix's legacy constraint would match.
        await cursor.execute(
            """
            SELECT 1
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE c.conname = %s AND t.relname = %s
            """,
            (old_name, table),
        )
        if await cursor.fetchone():
            await cursor.execute(
                sql.SQL(
                    "ALTER TABLE {table} RENAME CONSTRAINT {old} TO {new}"
                ).format(
                    table=sql.Identifier(table),
                    old=sql.Identifier(old_name),
                    new=sql.Identifier(new_name),
                )
            )
