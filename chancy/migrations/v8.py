from psycopg import sql

from chancy.migrate import Migration


class JobClaims(Migration):
    """Fence worker updates to the execution that claimed the job."""

    async def up(self, migrator, cursor):
        await cursor.execute(
            sql.SQL("ALTER TABLE {jobs} ADD COLUMN claim_id UUID").format(
                jobs=sql.Identifier(f"{migrator.prefix}jobs")
            )
        )

    async def down(self, migrator, cursor):
        await cursor.execute(
            sql.SQL("ALTER TABLE {jobs} DROP COLUMN claim_id").format(
                jobs=sql.Identifier(f"{migrator.prefix}jobs")
            )
        )
