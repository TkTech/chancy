from psycopg import sql, AsyncCursor
from psycopg.rows import DictRow

from chancy.job import COMPLETED_STATES
from chancy.migrate import Migration, Migrator


class AddJobDeadline(Migration):
    """
    Adds a deadline column to the jobs table and updates the jobs_unique_key index
    to include the 'expired' state in the exclusion list.
    """

    async def up(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        # Add the deadline column
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs} ADD COLUMN deadline TIMESTAMP WITH TIME ZONE
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )

        # Drop the existing unique index
        await cursor.execute(
            sql.SQL("DROP INDEX IF EXISTS {jobs_unique_key}").format(
                jobs_unique_key=sql.Identifier(
                    f"{migrator.prefix}jobs_unique_key"
                )
            )
        )

        # Create the updated index including 'expired' state
        await cursor.execute(
            sql.SQL(
                """
                CREATE UNIQUE INDEX {jobs_unique_key}
                ON {jobs} (unique_key)
                WHERE
                    unique_key IS NOT NULL
                        AND state != ANY({terminal_states})
                """
            ).format(
                jobs_unique_key=sql.Identifier(
                    f"{migrator.prefix}jobs_unique_key"
                ),
                jobs=sql.Identifier(f"{migrator.prefix}jobs"),
                terminal_states=sql.Literal(list(COMPLETED_STATES)),
            )
        )

    async def down(self, migrator: Migrator, cursor: AsyncCursor[DictRow]):
        # Drop the deadline column
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs} DROP COLUMN deadline
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )

        # Restore the original index condition (without 'expired')
        await cursor.execute(
            sql.SQL("DROP INDEX IF EXISTS {jobs_unique_key}").format(
                jobs_unique_key=sql.Identifier(
                    f"{migrator.prefix}jobs_unique_key"
                )
            )
        )

        await cursor.execute(
            sql.SQL(
                """
                CREATE UNIQUE INDEX {jobs_unique_key}
                ON {jobs} (unique_key)
                WHERE
                    unique_key IS NOT NULL
                        AND state NOT IN ('succeeded', 'failed');
                """
            ).format(
                jobs_unique_key=sql.Identifier(
                    f"{migrator.prefix}jobs_unique_key"
                ),
                jobs=sql.Identifier(f"{migrator.prefix}jobs"),
            )
        )
