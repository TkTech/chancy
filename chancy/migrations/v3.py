from psycopg import sql

from chancy.migrate import Migration


class MakeMaxAttemptsNonNull(Migration):
    """
    Makes the max_attempts column non-null with a default value of 1, which
    matches the default value in the Job class.
    """

    async def up(self, migrator, cursor):
        # First set any existing NULL values to 1
        await cursor.execute(
            sql.SQL(
                """
                UPDATE {jobs}
                SET max_attempts = 1
                WHERE max_attempts IS NULL
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )

        # Then alter the column to be non-null with a default
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs}
                ALTER COLUMN max_attempts SET NOT NULL,
                ALTER COLUMN max_attempts SET DEFAULT 1
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )

    async def down(self, migrator, cursor):
        await cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {jobs}
                ALTER COLUMN max_attempts DROP NOT NULL,
                ALTER COLUMN max_attempts DROP DEFAULT
                """
            ).format(jobs=sql.Identifier(f"{migrator.prefix}jobs"))
        )
