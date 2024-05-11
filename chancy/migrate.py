"""
Implements a simple linear migrator for PostgreSQL databases.

We can't use a full-featured migrator like Alembic because we don't want to
introduce dependencies on SQLAlchemy or other libraries that are likely to
conflict with the user's own dependencies. So we implement a simple migrator
that applies and reverts migrations in order.

Migrations are defined as classes that inherit from the `Migration` class and
implement the `up` and `down` methods. The `up` method should apply the
migration, and the `down` method should revert it.

Migrations are stored in the `chancy.migrations` package, and are discovered
automatically by the migrator.
"""

import abc
import importlib.resources

from psycopg import AsyncConnection
from psycopg import sql

from chancy.app import Chancy
from chancy.logger import logger, PrefixAdapter


class Migration(abc.ABC):
    """
    A migration is a single unit of work that is applied to the database to
    bring it from one schema version to another.
    """

    @abc.abstractmethod
    async def up(self, app: Chancy, conn: AsyncConnection):
        pass

    @abc.abstractmethod
    async def down(self, app: Chancy, conn: AsyncConnection):
        pass


def highest_allowed_version(app: Chancy) -> int:
    """
    Return the highest allowed schema version for the given app.
    """
    return max(app.allowed_schema_versions)


class Migrator:
    """
    The migrator is responsible for applying or reverting migrations to the
    database.

    The migrator stores the current schema version in a table called
    `schema_version` in the Chancy prefix.

    Migrations can't fork - they're a simple linear progression from one
    version to the next.
    """

    def __init__(self, app: Chancy):
        self.app = app

    @staticmethod
    def discover_all_migrations() -> list[Migration]:
        """
        Discovers all available migrations in the `chancy.migrations` package.

        Migrations are discovered by looking for classes that inherit from the
        `Migration` class in the `chancy.migrations` package.

        Migrations are sorted by their version number, which is the number at
        the beginning of the migration filename, ignoring the `v` prefix.

        For example, a migration file named `v1.py` would have a version number
        of 1.
        """
        migrations = []

        all_migrations = (
            resource
            for resource in importlib.resources.files(
                "chancy.migrations"
            ).iterdir()
            if resource.is_file()
        )

        for migration in all_migrations:
            if migration.name == "__init__.py":
                continue

            module = importlib.import_module(
                f"chancy.migrations.{migration.name[:-3]}"
            )
            found_migration = False
            for name in dir(module):
                obj = getattr(module, name)
                if (
                    isinstance(obj, type)
                    and issubclass(obj, Migration)
                    and obj != Migration
                ):
                    if found_migration:
                        raise ValueError(
                            f"Multiple migrations found in {migration.name}"
                        )

                    migrations.append((int(migration.name[1:-3]), obj()))
                    found_migration = True

        return [migration for _, migration in sorted(migrations)]

    async def migrate(self):
        """
        A shortcut for migrating the database from the current version to the
        latest version.
        """
        mig_logger = PrefixAdapter(logger, {"prefix": "MIGRATOR"})

        conn = await AsyncConnection.connect(self.app.postgres, autocommit=True)

        if not await self.is_migration_required(conn):
            mig_logger.info("No migration required.")
            return

        mig_logger.info("A migration is required, preparing to migrate.")

        current_version = await self.get_current_version(conn)
        highest_version = highest_allowed_version(self.app)
        migrations = self.discover_all_migrations()

        mig_logger.info(f"Current schema version: {current_version}")
        mig_logger.info(f"Found {len(migrations)} migration(s).")
        mig_logger.info(f"Highest allowed version: {highest_version}")

        if current_version > highest_version:
            mig_logger.error(
                "The current database schema is newer than the application "
                "allows. This is likely due to switching to an older version "
                "of the application without downgrading the schema first."
            )
            return

        if current_version == highest_version:
            mig_logger.info("Database schema is up to date.")
            return

        mig_logger.info("Migrating database schema.")
        for version, migration in enumerate(migrations, 1):
            if current_version < version <= highest_version:
                mig_logger.info(f"Applying migration {version}.")
                await migration.up(self.app, conn)
                await conn.execute(
                    sql.SQL(
                        "INSERT INTO {prefix} (version) VALUES (%s)"
                    ).format(
                        prefix=sql.Identifier(
                            f"{self.app.prefix}schema_version"
                        )
                    ),
                    [version],
                )
                current_version = version

    async def is_migration_required(self, conn: AsyncConnection) -> bool:
        """
        Check the current schema version and determine if a migration is
        required.

        This does _not_ mean that the current schema is the most recent schema,
        only that it is compatible with the application's allowed schema
        versions.
        """
        await self.upsert_version_table(conn)

        current_version = await self.get_current_version(conn)
        if current_version not in self.app.allowed_schema_versions:
            return True

        return False

    async def upsert_version_table(self, conn: AsyncConnection):
        """
        Create the schema_version table if it doesn't exist.
        """
        await conn.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {prefix} (
                    version INT PRIMARY KEY
                )
                """
            ).format(prefix=sql.Identifier(f"{self.app.prefix}schema_version"))
        )

    async def get_current_version(self, conn: AsyncConnection) -> int:
        """
        Get the current schema version from the database.
        """
        async with conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL("SELECT version FROM {prefix}").format(
                    prefix=sql.Identifier(f"{self.app.prefix}schema_version")
                )
            )
            result = await cursor.fetchone()
            return 0 if result is None else result[0]
