"""
Utilities for performing database migrations in a PostgreSQL database.

The functionality in this module is standalone and can be used with any
PostgreSQL database without any dependencies on the rest of the Chancy
project.
"""

import re
import abc
import importlib.resources

from psycopg import AsyncConnection, AsyncCursor
from psycopg import sql

VERSION_R = re.compile(r"v(\d+)\.py")


class MigrationError(Exception):
    """
    An error occurred while migrating the database schema.
    """


class Migration(abc.ABC):
    """
    A migration is a single unit of work that is applied to the database to
    bring it from one schema version to another.
    """

    @abc.abstractmethod
    async def up(self, migrator: "Migrator", cursor: AsyncCursor):
        pass

    @abc.abstractmethod
    async def down(self, migrator: "Migrator", cursor: AsyncCursor):
        pass


class Migrator:
    """
    A migrator is responsible for managing the database schema version and
    applying migrations to the database.

    :param key: A unique identifier for the application.
    :param migrations_package: The package where migrations are stored.
    :param prefix: A prefix to apply to all tables.
    """

    def __init__(self, key: str, migrations_package: str, *, prefix: str = ""):
        self.prefix = prefix
        self.key = key
        self.migrations_package = migrations_package

    def discover_all_migrations(self) -> list[Migration]:
        """
        Discovers all available migrations in the migrations package.

        Migrations are discovered by looking for classes that inherit from the
        `Migration` class.

        Migrations are sorted by their version number, which is the number at
        the beginning of the migration filename, ignoring the `v` prefix.

        For example, a migration file named `v1.py` would have a version number
        of 1.
        """
        migrations = []

        all_migrations = (
            resource
            for resource in importlib.resources.files(
                self.migrations_package
            ).iterdir()
            if resource.is_file()
        )

        for migration in all_migrations:
            if not VERSION_R.match(migration.name):
                continue

            module = importlib.import_module(
                f"{self.migrations_package}.{migration.name[:-3]}"
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
                            f"Multiple migrations found in {migration.name!r}"
                        )

                    migrations.append((int(migration.name[1:-3]), obj()))
                    found_migration = True

        return [migration for _, migration in sorted(migrations)]

    async def migrate(
        self, conn: AsyncConnection, to_version: int | None = None
    ) -> bool:
        """
        Migrate the database schema to the given version.

        This will migrate the database schema up or down as necessary to reach
        the given version. If `to_version` is less than the current schema
        version, the database will be migrated down. If `to_version` is greater
        than the current schema version, the database will be migrated up.

        If `to_version` is not provided, the database will be migrated to the
        highest available version.
        """
        migrations = self.discover_all_migrations()
        to_version = len(migrations) if to_version is None else to_version

        async with conn.cursor() as cursor:
            async with conn.transaction():
                current_version = await self.get_current_version(cursor)

                if current_version == to_version:
                    return False

                if to_version > len(migrations):
                    raise MigrationError(
                        f"Migration {to_version} does not exist for {self.key}"
                    )

                while current_version != to_version:
                    if current_version < to_version:
                        current_version += 1
                        await migrations[current_version - 1].up(self, cursor)
                        await self.set_current_version(cursor, current_version)
                    else:
                        await migrations[current_version - 1].down(self, cursor)
                        await self.set_current_version(
                            cursor, current_version - 1
                        )
                        current_version -= 1

        return True

    async def is_migration_required(self, cursor: AsyncCursor) -> bool:
        """
        Check if a newer schema version is available.
        """
        await self.upsert_version_table(cursor)
        current_version = await self.get_current_version(cursor)
        migrations = self.discover_all_migrations()
        return current_version < len(migrations)

    async def upsert_version_table(self, cursor: AsyncCursor):
        """
        Create the schema_version table if it doesn't exist.
        """
        await cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {prefix} (
                    version_of VARCHAR(255) PRIMARY KEY,
                    version INT
                )
                """
            ).format(prefix=sql.Identifier(f"{self.prefix}schema_version"))
        )

    async def get_current_version(self, cursor: AsyncCursor) -> int:
        """
        Get the current schema version from the database.
        """
        await self.upsert_version_table(cursor)
        await cursor.execute(
            sql.SQL(
                "SELECT version FROM {prefix} WHERE version_of = %s"
            ).format(prefix=sql.Identifier(f"{self.prefix}schema_version")),
            [self.key],
        )
        result = await cursor.fetchone()
        return 0 if result is None else result[0]

    async def set_current_version(self, cursor: AsyncCursor, version: int):
        """
        Set the current schema version in the database.

        .. note::

            This does not perform any sanity checks nor does it run any
            migrations. It simply sets the version in the database.
        """
        await self.upsert_version_table(cursor)
        await cursor.execute(
            sql.SQL(
                """
                INSERT INTO {prefix} (version_of, version) VALUES
                (%s, %s) ON CONFLICT (version_of) DO UPDATE SET
                version = EXCLUDED.version
                """
            ).format(prefix=sql.Identifier(f"{self.prefix}schema_version")),
            [self.key, version],
        )
