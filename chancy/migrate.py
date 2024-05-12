import abc
import importlib.resources

from psycopg import AsyncConnection
from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from chancy.logger import logger, PrefixAdapter


class Migration(abc.ABC):
    """
    A migration is a single unit of work that is applied to the database to
    bring it from one schema version to another.
    """

    @abc.abstractmethod
    async def up(self, migrator: "Migrator", conn: AsyncConnection):
        pass

    @abc.abstractmethod
    async def down(self, migrator: "Migrator", conn: AsyncConnection):
        pass


class Migrator:
    """
    The migrator is responsible for applying or reverting migrations to the
    database.

    Migrations can't fork - they're a simple linear progression from one
    version to the next.

    :param dsn: The database connection string.
    :param key: A unique identifier for the application.
    :param migrations_package: The package where migrations are stored.
    :param prefix: A prefix to apply to all tables.
    """

    def __init__(
        self, dsn: str, key: str, migrations_package: str, *, prefix: str = ""
    ):
        self.prefix = prefix
        self.key = key
        self.migrations_package = migrations_package
        self.pool = AsyncConnectionPool(
            dsn,
            open=False,
            check=AsyncConnectionPool.check_connection,
            min_size=1,
            max_size=1,
        )

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
            if migration.name == "__init__.py":
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
                            f"Multiple migrations found in {migration.name}"
                        )

                    migrations.append((int(migration.name[1:-3]), obj()))
                    found_migration = True

        return [migration for _, migration in sorted(migrations)]

    async def migrate(self, to_version: int | None = None):
        """
        Migrate the database schema to the given version.

        This will migrate the database schema up or down as necessary to reach
        the given version. If `to_version` is less than the current schema
        version, the database will be migrated down. If `to_version` is greater
        than the current schema version, the database will be migrated up.

        If `to_version` is not provided, the database will be migrated to the
        highest available version.

        If the database schema is already at the given version, no migration
        will be performed.
        """
        mig_logger = PrefixAdapter(logger, {"prefix": f"MIGRATOR:{self.key}"})

        async with self.pool.connection() as conn:
            current_version = await self.get_current_version(conn)
            migrations = self.discover_all_migrations()
            to_version = len(migrations) if to_version is None else to_version

            mig_logger.info(
                f"Current database schema version is {current_version}."
                f" Requested version is {to_version}."
                f" Highest available version is {len(migrations)}."
            )

            if current_version == to_version:
                mig_logger.info(
                    "Database schema is already at the requested version."
                )
                return

            if to_version > len(migrations):
                mig_logger.error(
                    "The requested migration version is higher than the "
                    "highest available version."
                )
                return

            mig_logger.info(
                f"Migrating database schema from {current_version} to"
                f" {to_version}."
            )

            while current_version != to_version:
                if current_version < to_version:
                    current_version += 1
                    mig_logger.info(f"Applying migration {current_version}.")
                    await migrations[current_version - 1].up(self, conn)
                    await conn.execute(
                        sql.SQL(
                            """
                            INSERT INTO {prefix} (version_of, version) VALUES
                            (%s, %s) ON CONFLICT (version_of) DO UPDATE SET
                            version = EXCLUDED.version
                            """
                        ).format(
                            prefix=sql.Identifier(
                                f"{self.prefix}schema_version"
                            )
                        ),
                        [self.key, current_version],
                    )
                else:
                    mig_logger.info(f"Reverting migration {current_version}.")
                    await migrations[current_version - 1].down(self, conn)
                    await conn.execute(
                        sql.SQL(
                            """
                            INSERT INTO {prefix} (version_of, version) VALUES
                            (%s, %s) ON CONFLICT (version_of) DO UPDATE SET
                            version = EXCLUDED.version
                            """
                        ).format(
                            prefix=sql.Identifier(
                                f"{self.prefix}schema_version"
                            )
                        ),
                        [self.key, current_version - 1],
                    )
                    current_version -= 1

            mig_logger.info("Migration complete.")

    async def is_migration_required(self, conn: AsyncConnection) -> bool:
        """
        Check if a newer schema version is available.
        """
        await self.upsert_version_table(conn)
        current_version = await self.get_current_version(conn)
        migrations = self.discover_all_migrations()
        return current_version < len(migrations)

    async def upsert_version_table(self, conn: AsyncConnection):
        """
        Create the schema_version table if it doesn't exist.
        """
        await conn.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {prefix} (
                    version_of VARCHAR(255) PRIMARY KEY,
                    version INT
                )
                """
            ).format(prefix=sql.Identifier(f"{self.prefix}schema_version"))
        )

    async def get_current_version(self, conn: AsyncConnection) -> int:
        """
        Get the current schema version from the database.
        """
        await self.upsert_version_table(conn)
        async with conn.cursor() as cursor:
            await cursor.execute(
                sql.SQL(
                    "SELECT version FROM {prefix} WHERE version_of = %s"
                ).format(prefix=sql.Identifier(f"{self.prefix}schema_version")),
                [self.key],
            )
            result = await cursor.fetchone()
            return 0 if result is None else result[0]

    async def __aenter__(self):
        await self.pool.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.pool.close()
        return False
