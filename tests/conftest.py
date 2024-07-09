from pytest_postgresql import factories


def run_chancy_migrations(host, port, user, dbname, password):
    """
    Bootstraps the database with the required Chancy migrations.
    """
    import asyncio

    from chancy.app import Chancy

    async def main():
        async with Chancy(
            dsn=f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        ) as app:
            await app.migrate()

    asyncio.run(main())


external_postgres = factories.postgresql_noproc(
    host="localhost",
    password="localtest",
    user="postgres",
    port=8190,
    load=[run_chancy_migrations],
)
postgresql = factories.postgresql(
    "external_postgres",
)
