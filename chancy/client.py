from psycopg import sql, AsyncCursor
from psycopg.types.json import Json

from chancy.app import Chancy, Queue
from chancy.job import Job


class Client:
    """
    A client for submitting jobs to the Chancy queue.
    """

    def __init__(self, app: Chancy):
        self.app = app
        self.jobs_table = sql.Identifier(f"{self.app.prefix}jobs")

    async def submit(self, job: Job, queue: Queue):
        """
        Submit a job to the specified queue.
        """
        async with self.app.pool.connection() as conn:
            async with conn.cursor() as cur:
                async with conn.transaction():
                    await self.submit_to_cursor(cur, job, queue)

    async def submit_to_cursor(
        self, cursor: AsyncCursor, job: Job, queue: Queue
    ):
        """
        Submit a job to the specified queue using the provided cursor.

        This method is useful for submitting multiple jobs in a single
        transaction or for ensuring your job is only inserted when a
        transaction the caller is managing is successful.
        """
        await cursor.execute(
            sql.SQL(
                """
                INSERT INTO {jobs} (
                    queue,
                    priority,
                    max_attempts,
                    payload
                )
                VALUES (
                    %(name)s,
                    %(priority)s,
                    %(max_attempts)s,
                    %(payload)s
                )
                """
            ).format(jobs=self.jobs_table),
            {
                "name": queue.name,
                "payload": Json(job.serialize()["payload"]),
                "priority": job.priority,
                "max_attempts": job.max_attempts,
            },
        )
        await cursor.execute(
            "SELECT pg_notify(%s, %s::text)",
            [
                f"{self.app.prefix}events",
                Json(
                    {
                        "event": "job_submitted",
                        "job": job.id,
                        "queue": queue.name,
                    }
                ),
            ],
        )
