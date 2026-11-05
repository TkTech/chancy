from psycopg import OperationalError

from chancy.job import QueuedJob


class MigrationsNeededError(Exception):
    """
    Raised when the database is out of date.
    """


class LeadershipLost(Exception):
    """The worker no longer owns a valid leadership lease."""


class UnconfirmedClaims(OperationalError):
    """A claim transaction ended without a confirmed commit or rollback."""

    def __init__(self, jobs: list[QueuedJob]):
        super().__init__("Could not confirm claimed jobs")
        self.jobs = jobs
