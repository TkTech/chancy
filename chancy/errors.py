class MigrationsNeededError(Exception):
    """
    Raised when the database is out of date.
    """


class LeadershipLost(Exception):
    """The worker no longer owns a valid leadership lease."""
