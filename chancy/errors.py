class Retry(Exception):
    """
    Raised when the user wants to retry the Job.
    """

    def __init__(self):
        pass


class TryAgainLater(Exception):
    """
    Similar to a :class:`Retry`, but this exception is used when the user
    does not want the retry to count against the job's maximum attempts.

    Very useful for things like fetching reports from an API that may not be
    ready yet.
    """

    def __init__(self):
        pass
