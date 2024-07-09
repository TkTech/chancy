import pytest

from chancy.app import Chancy, Queue


def test_queue_names(postgresql):
    """
    Ensure a ValueError is raised when duplicate or invalid queue names are
    provided.
    """
    with pytest.raises(ValueError):
        Chancy(
            dsn=postgresql.info.dsn,
            plugins=[
                Queue("test"),
                Queue("test"),
            ],
        )
