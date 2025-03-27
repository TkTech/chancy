import abc
import typing
from typing import Optional


class RouteT(typing.TypedDict):
    """
    A type hint for a route.
    """

    path: str
    endpoint: typing.Callable
    methods: str | None
    name: str | None
    is_websocket: Optional[bool]


class ApiPlugin(abc.ABC):
    """
    A plugin that provides additional API endpoints.
    """

    def __init__(self, api):
        self.api = api

    @abc.abstractmethod
    def name(self) -> str:
        """
        Get the name of the plugin.
        """

    def routes(self) -> list[RouteT]:
        """
        Get a list of routes to add to the API.
        """
        return []
