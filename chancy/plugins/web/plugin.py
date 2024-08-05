import abc

import jinja2
from starlette.requests import Request

from chancy.plugin import RouteT


class WebPlugin(abc.ABC):
    """
    A plugin that provides additional views to the web interface.
    """

    def __init__(self, environment: jinja2.Environment):
        self.environment = environment

    @abc.abstractmethod
    def name(self) -> str:
        """
        Get the name of the plugin.
        """

    def links(self) -> list[tuple[str, str]]:
        """
        Returns a list of [(label, path)] tuples to add to the web interface's
        primary navigation.
        """
        return []

    def routes(self) -> list[RouteT]:
        """
        Get a list of routes to add to the web interface.
        """
        return []

    async def render(
        self, template_name: str, request: Request, /, **context
    ) -> str:
        """
        Render a template with the given context.
        """

        def url_for(name, **kwargs):
            return request.url_for(name, **kwargs)

        return await self.environment.get_template(
            f"{self.name()}/{template_name}"
        ).render_async(
            {
                **context,
                "plugin": self,
                "url_for": url_for,
                "request": request,
            }
        )
