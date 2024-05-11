import logging
from rich.logging import RichHandler
from rich.markup import escape


logger = logging.getLogger("chancy")
logger.setLevel(logging.DEBUG)
logger.addHandler(RichHandler(markup=True))


class PrefixAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        prefix = f"[{self.extra["prefix"]}]"
        return f"[blue]{escape(prefix)}[/blue] {msg}", kwargs
