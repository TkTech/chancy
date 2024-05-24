import logging


logger = logging.getLogger("chancy")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class PrefixAdapter(logging.LoggerAdapter):
    """
    A logging adapter that prefixes log messages with a custom string.
    """

    def process(self, msg, kwargs):
        return f"[{self.extra['prefix']}] {msg}", kwargs
