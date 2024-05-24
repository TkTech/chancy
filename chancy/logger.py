import logging


logger = logging.getLogger("chancy")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter(
        fmt="%(asctime)s • %(levelname)s • %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
logger.addHandler(handler)


class PrefixAdapter(logging.LoggerAdapter):
    """
    A logging adapter that prefixes log messages with a custom string.
    """

    def process(self, msg, kwargs):
        return f"{self.extra['prefix']} • {msg}", kwargs
