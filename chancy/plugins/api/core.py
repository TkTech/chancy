from chancy.plugins.api.plugin import ApiPlugin


class CoreWebPlugin(ApiPlugin):
    def name(self):
        return "base"

    def routes(self):
        return []
