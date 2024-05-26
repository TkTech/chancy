class Plugin:
    @classmethod
    def is_leader_only(cls) -> bool:
        """
        Return True if this plugin should only be run on the leader node.
        Defaults to True.
        """
        return True
