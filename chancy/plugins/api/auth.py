__all__ = ("SimpleAuthBackend", "AuthBackend")

import abc

from starlette.authentication import (
    AuthenticationBackend,
    AuthCredentials,
    BaseUser,
    SimpleUser,
)
from starlette.requests import HTTPConnection, Request


class AuthBackend(AuthenticationBackend, abc.ABC):
    @abc.abstractmethod
    async def login(
        self, request: Request, username: str, password: str
    ) -> bool:
        pass

    @abc.abstractmethod
    async def logout(self, request: Request) -> None:
        pass


class SimpleAuthBackend(AuthBackend):
    """
    A simple authentication backend that uses a dictionary of users and
    passwords.

    :param users: A dictionary of users and their passwords.
    """

    def __init__(self, users: dict[str, str]):
        self.users = users

    async def login(
        self, request: Request, username: str, password: str
    ) -> bool:
        if username in self.users and self.users[username] == password:
            request.session["username"] = username
            return True
        return False

    async def logout(self, request: Request) -> None:
        request.session.pop("username", None)

    async def authenticate(
        self, conn: HTTPConnection
    ) -> tuple[AuthCredentials, BaseUser] | None:
        username = conn.session.get("username")
        if username is None:
            return None

        return AuthCredentials(["authenticated"]), SimpleUser(username)
