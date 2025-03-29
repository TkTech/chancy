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
    """
    Base class for authentication backends.
    """

    @abc.abstractmethod
    async def login(
        self, request: Request, username: str, password: str
    ) -> bool:
        """
        Authenticate a user, setting any necessary session data.

        :param request: The request object.
        :param username: The username of the user.
        :param password: The password of the user.
        :return: True if the user is authenticated, False otherwise.
        """

    @abc.abstractmethod
    async def logout(self, request: Request) -> None:
        """
        Log out a user.

        :param request: The request object.
        """


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
