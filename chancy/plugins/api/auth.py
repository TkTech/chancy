__all__ = ("AuthBackend", "SimpleAuthBackend")

import abc
from contextlib import suppress

import itsdangerous
from starlette.authentication import (
    AuthCredentials,
    AuthenticationBackend,
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
            # In token-only mode, SessionMiddleware may be absent. Best-effort set.
            with suppress(AssertionError):
                request.session["username"] = username  # type: ignore[attr-defined]
            return True
        return False

    async def logout(self, request: Request) -> None:
        with suppress(AssertionError):
            request.session.pop("username", None)  # type: ignore[attr-defined]

    async def authenticate(
        self, conn: HTTPConnection
    ) -> tuple[AuthCredentials, BaseUser] | None:
        try:
            username = conn.session.get("username")  # type: ignore[attr-defined]
        except AssertionError:
            return None
        if username is None:
            return None

        return AuthCredentials(["authenticated"]), SimpleUser(username)


class TokenAuthBackend(AuthBackend):
    """
    Pure bearer-token authentication backend.

    Expects an Authorization: Bearer <token> header with a token signed by
    itsdangerous using the API's secret key.
    """

    def __init__(self, secret_key: str, *, max_age: int = 60 * 60 * 24 * 7):
        self.serializer = itsdangerous.URLSafeTimedSerializer(
            secret_key, salt="chancy.api.token"
        )
        self.max_age = max_age

    async def login(
        self, request: Request, username: str, password: str
    ) -> bool:  # not used
        return False

    async def logout(self, request: Request) -> None:  # not used
        return None

    async def authenticate(
        self, conn: HTTPConnection
    ) -> tuple[AuthCredentials, BaseUser] | None:
        auth = conn.headers.get("authorization") or conn.headers.get(
            "Authorization"
        )
        if not auth or not auth.lower().startswith("bearer "):
            return None
        token = auth.split(" ", 1)[1].strip()
        try:
            data = self.serializer.loads(token, max_age=self.max_age)
            username = data.get("u")
            if not username:
                return None
            return AuthCredentials(["authenticated"]), SimpleUser(username)
        except itsdangerous.BadSignature:
            return None
