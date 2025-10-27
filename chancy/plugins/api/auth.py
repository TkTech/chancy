__all__ = ("SimpleAuthBackend", "AuthBackend")

import abc

from starlette.authentication import (
    AuthenticationBackend,
    AuthCredentials,
    BaseUser,
    SimpleUser,
)
from starlette.requests import HTTPConnection, Request
import itsdangerous


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
            try:
                request.session["username"] = username  # type: ignore[attr-defined]
            except Exception:
                pass
            return True
        return False

    async def logout(self, request: Request) -> None:
        try:
            request.session.pop("username", None)  # type: ignore[attr-defined]
        except Exception:
            pass

    async def authenticate(
        self, conn: HTTPConnection
    ) -> tuple[AuthCredentials, BaseUser] | None:
        username = None
        try:
            username = conn.session.get("username")  # type: ignore[attr-defined]
        except Exception:
            pass
        if username is None:
            return None

        return AuthCredentials(["authenticated"]), SimpleUser(username)


class TokenOrSessionAuthBackend(AuthBackend):
    """
    Authentication backend that accepts either a signed bearer token in the
    Authorization header or falls back to a wrapped session-based backend.

    Tokens are signed with itsdangerous using the API's secret key and include
    the username. They are intended for short-lived UI sessions and remote use.
    """

    def __init__(
        self,
        wrapped: AuthBackend,
        secret_key: str,
        *,
        max_age: int = 60 * 60 * 24 * 7,
    ):
        self.wrapped = wrapped
        self.serializer = itsdangerous.URLSafeTimedSerializer(
            secret_key, salt="chancy.api.token"
        )
        self.max_age = max_age

    def _generate(self, username: str) -> str:
        return self.serializer.dumps({"u": username})

    def _verify(self, token: str) -> str | None:
        try:
            data = self.serializer.loads(token, max_age=self.max_age)
            return data.get("u")
        except itsdangerous.BadSignature:
            return None

    async def login(
        self, request: Request, username: str, password: str
    ) -> bool:
        return await self.wrapped.login(request, username, password)

    async def logout(self, request: Request) -> None:
        await self.wrapped.logout(request)

    async def authenticate(
        self, conn: HTTPConnection
    ) -> tuple[AuthCredentials, BaseUser] | None:
        auth = conn.headers.get("authorization") or conn.headers.get(
            "Authorization"
        )
        if auth and auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
            user = self._verify(token)
            if user:
                return AuthCredentials(["authenticated"]), SimpleUser(user)

        # Fallback to wrapped backend (session-based)
        return await self.wrapped.authenticate(conn)


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
