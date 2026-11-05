from contextlib import suppress

from django.contrib.auth import aauthenticate
from starlette.authentication import AuthCredentials, BaseUser, SimpleUser
from starlette.requests import HTTPConnection, Request

from chancy.plugins.api import AuthBackend


class DjangoAuthBackend(AuthBackend):
    async def login(
        self, request: Request, username: str, password: str
    ) -> bool:
        user = await aauthenticate(username=username, password=password)
        if user is not None and getattr(user, "is_superuser", False):
            with suppress(AssertionError):
                request.session["username"] = username
            return True
        return False

    async def logout(self, request: Request) -> None:
        with suppress(AssertionError):
            request.session.pop("username", None)

    async def authenticate(
        self, conn: HTTPConnection
    ) -> tuple[AuthCredentials, BaseUser] | None:
        try:
            username = conn.session.get("username")
        except AssertionError:
            return None
        if username is not None:
            return AuthCredentials(["authenticated"]), SimpleUser(username)
        return None
