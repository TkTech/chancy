from django.contrib.auth import aauthenticate
from django.contrib.auth.models import User
from starlette.authentication import AuthCredentials, BaseUser, SimpleUser
from starlette.requests import Request, HTTPConnection

from chancy.plugins.api import AuthBackend


class DjangoAuthBackend(AuthBackend):
    async def login(
        self, request: Request, username: str, password: str
    ) -> bool:
        user: User | None = await aauthenticate(
            username=username, password=password
        )
        if user is not None and user.is_superuser:
            try:
                request.session["username"] = username
            except Exception:
                pass
            return True
        return False

    async def logout(self, request: Request) -> None:
        try:
            request.session.pop("username", None)
        except Exception:
            pass

    async def authenticate(
        self, conn: HTTPConnection
    ) -> tuple[AuthCredentials, BaseUser] | None:
        username = conn.session.get("username")
        if username is not None:
            return AuthCredentials(["authenticated"]), SimpleUser(username)
        return None
