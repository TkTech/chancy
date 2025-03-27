from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from starlette.requests import Request

from chancy.plugins.api import AuthBackend


class DjangoAuthBackend(AuthBackend):
    async def login(
        self, request: Request, username: str, password: str
    ) -> bool:
        user: User | None = authenticate(username=username, password=password)
        if user is not None and user.is_superuser:
            request.session["username"] = username
            return True
        return False

    async def logout(self, request: Request) -> None:
        request.session.pop("username", None)
