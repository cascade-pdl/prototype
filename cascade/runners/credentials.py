"""Credentials for containers, served the way ECS serves a task its role.

On ECS a container has no keys of its own: the agent sets
``AWS_CONTAINER_CREDENTIALS_RELATIVE_URI``, and every AWS SDK fetches temporary
credentials from that local endpoint, refreshing them before they expire. A local docker
run does the same, so the container takes the identical path: for the run's lifetime the
executor serves *the credentials it uses for the store itself* (its boto session: env,
profile, SSO), and containers are given ``AWS_CONTAINER_CREDENTIALS_FULL_URI`` and
``AWS_CONTAINER_AUTHORIZATION_TOKEN``. Nothing is mounted, and a task that outlives
temporary credentials keeps working, because the SDK comes back for fresh ones.

botocore takes a plain-http credentials URI only on loopback or ECS's own addresses, so
the server binds to the cascade network's gateway (``169.254.170.2``, the host's side of
the bridge; see ``CONTAINER_NETWORK``) and nowhere else, and answers only requests that
carry the run's random token.
"""
from __future__ import annotations

import json
import secrets
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable


CREDENTIALS_PATH = "/credentials"

STATIC_LIFETIME = timedelta(hours=1)
"""The expiry served with credentials that have none (static keys, e.g. garage's). The
SDK refreshes 15 minutes before an expiry, so this must be longer, or every call would
fetch again."""


class CredentialsError(Exception):
    """This machine has no credentials to serve."""


@dataclass(frozen=True)
class ContainerCredentials:
    """Where a container fetches its credentials, and the token that lets it."""

    uri: str
    token: str

    def env(self) -> dict[str, str]:
        return {
            "AWS_CONTAINER_CREDENTIALS_FULL_URI": self.uri,
            "AWS_CONTAINER_AUTHORIZATION_TOKEN": self.token,
        }


Source = Callable[[], dict[str, Any]]
"""Returns one response body: ``AccessKeyId``, ``SecretAccessKey``, ``Token`` and
``Expiration`` (ISO 8601), the container-credentials format."""


def session_source(session=None) -> Source:
    """The credentials of a boto session (by default the one the store client uses),
    refreshed by boto as they near expiry."""
    if session is None:
        import boto3

        session = boto3.Session()

    def source() -> dict[str, Any]:
        credentials = session.get_credentials()
        if credentials is None:
            raise CredentialsError(
                "no AWS credentials on this machine to give containers for the store "
                "(environment, profile or SSO)"
            )
        frozen = credentials.get_frozen_credentials()
        ceiling = datetime.now(timezone.utc) + STATIC_LIFETIME
        # only refreshable credentials expire; boto keeps the time privately
        expiry = getattr(credentials, "_expiry_time", None)
        return {
            "AccessKeyId": frozen.access_key,
            "SecretAccessKey": frozen.secret_key,
            "Token": frozen.token,
            "Expiration": min(expiry, ceiling).isoformat() if expiry else ceiling.isoformat(),
        }

    return source


class CredentialsEndpoint:
    """An HTTP server on ``host`` serving ``source`` to holders of a random token, for as
    long as the ``with`` block runs. Entering it fetches once, so a machine with no
    credentials fails before anything starts."""

    def __init__(self, host: str, source: Source | None = None):
        self.host = host
        self.source = source or session_source()
        self.token = secrets.token_urlsafe(32)
        self._server: ThreadingHTTPServer | None = None

    def __enter__(self) -> ContainerCredentials:
        self.source()
        self._server = ThreadingHTTPServer((self.host, 0), _handler(self.source, self.token))
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        host, port = self._server.server_address[:2]
        return ContainerCredentials(uri=f"http://{host}:{port}{CREDENTIALS_PATH}", token=self.token)

    def __exit__(self, *exc) -> None:
        assert self._server is not None
        self._server.shutdown()
        self._server.server_close()
        self._server = None


def _handler(source: Source, token: str) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != CREDENTIALS_PATH:
                return self._reply(404, {"message": "not found"})
            given = self.headers.get("Authorization", "")
            if not secrets.compare_digest(given.encode(), token.encode()):
                return self._reply(403, {"message": "forbidden"})
            try:
                body = source()
            except Exception as e:  # the container sees a failure, the operator why
                print(f"cascade: serving credentials failed: {e}", file=sys.stderr)
                return self._reply(500, {"message": "no credentials"})
            self._reply(200, body)

        def _reply(self, status: int, body: dict[str, Any]) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format, *args) -> None:  # one line per fetch is noise
            pass

    return Handler
