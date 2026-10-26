"""The credentials endpoint: what a container's SDK fetches, and who may fetch it.

Served on loopback here (botocore accepts it there as it does on the network's gateway),
and read back by botocore's own container provider, so the format is the one the SDK
really parses."""
import json
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import pytest

from cascade.runners.credentials import (
    CREDENTIALS_PATH,
    CredentialsEndpoint,
    CredentialsError,
    session_source,
)

BODY = {
    "AccessKeyId": "AKIDTEST",
    "SecretAccessKey": "secret",
    "Token": None,
    "Expiration": "2099-01-01T00:00:00+00:00",
}


def _get(url, token=None):
    request = urllib.request.Request(url, headers={"Authorization": token} if token else {})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as e:
        return e.code, None


def test_the_sdk_gets_the_served_credentials_with_the_token():
    from botocore.credentials import ContainerProvider

    with CredentialsEndpoint("127.0.0.1", lambda: BODY) as served:
        provider = ContainerProvider(environ=served.env())
        frozen = provider.load().get_frozen_credentials()
    assert (frozen.access_key, frozen.secret_key, frozen.token) == ("AKIDTEST", "secret", None)
    assert served.uri.endswith(CREDENTIALS_PATH)


def test_nothing_without_the_token_or_off_the_path():
    with CredentialsEndpoint("127.0.0.1", lambda: BODY) as served:
        assert _get(served.uri) == (403, None)
        assert _get(served.uri, token="wrong") == (403, None)
        assert _get(served.uri.replace(CREDENTIALS_PATH, "/"), token=served.token)[0] == 404
        assert _get(served.uri, token=served.token) == (200, BODY)


def test_each_endpoint_has_its_own_token():
    with CredentialsEndpoint("127.0.0.1", lambda: BODY) as a:
        with CredentialsEndpoint("127.0.0.1", lambda: BODY) as b:
            assert a.token != b.token
            assert _get(a.uri, token=b.token)[0] == 403


def test_the_server_stops_with_the_block():
    with CredentialsEndpoint("127.0.0.1", lambda: BODY) as served:
        pass
    with pytest.raises(urllib.error.URLError):
        _get(served.uri, token=served.token)


def test_a_machine_without_credentials_fails_on_entry():
    def none():
        raise CredentialsError("no AWS credentials")

    with pytest.raises(CredentialsError):
        with CredentialsEndpoint("127.0.0.1", none):
            pass


# --- from a boto session -------------------------------------------------------

class _Frozen:
    access_key, secret_key, token = "AKID", "s", "t"


class _Static:
    def get_frozen_credentials(self):
        return _Frozen()


class _Expiring(_Static):
    def __init__(self, expiry):
        self._expiry_time = expiry


class _Session:
    def __init__(self, credentials):
        self.credentials = credentials

    def get_credentials(self):
        return self.credentials


def _expiration(body):
    return datetime.fromisoformat(body["Expiration"]) - datetime.now(timezone.utc)


def test_static_keys_are_served_for_an_hour():
    """The SDK refreshes 15 minutes ahead of an expiry: a short one would mean a fetch
    per call."""
    body = session_source(_Session(_Static()))()
    assert (body["AccessKeyId"], body["SecretAccessKey"], body["Token"]) == ("AKID", "s", "t")
    assert timedelta(minutes=59) < _expiration(body) <= timedelta(hours=1)


def test_expiring_credentials_are_served_with_their_own_expiry():
    soon = datetime.now(timezone.utc) + timedelta(minutes=20)
    body = session_source(_Session(_Expiring(soon)))()
    assert datetime.fromisoformat(body["Expiration"]) == soon


def test_a_session_without_credentials_is_an_error():
    with pytest.raises(CredentialsError, match="no AWS credentials"):
        session_source(_Session(None))()
