"""Small OAuth2 client-credentials helper for Keycloak."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass
class ClientCredentialsTokenProvider:
    """Fetch and cache a Keycloak access token for a service client."""

    token_url: str
    client_id: str
    client_secret: str
    refresh_skew_seconds: int = 30

    _access_token: str | None = None
    _expires_at: float = 0.0

    def get_token(self) -> str:
        now = time.time()
        if self._access_token and now < self._expires_at - self.refresh_skew_seconds:
            return self._access_token

        payload = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
        ).encode("utf-8")
        request = Request(
            self.token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=10) as response:
                token_response = json.load(response)
        except Exception as exc:
            raise RuntimeError("Could not obtain an OAuth2 access token") from exc

        access_token = token_response.get("access_token")
        expires_in = token_response.get("expires_in", 300)
        if not isinstance(access_token, str) or not access_token:
            raise RuntimeError("Keycloak token response did not contain access_token")

        self._access_token = access_token
        self._expires_at = now + int(expires_in)
        return access_token
