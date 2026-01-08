import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Protocol

import httpx
from authlib.integrations.httpx_client import AsyncOAuth2Client

class IOAuthTokenProvider(Protocol):
    """Protocol for OAuth token providers."""

    async def get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary."""
        ...


class OAuthTokenProvider(IOAuthTokenProvider):
    """
    Provides OAuth access tokens using OAuth 2.0 client credentials flow.
    Handles token caching and automatic refresh.
    """

    def __init__(
        self,
        domain: str,
        client_id: str,
        client_secret: str,
        audience: str,
        scope: str
    ):
        self._domain = domain
        self._client_id = client_id
        self._client_secret = client_secret
        self._audience = audience
        self._scope = scope
        self._logger = logging.getLogger(__name__)
        self._token: str | None = None
        self._token_expiry: datetime | None = None
        self._lock = asyncio.Lock()

    async def get_access_token(self) -> str:
        """
        Get a valid access token. Fetches a new token if none exists or if expired.

        Returns:
            str: A valid OAuth access token.

        Raises:
            RuntimeError: If token acquisition fails.
        """
        async with self._lock:
            # Check if we have a valid cached token
            if self._token and self._token_expiry:
                # Refresh 60 seconds before expiry to avoid edge cases
                if datetime.now(timezone.utc) < (self._token_expiry - timedelta(seconds=60)):
                    return self._token

            # Fetch new token
            self._token = await self._fetch_token()
            return self._token

    async def _fetch_token(self) -> str:
        """
        Fetch a new access token from OAuth using client credentials flow.

        Returns:
            str: The access token.

        Raises:
            RuntimeError: If token acquisition fails.
        """
        token_url = f"https://{self._domain}/oauth/token"

        try:
            async with AsyncOAuth2Client(
                client_id=self._client_id,
                client_secret=self._client_secret,
            ) as client:
                token = await client.fetch_token(
                    token_url,
                    grant_type="client_credentials",
                    audience=self._audience,
                    scope=self._scope,
                )

                # Cache token and expiry
                access_token: str = token["access_token"]
                self._token = access_token
                expires_in = token.get("expires_in", 3600)  # Default to 1 hour
                self._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

                self._logger.info(f"Successfully fetched OAuth access token, expires in {expires_in} seconds")
                return access_token

        except httpx.HTTPError as e:
            self._logger.error(f"HTTP error fetching OAuth token: {e}")
            raise RuntimeError(f"Failed to fetch OAuth access token: {e}") from e
        except Exception as e:
            self._logger.error(f"Unexpected error fetching OAuth token: {e}")
            raise RuntimeError(f"Failed to fetch OAuth access token: {e}") from e
