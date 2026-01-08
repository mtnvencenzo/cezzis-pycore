"""Cezzis OAuth - A lightweight library for OAuth authentication and utilities."""

from cezzis_oauth.oauth_token_provider import IOAuthTokenProvider, OAuthTokenProvider
from cezzis_oauth.oauth_verification import OAuth2TokenVerifier, TokenVerificationError

# Dynamically read version from package metadata
try:
    from importlib.metadata import version

    __version__ = version("cezzis_oauth")
except Exception:
    __version__ = "unknown"

__all__ = [
    "IOAuthTokenProvider",
    "OAuthTokenProvider",
    "OAuth2TokenVerifier",
    "TokenVerificationError",
]
