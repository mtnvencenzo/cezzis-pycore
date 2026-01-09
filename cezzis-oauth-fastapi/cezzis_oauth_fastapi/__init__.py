"""Cezzis OAuth FastAPI - A lightweight library for OAuth authentication and utilities for fastapi."""

from cezzis_oauth_fastapi.oauth_authorization import oauth_authorization
from cezzis_oauth_fastapi.oauth_config import OAuthConfig

# Dynamically read version from package metadata
try:
    from importlib.metadata import version

    __version__ = version("cezzis_oauth_fastapi")
except Exception:
    __version__ = "unknown"

__all__ = [
    "oauth_authorization",
    "OAuthConfig",
]
