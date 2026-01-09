import logging
import os
from functools import wraps
from typing import Callable, Union

from cezzis_oauth import (
    OAuth2TokenVerifier,
    TokenVerificationError,
)
from fastapi import HTTPException, Request

from cezzis_oauth_fastapi.oauth_config import OAuthConfig

_logger = logging.getLogger("oauth_authorization")

# Cache verifiers by config to avoid recreating them
_verifiers: dict[tuple, OAuth2TokenVerifier] = {}


def _get_config_key(config: OAuthConfig) -> tuple:
    """Create a hashable key from OAuthConfig for caching verifiers."""
    return (config.domain, config.audience, tuple(config.algorithms), config.issuer)


def oauth_authorization(scopes: list[str], config_provider: Callable[[], OAuthConfig]):
    """Decorator for OAuth2 authorization with OAuth token verification.

    Can be applied to:
    - Individual endpoint methods (async functions)
    - APIRouter classes (all methods will be protected)

    Args:
        scopes: List of required OAuth scopes.
        config_provider: A callable that returns an OAuthConfig instance.

    Example:
        # On a method
        @oauth_authorization(scopes=["write:embeddings"], config_provider=get_oauth_config)
        async def get_cocktails(self, _rq: Request):
            ...

        # On a class (protects all methods)
        @oauth_authorization(scopes=["admin:cocktails"], config_provider=get_oauth_config)
        class AdminRouter(APIRouter):
            ...

        # Without scope verification
        @oauth_authorization(scopes=[], config_provider=get_oauth_config)
        async def public_endpoint(self, _rq: Request):
            ...
    """
    required_scopes = scopes or []

    def decorator(target: Union[Callable, type]) -> Union[Callable, type]:
        # Check if target is a class
        if isinstance(target, type):
            return _wrap_class(target, required_scopes, config_provider())
        else:
            return _wrap_function(target, required_scopes, config_provider())

    return decorator


def _wrap_function(func: Callable, required_scopes: list[str], oauth_config: OAuthConfig) -> Callable:
    """Wrap an individual async function with OAuth authorization."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        global _verifiers

        # Extract the Request object from kwargs
        request: Request | None = kwargs.get("_rq")

        if request is None:
            _logger.error("Request object not found in function arguments")
            raise HTTPException(status_code=500, detail="Internal server error")

        # Check if authorization should be bypassed in local environment
        if os.getenv("ENV") == "local":
            _logger.info("OAuth authorization bypassed in local environment")
            return await func(*args, **kwargs)

        # Extract token from Authorization header
        auth_header = request.headers.get("Authorization", "")
        if not auth_header or not auth_header.startswith("Bearer "):
            _logger.warning("Missing or invalid Authorization header")
            raise HTTPException(status_code=401, detail="Missing or invalid authorization token")

        token = auth_header.replace("Bearer ", "").strip()

        try:
            # Get or create verifier for this config
            config_key = _get_config_key(oauth_config)
            if config_key not in _verifiers:
                _verifiers[config_key] = OAuth2TokenVerifier(
                    domain=oauth_config.domain,
                    audience=oauth_config.audience,
                    algorithms=oauth_config.algorithms,
                    issuer=oauth_config.issuer,
                )

            verifier = _verifiers[config_key]
            payload = await verifier.verify_token(token)

            # Verify scopes if required
            if required_scopes:
                verifier.verify_scopes(payload, required_scopes)

            _logger.info(f"OAuth authorization successful for subject: {payload.get('sub', 'unknown')}")

        except TokenVerificationError as e:
            _logger.warning(f"OAuth authorization failed: {e}")
            raise HTTPException(status_code=403, detail=str(e))
        except Exception as e:
            _logger.error(f"Unexpected error during OAuth authorization: {e}")
            raise HTTPException(status_code=500, detail="Authorization error")

        # Call the original function
        return await func(*args, **kwargs)

    return wrapper


def _wrap_class(cls: type, required_scopes: list[str], oauth_config: OAuthConfig) -> type:
    """Wrap all async methods in a class with OAuth authorization."""

    # Get all methods from the class
    for attr_name in dir(cls):
        # Skip private/magic methods and non-callables
        if attr_name.startswith("_"):
            continue

        attr = getattr(cls, attr_name)

        # Check if it's a callable method (not a property or static attribute)
        if callable(attr) and not isinstance(attr, (staticmethod, classmethod, property)):
            # Wrap the method
            wrapped = _wrap_function(attr, required_scopes, oauth_config)
            setattr(cls, attr_name, wrapped)

    return cls
