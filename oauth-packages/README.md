# 🚀 Cezzis OAuth

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![PyPI version](https://img.shields.io/pypi/v/cezzis-oauth.svg)](https://pypi.org/project/cezzis-oauth/)
[![CI/CD](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-oauth-cicd.yaml/badge.svg)](https://github.com/mtnvencenzo/cezzis-pycore/actions/workflows/cezzis-oauth-cicd.yaml)

A lightweight, production-ready Python library for OAuth 2.0 authentication. Provides both **token acquisition** and **JWT verification** with enterprise-grade features including automatic token caching, refresh management, JWKS-based verification, and scope validation.


## Installation

### Using Poetry (Recommended)

```bash
poetry add cezzis-oauth
```

### Using pip

```bash
pip install cezzis-oauth
```

## Features

### 🔑 Token Acquisition
- **OAuth 2.0 Client Credentials Flow** - Secure token acquisition using client credentials
- **Automatic Token Caching** - Tokens are cached and reused until near expiration
- **Smart Token Refresh** - Automatic refresh 60 seconds before expiry to avoid edge cases
- **Thread-Safe Operations** - Async lock ensures safe concurrent access

### ✅ Token Verification
- **JWT Verification** - Validate JWT tokens using JWKS (JSON Web Key Set)
- **JWKS Caching** - Public keys are cached for performance
- **Scope Validation** - Verify tokens have required scopes/permissions
- **Comprehensive Error Handling** - Clear error messages for debugging

## Quick Start

### Token Acquisition

Acquire OAuth 2.0 access tokens using client credentials flow:

```python
import asyncio
from cezzis_oauth import OAuthTokenProvider

async def main():
    # Initialize the token provider
    provider = OAuthTokenProvider(
        domain="your-domain.auth0.com",
        client_id="your_client_id",
        client_secret="your_client_secret",
        audience="https://your-api.com",
        scope="read:data write:data"
    )
    
    # Get an access token (automatically cached and refreshed)
    token = await provider.get_access_token()
    print(f"Access Token: {token}")
    
    # Subsequent calls return cached token if still valid
    token = await provider.get_access_token()

if __name__ == "__main__":
    asyncio.run(main())
```

### Token Verification

Verify JWT tokens and validate scopes:

```python
import asyncio
from cezzis_oauth import OAuth2TokenVerifier, TokenVerificationError

async def main():
    # Initialize the verifier
    verifier = OAuth2TokenVerifier(
        domain="your-domain.auth0.com",
        audience="https://your-api.com",
        algorithms=["RS256"],
        issuer="https://your-domain.auth0.com/"
    )
    
    try:
        # Verify the token
        token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."
        payload = await verifier.verify_token(token)
        
        print(f"Token is valid! User: {payload.get('sub')}")
        
        # Verify required scopes
        required_scopes = ["read:data", "write:data"]
        verifier.verify_scopes(payload, required_scopes)
        print("Token has all required scopes!")
        
    except TokenVerificationError as e:
        print(f"Token verification failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Using with FastAPI

Protect your API endpoints with JWT verification:

```python
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from cezzis_oauth import OAuth2TokenVerifier, TokenVerificationError

app = FastAPI()
security = HTTPBearer()

# Initialize verifier (do this once at startup)
verifier = OAuth2TokenVerifier(
    domain="your-domain.auth0.com",
    audience="https://your-api.com",
    algorithms=["RS256"],
    issuer="https://your-domain.auth0.com/"
)

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency to verify JWT tokens."""
    try:
        payload = await verifier.verify_token(credentials.credentials)
        return payload
    except TokenVerificationError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token"
        )

@app.get("/protected")
async def protected_route(payload: dict = Depends(verify_token)):
    """Protected endpoint requiring valid JWT."""
    return {"message": f"Hello {payload.get('sub')}!"}

@app.get("/admin")
async def admin_route(payload: dict = Depends(verify_token)):
    """Admin endpoint requiring specific scopes."""
    try:
        verifier.verify_scopes(payload, ["admin:access"])
        return {"message": "Welcome, admin!"}
    except TokenVerificationError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
```

## API Reference

### OAuthTokenProvider

**Constructor Parameters:**
- `domain` (str): OAuth domain (e.g., "your-domain.auth0.com")
- `client_id` (str): OAuth application client ID
- `client_secret` (str): OAuth application client secret
- `audience` (str): API identifier/audience
- `scope` (str): Space-separated list of scopes

**Methods:**
- `async get_access_token() -> str`: Get a valid access token (cached and auto-refreshed)

### OAuth2TokenVerifier

**Constructor Parameters:**
- `domain` (str): OAuth domain
- `audience` (str): Expected token audience
- `algorithms` (list[str]): Allowed signing algorithms (e.g., ["RS256"])
- `issuer` (str): Expected token issuer

**Methods:**
- `async verify_token(token: str) -> dict`: Verify JWT and return payload
- `verify_scopes(payload: dict, required_scopes: list[str]) -> bool`: Validate token has required scopes
- `async get_jwks() -> dict`: Fetch JWKS from OAuth (cached)

**Exceptions:**
- `TokenVerificationError`: Raised when token verification fails


## License
This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.


## Support

- Issues: [GitHub Issues](https://github.com/mtnvencenzo/cezzis-pycore/issues)
- Discussions: [GitHub Discussions](https://github.com/mtnvencenzo/cezzis-pycore/discussions)

## Acknowledgments

Built with:
- [Poetry](https://python-poetry.org/) - Dependency management and packaging


