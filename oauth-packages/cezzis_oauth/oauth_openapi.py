from typing import Any


def generate_openapi_oauth2_scheme(
    name: str,
    client_id: str,
    domain: str,
    audience: str,
    scopes: dict[str, str] | None = None,
    pkce: str | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Generate OpenAPI OAuth2 scheme definition.

    Returns:
        A dictionary representing the OpenAPI OAuth2 scheme.
    """
    if scopes is None:
        scopes = {}

    return {
        name: {
            "type": "oauth2",
            "flows": {
                "authorizationCode": {
                    "authorizationUrl": f"https://{domain}/authorize?audience={audience}",
                    "tokenUrl": f"https://{domain}/oauth/token",
                    "scopes": scopes,
                    "x-usePkce": pkce,
                    "x-defaultClientId": client_id or "",
                }
            },
        }
    }
