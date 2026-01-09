from typing import Protocol


class OAuthConfig(Protocol):
    """Protocol defining required OAuth configuration properties."""

    domain: str
    audience: str
    algorithms: list[str]
    issuer: str
