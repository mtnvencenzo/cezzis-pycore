from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from jose import JWTError

from cezzis_oauth import OAuth2TokenVerifier, TokenVerificationError


@pytest.fixture
def verifier():
    """Fixture to create an OAuth2TokenVerifier instance."""
    return OAuth2TokenVerifier(
        domain="test-domain.auth0.com",
        audience="https://test-api.com",
        algorithms=["RS256"],
        issuer="https://test-domain.auth0.com/",
    )


@pytest.fixture
def mock_jwks():
    """Fixture for a mock JWKS response."""
    return {
        "keys": [
            {"kid": "test-key-id-123", "kty": "RSA", "use": "sig", "n": "test-modulus", "e": "AQAB"},
            {"kid": "test-key-id-456", "kty": "RSA", "use": "sig", "n": "test-modulus-2", "e": "AQAB"},
        ]
    }


@pytest.fixture
def mock_jwt_payload():
    """Fixture for a decoded JWT payload."""
    return {
        "sub": "user123",
        "aud": "https://test-api.com",
        "iss": "https://test-domain.auth0.com/",
        "exp": 1704844800,
        "iat": 1704841200,
        "scope": "read:data write:data",
    }


class TestOAuth2TokenVerifierInit:
    """Tests for OAuth2TokenVerifier initialization."""

    def test_initialization(self, verifier):
        """Test that verifier initializes with correct attributes."""
        assert verifier._domain == "test-domain.auth0.com"
        assert verifier._audience == "https://test-api.com"
        assert verifier._algorithms == ["RS256"]
        assert verifier._issuer == "https://test-domain.auth0.com/"
        assert verifier._jwks_cache is None

    def test_initialization_with_multiple_algorithms(self):
        """Test initialization with multiple algorithms."""
        verifier = OAuth2TokenVerifier(
            domain="test-domain.auth0.com",
            audience="https://test-api.com",
            algorithms=["RS256", "RS384", "RS512"],
            issuer="https://test-domain.auth0.com/",
        )
        assert verifier._algorithms == ["RS256", "RS384", "RS512"]


class TestGetJwks:
    """Tests for get_jwks method."""

    @pytest.mark.asyncio
    async def test_get_jwks_success(self, verifier, mock_jwks):
        """Test successful JWKS fetch."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.json = MagicMock(return_value=mock_jwks)
            mock_response.raise_for_status = MagicMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            jwks = await verifier.get_jwks()

            assert jwks == mock_jwks
            assert verifier._jwks_cache == mock_jwks
            mock_client.get.assert_called_once_with("https://test-domain.auth0.com/.well-known/jwks.json", timeout=10.0)

    @pytest.mark.asyncio
    async def test_get_jwks_caching(self, verifier, mock_jwks):
        """Test that JWKS is cached after first fetch."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.json = MagicMock(return_value=mock_jwks)
            mock_response.raise_for_status = MagicMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            # First call - should fetch
            jwks1 = await verifier.get_jwks()
            assert mock_client.get.call_count == 1

            # Second call - should use cache
            jwks2 = await verifier.get_jwks()
            assert jwks1 == jwks2
            assert mock_client.get.call_count == 1  # Still only called once

    @pytest.mark.asyncio
    async def test_get_jwks_http_error(self, verifier):
        """Test handling of HTTP errors during JWKS fetch."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.raise_for_status = MagicMock(
                side_effect=httpx.HTTPStatusError("404 Not Found", request=MagicMock(), response=MagicMock())
            )
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            with pytest.raises(TokenVerificationError) as exc_info:
                await verifier.get_jwks()

            assert "Unable to fetch JWKS for token verification" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_jwks_network_error(self, verifier, caplog):
        """Test handling of network errors during JWKS fetch."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get = AsyncMock(side_effect=httpx.ConnectError("Connection failed"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            with caplog.at_level("ERROR"):
                with pytest.raises(TokenVerificationError):
                    await verifier.get_jwks()

            assert "Failed to fetch JWKS from OAuth" in caplog.text


class TestGetSigningKey:
    """Tests for _get_signing_key method."""

    def test_get_signing_key_success(self, verifier, mock_jwks):
        """Test successful extraction of signing key."""
        mock_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3Qta2V5LWlkLTEyMyJ9.payload.signature"

        with patch("jose.jwt.get_unverified_header") as mock_header:
            mock_header.return_value = {"kid": "test-key-id-123", "alg": "RS256"}

            with patch("jose.backends.cryptography_backend.CryptographyRSAKey") as mock_rsa_key:
                mock_key_instance = MagicMock()
                mock_key_instance.to_pem.return_value = b"-----BEGIN PUBLIC KEY-----\ntest\n-----END PUBLIC KEY-----"
                mock_rsa_key.return_value = mock_key_instance

                key = verifier._get_signing_key(mock_token, mock_jwks)

                assert key == "-----BEGIN PUBLIC KEY-----\ntest\n-----END PUBLIC KEY-----"
                mock_header.assert_called_once_with(mock_token)

    def test_get_signing_key_missing_kid(self, verifier, mock_jwks):
        """Test handling of token with missing kid."""
        mock_token = "eyJhbGciOiJSUzI1NiJ9.payload.signature"

        with patch("jose.jwt.get_unverified_header") as mock_header:
            mock_header.return_value = {"alg": "RS256"}  # No kid

            with pytest.raises(TokenVerificationError) as exc_info:
                verifier._get_signing_key(mock_token, mock_jwks)

            assert "Token header missing 'kid' field" in str(exc_info.value)

    def test_get_signing_key_not_found(self, verifier, mock_jwks):
        """Test handling when signing key is not found in JWKS."""
        mock_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InVua25vd24ta2lkIn0.payload.signature"

        with patch("jose.jwt.get_unverified_header") as mock_header:
            mock_header.return_value = {"kid": "unknown-kid", "alg": "RS256"}

            with pytest.raises(TokenVerificationError) as exc_info:
                verifier._get_signing_key(mock_token, mock_jwks)

            assert "Unable to find signing key with kid: unknown-kid" in str(exc_info.value)

    def test_get_signing_key_jwt_error(self, verifier, mock_jwks, caplog):
        """Test handling of JWT errors when extracting signing key."""
        mock_token = "invalid.token.format"

        with patch("jose.jwt.get_unverified_header") as mock_header:
            mock_header.side_effect = JWTError("Invalid token format")

            with caplog.at_level("ERROR"):
                with pytest.raises(TokenVerificationError) as exc_info:
                    verifier._get_signing_key(mock_token, mock_jwks)

            assert "Invalid token header" in str(exc_info.value)
            assert "Error extracting signing key" in caplog.text


class TestVerifyToken:
    """Tests for verify_token method."""

    @pytest.mark.asyncio
    async def test_verify_token_success(self, verifier, mock_jwks, mock_jwt_payload):
        """Test successful token verification."""
        mock_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3Qta2V5LWlkLTEyMyJ9.payload.signature"

        with patch.object(verifier, "get_jwks", AsyncMock(return_value=mock_jwks)):
            with patch.object(verifier, "_get_signing_key", return_value="mock-rsa-key"):
                with patch("jose.jwt.decode") as mock_decode:
                    mock_decode.return_value = mock_jwt_payload

                    payload = await verifier.verify_token(mock_token)

                    assert payload == mock_jwt_payload
                    mock_decode.assert_called_once_with(
                        mock_token,
                        "mock-rsa-key",
                        algorithms=["RS256"],
                        audience="https://test-api.com",
                        issuer="https://test-domain.auth0.com/",
                    )

    @pytest.mark.asyncio
    async def test_verify_token_incomplete_config(self):
        """Test verification fails with incomplete configuration."""
        verifier = OAuth2TokenVerifier(
            domain="", audience="https://test-api.com", algorithms=["RS256"], issuer="https://test-domain.auth0.com/"
        )

        with pytest.raises(TokenVerificationError) as exc_info:
            await verifier.verify_token("some.jwt.token")

        assert "OAuth configuration is incomplete" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_verify_token_jwt_error(self, verifier, mock_jwks, caplog):
        """Test handling of JWT verification errors."""
        mock_token = "invalid.jwt.token"

        with patch.object(verifier, "get_jwks", AsyncMock(return_value=mock_jwks)):
            with patch.object(verifier, "_get_signing_key", return_value="mock-rsa-key"):
                with patch("jose.jwt.decode") as mock_decode:
                    mock_decode.side_effect = JWTError("Token is invalid")

                    with caplog.at_level("WARNING"):
                        with pytest.raises(TokenVerificationError) as exc_info:
                            await verifier.verify_token(mock_token)

                    assert "Token verification failed" in str(exc_info.value)
                    assert "JWT verification failed" in caplog.text

    @pytest.mark.asyncio
    async def test_verify_token_unexpected_error(self, verifier, mock_jwks, caplog):
        """Test handling of unexpected errors during verification."""
        mock_token = "some.jwt.token"

        with patch.object(verifier, "get_jwks", AsyncMock(return_value=mock_jwks)):
            with patch.object(verifier, "_get_signing_key", side_effect=ValueError("Unexpected error")):
                with caplog.at_level("ERROR"):
                    with pytest.raises(TokenVerificationError) as exc_info:
                        await verifier.verify_token(mock_token)

                assert "Token verification failed" in str(exc_info.value)
                assert "Unexpected error during token verification" in caplog.text

    @pytest.mark.asyncio
    async def test_verify_token_expired(self, verifier, mock_jwks):
        """Test handling of expired tokens."""
        mock_token = "expired.jwt.token"

        with patch.object(verifier, "get_jwks", AsyncMock(return_value=mock_jwks)):
            with patch.object(verifier, "_get_signing_key", return_value="mock-rsa-key"):
                with patch("jose.jwt.decode") as mock_decode:
                    mock_decode.side_effect = JWTError("Token has expired")

                    with pytest.raises(TokenVerificationError) as exc_info:
                        await verifier.verify_token(mock_token)

                    assert "Token verification failed: Token has expired" in str(exc_info.value)


class TestVerifyScopes:
    """Tests for verify_scopes method."""

    def test_verify_scopes_success(self, verifier):
        """Test successful scope verification."""
        payload = {"scope": "read:data write:data admin:access"}
        required_scopes = ["read:data", "write:data"]

        result = verifier.verify_scopes(payload, required_scopes)

        assert result is True

    def test_verify_scopes_all_required(self, verifier):
        """Test verification with all required scopes present."""
        payload = {"scope": "read:data write:data delete:data"}
        required_scopes = ["read:data", "write:data", "delete:data"]

        result = verifier.verify_scopes(payload, required_scopes)

        assert result is True

    def test_verify_scopes_missing(self, verifier, caplog):
        """Test verification fails when scopes are missing."""
        payload = {"scope": "read:data"}
        required_scopes = ["read:data", "write:data", "delete:data"]

        with caplog.at_level("WARNING"):
            with pytest.raises(TokenVerificationError) as exc_info:
                verifier.verify_scopes(payload, required_scopes)

        assert "Missing required scopes" in str(exc_info.value)
        assert "write:data" in str(exc_info.value)
        assert "delete:data" in str(exc_info.value)
        assert "Token missing required scopes" in caplog.text

    def test_verify_scopes_empty_required(self, verifier):
        """Test verification succeeds with no required scopes."""
        payload = {"scope": "read:data"}
        required_scopes = []

        result = verifier.verify_scopes(payload, required_scopes)

        assert result is True

    def test_verify_scopes_no_scope_claim(self, verifier):
        """Test verification fails when token has no scope claim."""
        payload = {
            "sub": "user123"
            # No scope claim
        }
        required_scopes = ["read:data"]

        with pytest.raises(TokenVerificationError) as exc_info:
            verifier.verify_scopes(payload, required_scopes)

        assert "Missing required scopes: read:data" in str(exc_info.value)

    def test_verify_scopes_permissions_array(self, verifier):
        """Test verification using permissions array fallback."""
        payload = {"permissions": ["read:data", "write:data", "admin:access"]}
        required_scopes = ["read:data", "write:data"]

        result = verifier.verify_scopes(payload, required_scopes)

        assert result is True

    def test_verify_scopes_permissions_array_missing(self, verifier):
        """Test verification fails with missing permissions."""
        payload = {"permissions": ["read:data"]}
        required_scopes = ["read:data", "write:data"]

        with pytest.raises(TokenVerificationError) as exc_info:
            verifier.verify_scopes(payload, required_scopes)

        assert "Missing required scopes: write:data" in str(exc_info.value)

    def test_verify_scopes_empty_scope_string(self, verifier):
        """Test verification with empty scope string."""
        payload = {"scope": ""}
        required_scopes = ["read:data"]

        with pytest.raises(TokenVerificationError) as exc_info:
            verifier.verify_scopes(payload, required_scopes)

        assert "Missing required scopes: read:data" in str(exc_info.value)

    def test_verify_scopes_single_scope(self, verifier):
        """Test verification with single scope."""
        payload = {"scope": "read:data"}
        required_scopes = ["read:data"]

        result = verifier.verify_scopes(payload, required_scopes)

        assert result is True


class TestIntegration:
    """Integration tests combining multiple methods."""

    @pytest.mark.asyncio
    async def test_full_verification_flow(self, verifier, mock_jwks, mock_jwt_payload):
        """Test complete verification flow from JWKS fetch to scope validation."""
        mock_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3Qta2V5LWlkLTEyMyJ9.payload.signature"

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.json = MagicMock(return_value=mock_jwks)
            mock_response.raise_for_status = MagicMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            with patch.object(verifier, "_get_signing_key", return_value="mock-rsa-key"):
                with patch("jose.jwt.decode") as mock_decode:
                    mock_decode.return_value = mock_jwt_payload

                    # Verify token
                    payload = await verifier.verify_token(mock_token)
                    assert payload == mock_jwt_payload

                    # Verify scopes
                    result = verifier.verify_scopes(payload, ["read:data"])
                    assert result is True

    @pytest.mark.asyncio
    async def test_jwks_cached_across_verifications(self, verifier, mock_jwks, mock_jwt_payload):
        """Test that JWKS is cached and reused across multiple verifications."""
        mock_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3Qta2V5LWlkLTEyMyJ9.payload.signature"

        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = MagicMock()
            mock_response.json = MagicMock(return_value=mock_jwks)
            mock_response.raise_for_status = MagicMock()
            mock_client.get = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            with patch.object(verifier, "_get_signing_key", return_value="mock-rsa-key"):
                with patch("jose.jwt.decode") as mock_decode:
                    mock_decode.return_value = mock_jwt_payload

                    # First verification
                    await verifier.verify_token(mock_token)
                    assert mock_client.get.call_count == 1

                    # Second verification - should use cached JWKS
                    await verifier.verify_token(mock_token)
                    assert mock_client.get.call_count == 1  # Still only called once
