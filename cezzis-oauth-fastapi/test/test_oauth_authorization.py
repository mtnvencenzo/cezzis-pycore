import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cezzis_oauth import TokenVerificationError
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

from cezzis_oauth_fastapi.oauth_authorization import oauth_authorization


class MockOAuthConfig:
    """Mock OAuth configuration for testing."""

    def __init__(
        self,
        domain: str = "test.auth0.com",
        audience: str = "https://api.test.com",
        algorithms: list[str] | None = None,
        issuer: str = "https://test.auth0.com/",
    ):
        self.domain = domain
        self.audience = audience
        self.algorithms = algorithms or ["RS256"]
        self.issuer = issuer


def get_test_oauth_config():
    """Test config provider function."""
    return MockOAuthConfig()


@pytest.fixture
def mock_request():
    """Create a mock FastAPI Request object."""
    request = MagicMock(spec=Request)
    request.headers = {"Authorization": "Bearer test_token_123"}
    return request


@pytest.fixture
def mock_request_no_auth():
    """Create a mock Request without Authorization header."""
    request = MagicMock(spec=Request)
    request.headers = {}
    return request


@pytest.fixture(autouse=True)
def reset_verifier():
    """Reset the global _verifiers cache before and after each test."""
    import sys

    oauth_auth_module = sys.modules["cezzis_oauth_fastapi.oauth_authorization"]
    oauth_auth_module._verifiers.clear()
    yield
    oauth_auth_module._verifiers.clear()


@pytest.fixture
def mock_verifier():
    """Create a mock OAuth2TokenVerifier."""
    with patch("cezzis_oauth_fastapi.oauth_authorization.OAuth2TokenVerifier") as mock:
        verifier_instance = MagicMock()
        verifier_instance.verify_token = AsyncMock(return_value={"sub": "user123", "scope": "read:data write:data"})
        verifier_instance.verify_scopes = MagicMock()
        mock.return_value = verifier_instance
        yield mock


class TestOAuthAuthorizationDecorator:
    """Tests for the oauth_authorization decorator."""

    @pytest.mark.asyncio
    async def test_successful_authorization_with_scopes(self, mock_request, mock_verifier):
        """Test successful authorization with valid token and scopes."""

        @oauth_authorization(scopes=["read:data"], config_provider=get_test_oauth_config)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        result = await protected_endpoint(_rq=mock_request)

        assert result == {"message": "success"}
        mock_verifier.assert_called_once()
        verifier = mock_verifier.return_value
        verifier.verify_token.assert_called_once_with("test_token_123")
        verifier.verify_scopes.assert_called_once()

    @pytest.mark.asyncio
    async def test_successful_authorization_without_scopes(self, mock_request, mock_verifier):
        """Test successful authorization without scope verification."""

        @oauth_authorization(scopes=[], config_provider=get_test_oauth_config)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        result = await protected_endpoint(_rq=mock_request)

        assert result == {"message": "success"}
        mock_verifier.assert_called_once()
        verifier = mock_verifier.return_value
        verifier.verify_token.assert_called_once_with("test_token_123")
        verifier.verify_scopes.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_authorization_header(self, mock_request_no_auth, mock_verifier):
        """Test that missing Authorization header raises 401."""

        @oauth_authorization(scopes=["read:data"], config_provider=get_test_oauth_config)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        with pytest.raises(HTTPException) as exc_info:
            await protected_endpoint(_rq=mock_request_no_auth)

        assert exc_info.value.status_code == 401
        assert "Missing or invalid authorization token" in exc_info.value.detail
        mock_verifier.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_authorization_header_format(self, mock_verifier):
        """Test that invalid Authorization header format raises 401."""
        request = MagicMock(spec=Request)
        request.headers = {"Authorization": "InvalidFormat token123"}

        @oauth_authorization(scopes=["read:data"], config_provider=get_test_oauth_config)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        with pytest.raises(HTTPException) as exc_info:
            await protected_endpoint(_rq=request)

        assert exc_info.value.status_code == 401
        assert "Missing or invalid authorization token" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_token_verification_failure(self, mock_request, mock_verifier):
        """Test that token verification failure raises 403."""
        mock_verifier.return_value.verify_token.side_effect = TokenVerificationError("Invalid token")

        @oauth_authorization(scopes=["read:data"], config_provider=get_test_oauth_config)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        with pytest.raises(HTTPException) as exc_info:
            await protected_endpoint(_rq=mock_request)

        assert exc_info.value.status_code == 403
        assert "Invalid token" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_scope_verification_failure(self, mock_request, mock_verifier):
        """Test that scope verification failure raises 403."""
        mock_verifier.return_value.verify_scopes.side_effect = TokenVerificationError("Insufficient scopes")

        @oauth_authorization(scopes=["write:data"], config_provider=get_test_oauth_config)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        with pytest.raises(HTTPException) as exc_info:
            await protected_endpoint(_rq=mock_request)

        assert exc_info.value.status_code == 403
        assert "Insufficient scopes" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_unexpected_error(self, mock_request, mock_verifier):
        """Test that unexpected errors raise 500."""
        mock_verifier.return_value.verify_token.side_effect = Exception("Unexpected error")

        @oauth_authorization(scopes=["read:data"], config_provider=get_test_oauth_config)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        with pytest.raises(HTTPException) as exc_info:
            await protected_endpoint(_rq=mock_request)

        assert exc_info.value.status_code == 500
        assert "Authorization error" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_missing_request_object(self, mock_verifier):
        """Test that missing Request object raises 500."""

        @oauth_authorization(scopes=["read:data"], config_provider=get_test_oauth_config)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        with pytest.raises(HTTPException) as exc_info:
            await protected_endpoint()

        assert exc_info.value.status_code == 500
        assert "Internal server error" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_local_environment_bypass(self, mock_request, mock_verifier):
        """Test that authorization is bypassed in local environment."""
        with patch.dict(os.environ, {"ENV": "local"}):

            @oauth_authorization(scopes=["read:data"], config_provider=get_test_oauth_config)
            async def protected_endpoint(_rq: Request):
                return {"message": "success"}

            result = await protected_endpoint(_rq=mock_request)

            assert result == {"message": "success"}
            mock_verifier.assert_not_called()

    @pytest.mark.asyncio
    async def test_oauth_config_is_used(self, mock_request, mock_verifier):
        """Test that the OAuth config from config_provider is properly used."""
        custom_config = MockOAuthConfig(
            domain="custom.auth0.com",
            audience="https://custom-api.com",
            algorithms=["RS512"],
            issuer="https://custom.auth0.com/",
        )

        def custom_config_provider():
            return custom_config

        @oauth_authorization(scopes=["read:data"], config_provider=custom_config_provider)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        await protected_endpoint(_rq=mock_request)

        # Verify the verifier was created with the custom config
        mock_verifier.assert_called_once_with(
            domain="custom.auth0.com",
            audience="https://custom-api.com",
            algorithms=["RS512"],
            issuer="https://custom.auth0.com/",
        )

    @pytest.mark.asyncio
    async def test_decorator_on_method_with_self(self, mock_request, mock_verifier):
        """Test decorator works with instance methods that have 'self' parameter."""

        class TestRouter:
            @oauth_authorization(scopes=["read:data"], config_provider=get_test_oauth_config)
            async def get_data(self, _rq: Request):
                return {"data": "test"}

        router = TestRouter()
        result = await router.get_data(_rq=mock_request)

        assert result == {"data": "test"}
        mock_verifier.assert_called_once()


class TestOAuthAuthorizationClassDecorator:
    """Tests for applying oauth_authorization to entire classes."""

    @pytest.mark.asyncio
    async def test_class_decorator_protects_all_methods(self, mock_request, mock_verifier):
        """Test that class decorator protects all public methods."""

        @oauth_authorization(scopes=["admin:data"], config_provider=get_test_oauth_config)
        class ProtectedRouter:
            async def method_one(self, _rq: Request):
                return {"method": "one"}

            async def method_two(self, _rq: Request):
                return {"method": "two"}

        router = ProtectedRouter()

        # Test first method
        result1 = await router.method_one(_rq=mock_request)
        assert result1 == {"method": "one"}

        # Test second method
        result2 = await router.method_two(_rq=mock_request)
        assert result2 == {"method": "two"}

        # Verify verifier was created once (cached for both methods since they use same config)
        mock_verifier.assert_called_once()
        # But verify token was checked for both calls
        verifier = mock_verifier.return_value
        assert verifier.verify_token.call_count == 2

    @pytest.mark.asyncio
    async def test_class_decorator_skips_private_methods(self, mock_request, mock_verifier):
        """Test that class decorator skips private methods."""

        @oauth_authorization(scopes=["admin:data"], config_provider=get_test_oauth_config)
        class ProtectedRouter:
            async def public_method(self, _rq: Request):
                return await self._private_method()

            async def _private_method(self):
                return {"private": "data"}

        router = ProtectedRouter()
        result = await router.public_method(_rq=mock_request)

        assert result == {"private": "data"}
        # Only the public method should be protected
        mock_verifier.assert_called_once()

    @pytest.mark.asyncio
    async def test_class_decorator_with_failed_auth(self, mock_request_no_auth):
        """Test that class decorator properly raises exceptions on failed auth."""

        @oauth_authorization(scopes=["admin:data"], config_provider=get_test_oauth_config)
        class ProtectedRouter:
            async def protected_method(self, _rq: Request):
                return {"data": "secret"}

        router = ProtectedRouter()

        with pytest.raises(HTTPException) as exc_info:
            await router.protected_method(_rq=mock_request_no_auth)

        assert exc_info.value.status_code == 401


class TestOAuthConfigProvider:
    """Tests for the config_provider parameter."""

    @pytest.mark.asyncio
    async def test_config_provider_called_at_decoration_time(self, mock_request, mock_verifier):
        """Test that config_provider is called when the decorator is applied."""
        call_count = 0

        def counting_config_provider():
            nonlocal call_count
            call_count += 1
            return MockOAuthConfig()

        @oauth_authorization(scopes=["read:data"], config_provider=counting_config_provider)
        async def protected_endpoint(_rq: Request):
            return {"message": "success"}

        # Config provider should be called once during decoration
        assert call_count == 1

        # Call the endpoint multiple times
        await protected_endpoint(_rq=mock_request)
        await protected_endpoint(_rq=mock_request)

        # Config provider should not be called again
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_different_configs_for_different_endpoints(self, mock_request, mock_verifier):
        """Test that different endpoints can use different configs."""

        def config_a():
            return MockOAuthConfig(domain="a.auth0.com", audience="https://api-a.com")

        def config_b():
            return MockOAuthConfig(domain="b.auth0.com", audience="https://api-b.com")

        @oauth_authorization(scopes=["read:data"], config_provider=config_a)
        async def endpoint_a(_rq: Request):
            return {"endpoint": "a"}

        @oauth_authorization(scopes=["read:data"], config_provider=config_b)
        async def endpoint_b(_rq: Request):
            return {"endpoint": "b"}

        # Reset mock to track calls
        mock_verifier.reset_mock()

        await endpoint_a(_rq=mock_request)
        first_call = mock_verifier.call_args_list[0]

        await endpoint_b(_rq=mock_request)
        second_call = mock_verifier.call_args_list[1]

        # Verify different configs were used
        assert first_call[1]["domain"] == "a.auth0.com"
        assert first_call[1]["audience"] == "https://api-a.com"
        assert second_call[1]["domain"] == "b.auth0.com"
        assert second_call[1]["audience"] == "https://api-b.com"
