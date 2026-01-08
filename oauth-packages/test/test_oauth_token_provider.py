import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from cezzis_oauth import OAuthTokenProvider


@pytest.fixture
def token_provider():
    """Fixture to create an OAuthTokenProvider instance."""
    return OAuthTokenProvider(
        domain="test-domain.auth0.com",
        client_id="test_client_id",
        client_secret="test_client_secret",
        audience="https://test-api.com",
        scope="read:data write:data",
    )


@pytest.fixture
def mock_token_response():
    """Fixture for a successful token response."""
    return {"access_token": "test_access_token_12345", "token_type": "Bearer", "expires_in": 3600}


class TestOAuthTokenProviderInit:
    """Tests for OAuthTokenProvider initialization."""

    def test_initialization(self, token_provider):
        """Test that provider initializes with correct attributes."""
        assert token_provider._domain == "test-domain.auth0.com"
        assert token_provider._client_id == "test_client_id"
        assert token_provider._client_secret == "test_client_secret"
        assert token_provider._audience == "https://test-api.com"
        assert token_provider._scope == "read:data write:data"
        assert token_provider._token is None
        assert token_provider._token_expiry is None


class TestGetAccessToken:
    """Tests for get_access_token method."""

    @pytest.mark.asyncio
    async def test_get_access_token_success(self, token_provider, mock_token_response):
        """Test successful token acquisition."""
        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(return_value=mock_token_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            token = await token_provider.get_access_token()

            assert token == "test_access_token_12345"
            assert token_provider._token == "test_access_token_12345"
            assert token_provider._token_expiry is not None
            mock_client.fetch_token.assert_called_once_with(
                "https://test-domain.auth0.com/oauth/token",
                grant_type="client_credentials",
                audience="https://test-api.com",
                scope="read:data write:data",
            )

    @pytest.mark.asyncio
    async def test_get_access_token_caching(self, token_provider, mock_token_response):
        """Test that tokens are cached and reused when valid."""
        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(return_value=mock_token_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            # First call - should fetch token
            token1 = await token_provider.get_access_token()
            assert mock_client.fetch_token.call_count == 1

            # Second call - should return cached token
            token2 = await token_provider.get_access_token()
            assert token1 == token2
            assert mock_client.fetch_token.call_count == 1  # Still only called once

    @pytest.mark.asyncio
    async def test_get_access_token_refresh_before_expiry(self, token_provider):
        """Test that token is refreshed 60 seconds before expiry."""
        # Set up an expired token
        token_provider._token = "old_token"
        token_provider._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=30)

        new_token_response = {"access_token": "new_access_token", "expires_in": 3600}

        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(return_value=new_token_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            token = await token_provider.get_access_token()

            # Should fetch new token because old one expires in 30 seconds (< 60 second threshold)
            assert token == "new_access_token"
            assert token_provider._token == "new_access_token"
            mock_client.fetch_token.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_access_token_expired_token(self, token_provider):
        """Test that expired token triggers refresh."""
        # Set up an expired token
        token_provider._token = "expired_token"
        token_provider._token_expiry = datetime.now(timezone.utc) - timedelta(seconds=100)

        new_token_response = {"access_token": "fresh_token", "expires_in": 3600}

        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(return_value=new_token_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            token = await token_provider.get_access_token()

            assert token == "fresh_token"
            assert token_provider._token == "fresh_token"
            mock_client.fetch_token.assert_called_once()


class TestFetchToken:
    """Tests for _fetch_token method."""

    @pytest.mark.asyncio
    async def test_fetch_token_sets_expiry(self, token_provider, mock_token_response):
        """Test that token expiry is calculated correctly."""
        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(return_value=mock_token_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            before_time = datetime.now(timezone.utc)
            token = await token_provider._fetch_token()
            after_time = datetime.now(timezone.utc)

            assert token == "test_access_token_12345"
            assert token_provider._token_expiry is not None

            # Token should expire in approximately 3600 seconds from now
            expected_expiry = before_time + timedelta(seconds=3600)
            assert token_provider._token_expiry >= expected_expiry
            assert token_provider._token_expiry <= after_time + timedelta(seconds=3600)

    @pytest.mark.asyncio
    async def test_fetch_token_default_expiry(self, token_provider):
        """Test that default expiry is used when not provided."""
        token_response_no_expiry = {
            "access_token": "test_token",
            "token_type": "Bearer",
            # No expires_in field
        }

        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(return_value=token_response_no_expiry)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            before_time = datetime.now(timezone.utc)
            token = await token_provider._fetch_token()

            assert token == "test_token"
            # Should default to 3600 seconds (1 hour)
            expected_expiry = before_time + timedelta(seconds=3600)
            assert token_provider._token_expiry >= expected_expiry

    @pytest.mark.asyncio
    async def test_fetch_token_http_error(self, token_provider):
        """Test handling of HTTP errors during token fetch."""
        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(
                side_effect=httpx.HTTPStatusError("401 Unauthorized", request=MagicMock(), response=MagicMock())
            )
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            with pytest.raises(RuntimeError) as exc_info:
                await token_provider._fetch_token()

            assert "Failed to fetch OAuth access token" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_token_network_error(self, token_provider):
        """Test handling of network errors during token fetch."""
        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(side_effect=httpx.ConnectError("Connection failed"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            with pytest.raises(RuntimeError) as exc_info:
                await token_provider._fetch_token()

            assert "Failed to fetch OAuth access token" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_token_unexpected_error(self, token_provider):
        """Test handling of unexpected errors during token fetch."""
        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(side_effect=ValueError("Unexpected error"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            with pytest.raises(RuntimeError) as exc_info:
                await token_provider._fetch_token()

            assert "Failed to fetch OAuth access token" in str(exc_info.value)


class TestConcurrency:
    """Tests for concurrent access and thread safety."""

    @pytest.mark.asyncio
    async def test_concurrent_token_requests(self, token_provider, mock_token_response):
        """Test that concurrent requests for tokens only fetch once."""
        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()

            # Simulate slow token fetch
            async def slow_fetch(*args, **kwargs):
                await asyncio.sleep(0.1)
                return mock_token_response

            mock_client.fetch_token = AsyncMock(side_effect=slow_fetch)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            # Launch multiple concurrent requests
            tasks = [token_provider.get_access_token() for _ in range(5)]
            tokens = await asyncio.gather(*tasks)

            # All should return the same token
            assert all(token == "test_access_token_12345" for token in tokens)

            # But fetch_token should only be called once due to lock
            assert mock_client.fetch_token.call_count == 1


class TestLogging:
    """Tests for logging behavior."""

    @pytest.mark.asyncio
    async def test_successful_fetch_logs_info(self, token_provider, mock_token_response, caplog):
        """Test that successful token fetch logs info message."""
        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(return_value=mock_token_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            with caplog.at_level("INFO"):
                await token_provider.get_access_token()

            assert "Successfully fetched OAuth access token" in caplog.text
            assert "expires in 3600 seconds" in caplog.text

    @pytest.mark.asyncio
    async def test_http_error_logs_error(self, token_provider, caplog):
        """Test that HTTP errors are logged."""
        with patch("cezzis_oauth.oauth_token_provider.AsyncOAuth2Client") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.fetch_token = AsyncMock(
                side_effect=httpx.HTTPStatusError("401 Unauthorized", request=MagicMock(), response=MagicMock())
            )
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            with caplog.at_level("ERROR"):
                with pytest.raises(RuntimeError):
                    await token_provider.get_access_token()

            assert "HTTP error fetching OAuth token" in caplog.text
