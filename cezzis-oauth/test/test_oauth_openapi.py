import pytest

from cezzis_oauth import generate_openapi_oauth2_scheme


class TestGenerateOpenAPIAuth2Scheme:
    """Tests for generate_openapi_oauth2_scheme function."""

    def test_basic_generation_with_required_params(self):
        """Test generation with only required parameters."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="test-client-id", domain="test-domain.auth0.com", audience="https://test-api.com"
        )

        assert "oauth2" in result
        assert result["oauth2"]["type"] == "oauth2"
        assert "flows" in result["oauth2"]
        assert "authorizationCode" in result["oauth2"]["flows"]

    def test_authorization_url_format(self):
        """Test that authorization URL is correctly formatted."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="test-client-id", domain="test-domain.auth0.com", audience="https://test-api.com"
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["authorizationUrl"] == "https://test-domain.auth0.com/authorize?audience=https://test-api.com"

    def test_token_url_format(self):
        """Test that token URL is correctly formatted."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="test-client-id", domain="test-domain.auth0.com", audience="https://test-api.com"
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["tokenUrl"] == "https://test-domain.auth0.com/oauth/token"

    def test_client_id_included(self):
        """Test that client ID is included in the scheme."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2",
            client_id="test-client-id-123",
            domain="test-domain.auth0.com",
            audience="https://test-api.com",
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["x-defaultClientId"] == "test-client-id-123"

    def test_empty_client_id(self):
        """Test handling of empty client ID."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="", domain="test-domain.auth0.com", audience="https://test-api.com"
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["x-defaultClientId"] == ""

    def test_empty_scopes_default(self):
        """Test that empty scopes dict is used by default."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="test-client-id", domain="test-domain.auth0.com", audience="https://test-api.com"
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["scopes"] == {}

    def test_with_scopes(self):
        """Test generation with custom scopes."""
        scopes = {"read:data": "Read data", "write:data": "Write data", "admin:access": "Admin access"}

        result = generate_openapi_oauth2_scheme(
            name="oauth2",
            client_id="test-client-id",
            domain="test-domain.auth0.com",
            audience="https://test-api.com",
            scopes=scopes,
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["scopes"] == scopes
        assert "read:data" in auth_code["scopes"]
        assert "write:data" in auth_code["scopes"]
        assert "admin:access" in auth_code["scopes"]

    def test_with_single_scope(self):
        """Test generation with a single scope."""
        scopes = {"read:data": "Read access to data"}

        result = generate_openapi_oauth2_scheme(
            name="oauth2",
            client_id="test-client-id",
            domain="test-domain.auth0.com",
            audience="https://test-api.com",
            scopes=scopes,
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["scopes"] == scopes
        assert len(auth_code["scopes"]) == 1

    def test_pkce_none_default(self):
        """Test that PKCE is None by default."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="test-client-id", domain="test-domain.auth0.com", audience="https://test-api.com"
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["x-usePkce"] is None

    def test_with_pkce_enabled(self):
        """Test generation with PKCE enabled."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2",
            client_id="test-client-id",
            domain="test-domain.auth0.com",
            audience="https://test-api.com",
            pkce="true",
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["x-usePkce"] == "true"

    def test_with_pkce_custom_value(self):
        """Test generation with custom PKCE value."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2",
            client_id="test-client-id",
            domain="test-domain.auth0.com",
            audience="https://test-api.com",
            pkce="S256",
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert auth_code["x-usePkce"] == "S256"

    def test_custom_scheme_name(self):
        """Test generation with custom scheme name."""
        result = generate_openapi_oauth2_scheme(
            name="customOAuth",
            client_id="test-client-id",
            domain="test-domain.auth0.com",
            audience="https://test-api.com",
        )

        assert "customOAuth" in result
        assert "oauth2" not in result
        assert result["customOAuth"]["type"] == "oauth2"

    def test_different_domain(self):
        """Test with different OAuth domain."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="test-client-id", domain="example.eu.auth0.com", audience="https://api.example.com"
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert "example.eu.auth0.com" in auth_code["authorizationUrl"]
        assert "example.eu.auth0.com" in auth_code["tokenUrl"]

    def test_different_audience(self):
        """Test with different audience."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2",
            client_id="test-client-id",
            domain="test-domain.auth0.com",
            audience="https://custom-api.example.com",
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert "audience=https://custom-api.example.com" in auth_code["authorizationUrl"]

    def test_complete_structure(self):
        """Test that the complete structure is correct."""
        scopes = {"read:data": "Read data"}
        result = generate_openapi_oauth2_scheme(
            name="oauth2",
            client_id="test-client-id",
            domain="test-domain.auth0.com",
            audience="https://test-api.com",
            scopes=scopes,
            pkce="true",
        )

        # Verify top-level structure
        assert isinstance(result, dict)
        assert len(result) == 1
        assert "oauth2" in result

        # Verify scheme structure
        scheme = result["oauth2"]
        assert scheme["type"] == "oauth2"
        assert "flows" in scheme

        # Verify flows structure
        flows = scheme["flows"]
        assert "authorizationCode" in flows

        # Verify authorization code flow
        auth_code = flows["authorizationCode"]
        assert "authorizationUrl" in auth_code
        assert "tokenUrl" in auth_code
        assert "scopes" in auth_code
        assert "x-usePkce" in auth_code
        assert "x-defaultClientId" in auth_code

    def test_return_type(self):
        """Test that the function returns a dictionary."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="test-client-id", domain="test-domain.auth0.com", audience="https://test-api.com"
        )

        assert isinstance(result, dict)

    def test_all_parameters(self):
        """Test generation with all parameters provided."""
        scopes = {"read:users": "Read user data", "write:users": "Write user data", "delete:users": "Delete users"}

        result = generate_openapi_oauth2_scheme(
            name="myOAuth2Scheme",
            client_id="my-client-id-456",
            domain="my-domain.auth0.com",
            audience="https://my-api.example.com",
            scopes=scopes,
            pkce="S256",
        )

        auth_code = result["myOAuth2Scheme"]["flows"]["authorizationCode"]
        assert (
            auth_code["authorizationUrl"] == "https://my-domain.auth0.com/authorize?audience=https://my-api.example.com"
        )
        assert auth_code["tokenUrl"] == "https://my-domain.auth0.com/oauth/token"
        assert auth_code["scopes"] == scopes
        assert auth_code["x-usePkce"] == "S256"
        assert auth_code["x-defaultClientId"] == "my-client-id-456"

    def test_url_encoding_in_audience(self):
        """Test that audience with special characters is included correctly."""
        result = generate_openapi_oauth2_scheme(
            name="oauth2",
            client_id="test-client-id",
            domain="test-domain.auth0.com",
            audience="https://api.example.com/v1",
        )

        auth_code = result["oauth2"]["flows"]["authorizationCode"]
        assert "audience=https://api.example.com/v1" in auth_code["authorizationUrl"]

    def test_immutability_of_default_scopes(self):
        """Test that modifying returned scopes doesn't affect subsequent calls."""
        result1 = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="test-client-id", domain="test-domain.auth0.com", audience="https://test-api.com"
        )

        # Modify the scopes in result1
        result1["oauth2"]["flows"]["authorizationCode"]["scopes"]["new:scope"] = "New scope"

        # Create a new instance
        result2 = generate_openapi_oauth2_scheme(
            name="oauth2", client_id="test-client-id", domain="test-domain.auth0.com", audience="https://test-api.com"
        )

        # Verify result2 doesn't have the modified scope
        assert "new:scope" not in result2["oauth2"]["flows"]["authorizationCode"]["scopes"]
        assert result2["oauth2"]["flows"]["authorizationCode"]["scopes"] == {}
