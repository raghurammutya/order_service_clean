"""
Test authentication security - validates service-to-service auth works
Critical for production signoff - proves security vulnerabilities are fixed
"""
import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
import jwt
from app.api.v1.endpoints.positions_integration import verify_internal_service


class TestAuthenticationSecurity:
    """Test authentication and security functionality"""

    @pytest.mark.security
    def test_service_to_service_auth_required(self):
        """Test that internal APIs require proper service authentication"""
        # Test missing token
        with pytest.raises(HTTPException) as exc_info:
            verify_internal_service(None)
        
        assert exc_info.value.status_code == 401
        assert "Service authentication required" in str(exc_info.value.detail)

    @pytest.mark.security  
    def test_invalid_service_token_rejected(self):
        """Test that invalid service tokens are rejected"""
        with patch('app.api.v1.endpoints.positions_integration.settings') as mock_settings:
            mock_settings.INTERNAL_SERVICE_SECRET = "test-secret"
            
            # Test invalid token
            with pytest.raises(HTTPException) as exc_info:
                verify_internal_service("invalid-token")
            
            assert exc_info.value.status_code == 401
            assert "Invalid service token" in str(exc_info.value.detail)

    @pytest.mark.security
    def test_valid_service_token_accepted(self):
        """Test that valid service tokens are accepted"""
        with patch('app.api.v1.endpoints.positions_integration.settings') as mock_settings:
            mock_settings.INTERNAL_SERVICE_SECRET = "test-secret"
            
            # Create valid token
            token_payload = {
                "service_name": "ticker_service",
                "iat": 1640995200,
                "exp": 1640998800
            }
            valid_token = jwt.encode(token_payload, "test-secret", algorithm="HS256")
            
            # Should not raise exception
            result = verify_internal_service(valid_token)
            assert result == "ticker_service"

    @pytest.mark.security
    def test_unauthorized_service_rejected(self):
        """Test that unauthorized services are rejected"""
        with patch('app.api.v1.endpoints.positions_integration.settings') as mock_settings:
            mock_settings.INTERNAL_SERVICE_SECRET = "test-secret"
            
            # Create token for unauthorized service
            token_payload = {
                "service_name": "malicious_service",
                "iat": 1640995200,
                "exp": 1640998800
            }
            token = jwt.encode(token_payload, "test-secret", algorithm="HS256")
            
            with pytest.raises(HTTPException) as exc_info:
                verify_internal_service(token)
            
            assert exc_info.value.status_code == 403
            assert "not authorized for internal APIs" in str(exc_info.value.detail)

    @pytest.mark.security
    def test_expired_service_token_rejected(self):
        """Test that expired service tokens are rejected"""
        with patch('app.api.v1.endpoints.positions_integration.settings') as mock_settings:
            mock_settings.INTERNAL_SERVICE_SECRET = "test-secret"
            
            # Create expired token
            token_payload = {
                "service_name": "ticker_service", 
                "iat": 1640995200,
                "exp": 1640995201  # Expired immediately
            }
            token = jwt.encode(token_payload, "test-secret", algorithm="HS256")
            
            with pytest.raises(HTTPException) as exc_info:
                verify_internal_service(token)
            
            assert exc_info.value.status_code == 401
            assert "Service token expired" in str(exc_info.value.detail)

    @pytest.mark.security
    def test_test_auth_disabled_in_production(self):
        """Test that test auth is properly disabled in production"""
        with patch.dict('os.environ', {'ENVIRONMENT': 'production', 'TEST_AUTH_MODE': 'true'}):
            # Import after setting env vars to trigger the security check
            import importlib
            from app.auth import test_auth
            importlib.reload(test_auth)
            
            # Should be disabled in production regardless of TEST_AUTH_MODE
            assert test_auth.TEST_AUTH_MODE is False

    @pytest.mark.security
    def test_test_auth_only_in_dev_environments(self):
        """Test that test auth only works in development environments"""
        # Test development environment
        with patch.dict('os.environ', {'ENVIRONMENT': 'development', 'TEST_AUTH_MODE': 'true'}):
            import importlib
            from app.auth import test_auth
            importlib.reload(test_auth)
            assert test_auth.TEST_AUTH_MODE is True
            
        # Test production environment
        with patch.dict('os.environ', {'ENVIRONMENT': 'production', 'TEST_AUTH_MODE': 'true'}):
            importlib.reload(test_auth)
            assert test_auth.TEST_AUTH_MODE is False